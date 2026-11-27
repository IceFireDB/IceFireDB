package dht

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

// These values are first estimates, to be replaced by measurements on the
// public network.
const (
	// seedDialConcurrency is how many saved peers are dialed at once.
	seedDialConcurrency = 16
	// seedDialSoftTimeout frees a dial's slot for the next saved peer while
	// the dial keeps going until seedDialTimeout.
	seedDialSoftTimeout = 2 * time.Second
	seedDialTimeout     = 5 * time.Second
	// seedFallbackDelay is the longest a restore waits for restoreThreshold
	// saved peers to answer before dialing the bootstrap peers, and then for
	// those before refreshes may start.
	seedFallbackDelay = 20 * time.Second
)

const (
	maxSavedAddrsPerPeer = 10
	snapshotWriteTimeout = 10 * time.Second
	maxBaselineAge       = 15 * 24 * time.Hour
)

// rtPersistence holds the state for saving the routing table and seeding it
// back after a restart.
type rtPersistence struct {
	key ds.Key
	// interval is how often the routing table is saved, zero when
	// persistence is off.
	interval       time.Duration
	bootstrapOrder BootstrapOrderOpt
	// snapshot holds the peers saved last, or loaded at start; they seed the
	// routing table when it is empty.
	snapshot  atomic.Pointer[rtSnapshot]
	finalSave sync.Once
	// refreshHeld holds refreshes while a restore dials saved peers, and
	// refreshDeferred records that one was requested meanwhile.
	refreshHeld     atomic.Bool
	refreshDeferred atomic.Bool
}

// snapshotVersion is the version of the on-disk routing table snapshot. A
// snapshot with another version is ignored rather than migrated: it only
// speeds up the next restart, so losing one is harmless.
const snapshotVersion = 1

var snapshotKeyPrefix = ds.NewKey("/kad-dht/routing-table")

// snapshotKey returns the datastore key of the routing table snapshot of the
// DHT speaking proto. Keying by protocol lets DHTs that share a datastore, like
// the WAN and LAN halves of a dual DHT, keep separate snapshots.
func snapshotKey(proto protocol.ID) ds.Key {
	return snapshotKeyPrefix.Child(ds.NewKey(string(proto)))
}

// rtSnapshot is an immutable set of routing table peers saved at savedAt.
type rtSnapshot struct {
	peers   []peer.AddrInfo
	savedAt time.Time
}

// snapshotJSON is the on-disk layout of an rtSnapshot. Peer IDs and addresses
// are plain strings so that one entry this version can't parse, such as a
// multiaddr protocol it doesn't know, is skipped instead of failing the whole
// snapshot.
type snapshotJSON struct {
	Version int            `json:"version"`
	SavedAt time.Time      `json:"savedAt"`
	Peers   []snapshotPeer `json:"peers"`
}

type snapshotPeer struct {
	ID    string   `json:"id"`
	Addrs []string `json:"addrs"`
}

func encodeSnapshot(s *rtSnapshot) ([]byte, error) {
	raw := snapshotJSON{
		Version: snapshotVersion,
		SavedAt: s.savedAt.UTC(),
		Peers:   make([]snapshotPeer, 0, len(s.peers)),
	}
	for _, ai := range s.peers {
		addrs := make([]string, len(ai.Addrs))
		for i, a := range ai.Addrs {
			addrs[i] = a.String()
		}
		raw.Peers = append(raw.Peers, snapshotPeer{ID: ai.ID.String(), Addrs: addrs})
	}
	return json.Marshal(raw)
}

func decodeSnapshot(b []byte) (*rtSnapshot, error) {
	var raw snapshotJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("decoding routing table snapshot: %w", err)
	}
	if raw.Version != snapshotVersion {
		return nil, fmt.Errorf("unsupported routing table snapshot version %d", raw.Version)
	}

	s := &rtSnapshot{savedAt: raw.SavedAt, peers: make([]peer.AddrInfo, 0, len(raw.Peers))}
	skipped := 0
	for _, rp := range raw.Peers {
		ai, ok := parseSnapshotPeer(rp)
		if !ok {
			skipped++
			continue
		}
		s.peers = append(s.peers, ai)
	}
	if skipped > 0 {
		logger.Debugw("skipped unreadable routing table snapshot entries", "count", skipped)
	}
	return s, nil
}

// parseSnapshotPeer keeps the addresses of rp that parse, and reports false
// when the peer ID doesn't parse or no address does.
func parseSnapshotPeer(rp snapshotPeer) (peer.AddrInfo, bool) {
	id, err := peer.Decode(rp.ID)
	if err != nil {
		return peer.AddrInfo{}, false
	}
	addrs := make([]ma.Multiaddr, 0, len(rp.Addrs))
	for _, s := range rp.Addrs {
		if a, err := ma.NewMultiaddr(s); err == nil {
			addrs = append(addrs, a)
		}
	}
	return peer.AddrInfo{ID: id, Addrs: addrs}, len(addrs) > 0
}

// savableAddrs keeps, in order, the first maxSavedAddrsPerPeer addresses that
// the host can dial and that don't expire, as relayed and certhash addresses
// do. Leaving out the ones the host can't use keeps the cap for the ones it
// can.
func savableAddrs(addrs []ma.Multiaddr, canDial func(ma.Multiaddr) bool) []ma.Multiaddr {
	kept := make([]ma.Multiaddr, 0, min(len(addrs), maxSavedAddrsPerPeer))
	for _, a := range addrs {
		if len(kept) == maxSavedAddrsPerPeer {
			break
		}
		if !expires(a) && !slices.ContainsFunc(kept, a.Equal) && canDial(a) {
			kept = append(kept, a)
		}
	}
	return kept
}

// connectedAddrs returns the addresses p advertises that the host has a
// connection on, known to work. Other connection addresses can't be dialed:
// an inbound TCP connection usually comes from an ephemeral port, and an
// outbound WebTransport connection's address has no certhash.
func (dht *IpfsDHT) connectedAddrs(p peer.ID, advertised []ma.Multiaddr) []ma.Multiaddr {
	var addrs []ma.Multiaddr
	for _, c := range dht.host.Network().ConnsToPeer(p) {
		if a := c.RemoteMultiaddr(); slices.ContainsFunc(advertised, a.Equal) {
			addrs = append(addrs, a)
		}
	}
	return addrs
}

func expires(a ma.Multiaddr) bool {
	for _, c := range a {
		if c := c.Code(); c == ma.P_CIRCUIT || c == ma.P_CERTHASH {
			return true
		}
	}
	return false
}

// worthSaving reports whether a snapshot of peers should replace one of
// previous peers. A routing table shrinks when its peers stop answering, as
// during an outage, and right after a restart that was cut short; saving it
// then would throw away a better snapshot.
func worthSaving(peers, previous int) bool {
	return peers >= max(minRTRefreshThreshold, previous/2)
}

// snapshotPeers returns the routing table peers with the addresses worth
// saving.
func (dht *IpfsDHT) snapshotPeers() []peer.AddrInfo {
	ids := dht.routingTable.ListPeers()
	peers := make([]peer.AddrInfo, 0, len(ids))
	for _, p := range ids {
		advertised := dht.peerstore.Addrs(p)
		known := slices.Concat(dht.connectedAddrs(p, advertised), advertised)
		addrs := savableAddrs(dht.filterAddrs(known), func(a ma.Multiaddr) bool {
			return dht.host.Network().CanDial(p, a)
		})
		if len(addrs) > 0 {
			peers = append(peers, peer.AddrInfo{ID: p, Addrs: addrs})
		}
	}
	return peers
}

// saveBaseline is the size worthSaving holds a new snapshot against. A
// snapshot older than maxBaselineAge, or dated in the future because the clock
// was ahead when it was saved, stops being a baseline, so a routing table that
// settled smaller eventually replaces it.
func saveBaseline(s *rtSnapshot) int {
	if s == nil {
		return 0
	}
	if age := time.Since(s.savedAt); age < 0 || age > maxBaselineAge {
		return 0
	}
	return len(s.peers)
}

// saveRoutingTable writes the routing table peers to the datastore, unless
// the table shrank too much to be worth saving.
func (dht *IpfsDHT) saveRoutingTable(ctx context.Context) error {
	previous := saveBaseline(dht.persistence.snapshot.Load())
	peers := dht.snapshotPeers()
	if !worthSaving(len(peers), previous) {
		logger.Debugw("keeping the previous routing table snapshot", "peers", len(peers), "previous", previous)
		return nil
	}

	s := &rtSnapshot{peers: peers, savedAt: time.Now()}
	b, err := encodeSnapshot(s)
	if err != nil {
		return fmt.Errorf("encoding routing table snapshot: %w", err)
	}
	if err := dht.datastore.Put(ctx, dht.persistence.key, b); err != nil {
		return fmt.Errorf("saving routing table snapshot: %w", err)
	}
	if err := dht.datastore.Sync(ctx, dht.persistence.key); err != nil {
		return fmt.Errorf("syncing routing table snapshot: %w", err)
	}
	dht.persistence.snapshot.Store(s)
	return nil
}

// startRoutingTablePersistence loads the snapshot saved by a previous run and
// starts saving the routing table: first after one refresh period, so a node
// that crashes early still leaves a snapshot, then every persistence interval.
func (dht *IpfsDHT) startRoutingTablePersistence(refreshInterval time.Duration) {
	dht.loadRoutingTable(dht.ctx)
	firstSave := dht.persistence.interval
	if refreshInterval > 0 {
		firstSave = min(refreshInterval, firstSave)
	}
	dht.wg.Go(func() { dht.saveRoutingTableLoop(firstSave, dht.persistence.interval) })
}

// saveRoutingTableLoop saves the routing table after firstDelay, then every
// interval, until the DHT closes.
func (dht *IpfsDHT) saveRoutingTableLoop(firstDelay, interval time.Duration) {
	timer := time.NewTimer(firstDelay)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := dht.saveRoutingTable(dht.ctx); err != nil {
				logger.Warnw("failed to save the routing table", "error", err)
			}
			timer.Reset(interval)
		case <-dht.ctx.Done():
			return
		}
	}
}

// saveRoutingTableOnClose saves the routing table one last time, on the first
// Close only: by a second one, the caller may have closed the datastore.
// dht.ctx is cancelled by then, so the write gets a context of its own.
func (dht *IpfsDHT) saveRoutingTableOnClose() error {
	if dht.persistence.interval == 0 {
		return nil
	}
	var err error
	dht.persistence.finalSave.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), snapshotWriteTimeout)
		defer cancel()
		err = dht.saveRoutingTable(ctx)
	})
	return err
}

// loadRoutingTable reads the snapshot saved by a previous run. A missing or
// unreadable snapshot leaves the DHT without saved peers. An old snapshot is
// still used: any of its peers still online is a way back into the network.
// Trying the others costs at most one dial each.
func (dht *IpfsDHT) loadRoutingTable(ctx context.Context) {
	b, err := dht.datastore.Get(ctx, dht.persistence.key)
	if errors.Is(err, ds.ErrNotFound) {
		return
	}
	if err != nil {
		logger.Warnw("failed to read the routing table snapshot", "error", err)
		return
	}
	s, err := decodeSnapshot(b)
	if err != nil {
		logger.Warnw("ignoring the routing table snapshot", "error", err)
		return
	}
	dht.persistence.snapshot.Store(s)
}

// seedRoutingTable refills an empty routing table from the peers saved from
// the previous routing table and from the bootstrap peers, in the configured
// order. Saved peers only get dialed: they enter the routing table the way
// any peer does, after identify and a lookup check.
func (dht *IpfsDHT) seedRoutingTable() {
	var bootstrap []peer.AddrInfo
	if dht.bootstrapPeers != nil {
		bootstrap = dht.bootstrapPeers()
	}
	s := &seeding{
		dial:      dht.host.Connect,
		threshold: dht.restoreThreshold(),
		hold:      func() { dht.persistence.refreshHeld.Store(true) },
		release:   dht.releaseRefreshHold,
	}
	s.run(dht.ctx, dht.persistence.bootstrapOrder, dht.savedSeeds(bootstrap), bootstrap)
}

// savedSeeds returns a shuffled copy of the saved peers, without the bootstrap
// peers, which are dialed in their own step.
func (dht *IpfsDHT) savedSeeds(bootstrap []peer.AddrInfo) []peer.AddrInfo {
	s := dht.persistence.snapshot.Load()
	if s == nil {
		return nil
	}
	isBootstrap := make(map[peer.ID]struct{}, len(bootstrap))
	for _, ai := range bootstrap {
		isBootstrap[ai.ID] = struct{}{}
	}
	seeds := make([]peer.AddrInfo, 0, len(s.peers))
	for _, ai := range s.peers {
		if _, ok := isBootstrap[ai.ID]; !ok {
			seeds = append(seeds, ai)
		}
	}
	dht.shuffle(len(seeds), func(i, j int) { seeds[i], seeds[j] = seeds[j], seeds[i] })
	return seeds
}

// restoreThreshold is how many saved peers must answer for a restore to do
// without the bootstrap peers, and the routing table size at which refreshes
// may start. It is as many peers as a lookup queries at once, so a refresh
// that starts from them spreads its first queries over that many peers
// instead of resting on the first saved peer to answer. It is never below the
// number of bootstrap peers a node joins through, whatever Concurrency is set
// to.
func (dht *IpfsDHT) restoreThreshold() int {
	return max(dht.alpha, maxNBoostrappers)
}

// requestRefresh asks for a routing table refresh. While a restore holds
// refreshes, until the routing table has a broad enough base to start from,
// the request waits for the hold to end.
func (dht *IpfsDHT) requestRefresh() {
	if dht.persistence.refreshHeld.Load() {
		dht.persistence.refreshDeferred.Store(true)
		return
	}
	dht.rtRefreshManager.RefreshNoWait()
}

// releaseRefreshHoldAtThreshold ends a restore's refresh hold once the
// routing table has a broad enough base to refresh from.
func (dht *IpfsDHT) releaseRefreshHoldAtThreshold() {
	if dht.persistence.refreshHeld.Load() && dht.routingTable.Size() >= dht.restoreThreshold() {
		dht.releaseRefreshHold()
	}
}

// releaseRefreshHold ends the hold placed by a restore and starts the refresh
// it held back. It must run in a goroutine tracked by dht.wg, so the refresh
// request can't race RtRefreshManager.Close.
func (dht *IpfsDHT) releaseRefreshHold() {
	if !dht.persistence.refreshHeld.CompareAndSwap(true, false) {
		return
	}
	deferred := dht.persistence.refreshDeferred.Swap(false)
	// A refresh started while closing would only race the final save.
	if dht.ctx.Err() != nil || !(dht.autoRefresh || deferred) {
		return
	}
	// Refresh, unlike RefreshNoWait, isn't dropped while a refresh runs.
	dht.rtRefreshManager.Refresh(false)
}

// seeding refills an empty routing table from saved peers and bootstrap
// peers.
type seeding struct {
	dial func(context.Context, peer.AddrInfo) error
	// threshold is how many saved peers must answer for a restore to do
	// without the bootstrap peers.
	threshold int
	// answered counts the saved peers that answered. It is compared with the
	// threshold rather than the routing table size, which lags behind: a peer
	// enters the routing table only after a lookup check.
	answered atomic.Int64
	// hold and release hold refreshes while saved peers are dialed. release
	// may be called more than once: the DHT also releases the hold as soon
	// as the routing table reaches the threshold.
	hold, release func()
}

// run dials the saved peers and the bootstrap peers in the given order.
func (s *seeding) run(ctx context.Context, order BootstrapOrderOpt, saved, bootstrap []peer.AddrInfo) {
	switch order {
	case PersistedFirst:
		s.sweep(ctx, saved, bootstrap)
	case Concurrent:
		var wg sync.WaitGroup
		wg.Go(func() { s.dialBootstrappers(ctx, bootstrap) })
		s.sweep(ctx, saved, nil)
		wg.Wait()
	default:
		if !s.dialBootstrappers(ctx, bootstrap) {
			s.sweep(ctx, saved, nil)
		}
	}
}

// dialBootstrappers connects to bootstrap peers in random order, one at a
// time, until maxNBoostrappers of them succeed, or every one when fewer are
// configured. It reports whether enough succeeded; an empty list never does.
// Unlike saved peers, bootstrap peers get the swarm's own dial timeout:
// resolving their /dnsaddr addresses can take a while on a slow link.
func (s *seeding) dialBootstrappers(ctx context.Context, peers []peer.AddrInfo) bool {
	if len(peers) == 0 {
		return false
	}
	want := min(maxNBoostrappers, len(peers))
	found := 0
	for _, i := range rand.Perm(len(peers)) {
		if ctx.Err() != nil {
			return false
		}
		if err := s.dial(ctx, peers[i]); err != nil {
			logger.Warnw("failed to bootstrap", "peer", peers[i].ID, "error", err)
			continue
		}
		found++
		if found == want {
			return true
		}
	}
	return false
}

// sweep dials the saved peers, holding refreshes until the routing table
// reaches the threshold, the sweep ends, or seedFallbackDelay passes. If fewer
// saved peers than the threshold answered by then, it first dials the
// fallback peers, for seedFallbackDelay at most, so that refreshes don't start
// out resting on the few saved peers that answered. The sweep goes on
// meanwhile.
func (s *seeding) sweep(ctx context.Context, saved, fallback []peer.AddrInfo) {
	if len(saved) == 0 {
		if len(fallback) > 0 {
			s.dialBootstrappers(ctx, fallback)
		}
		return
	}

	s.hold()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.dialAll(ctx, saved)
	}()

	deadline := time.NewTimer(seedFallbackDelay)
	defer deadline.Stop()
	select {
	case <-done:
	case <-deadline.C:
	case <-ctx.Done():
	}
	if len(fallback) > 0 && ctx.Err() == nil && s.answered.Load() < int64(s.threshold) {
		logger.Debugw("too few saved peers answered, dialing bootstrap peers", "answered", s.answered.Load())
		// Bounded, as unreachable bootstrap peers would otherwise hold
		// refreshes back for a swarm dial timeout each.
		fallbackCtx, cancel := context.WithTimeout(ctx, seedFallbackDelay)
		s.dialBootstrappers(fallbackCtx, fallback)
		cancel()
	}
	s.release()
	<-done
}

// dialAll dials peers through seedDialConcurrency slots. A dial frees its
// slot after seedDialSoftTimeout, so a dead peer can't stall the next ones,
// but keeps going until seedDialTimeout. It gives up once
// 2*seedDialConcurrency dials have completed without a single success, and
// seedDialTimeout has passed: the node is offline, or the saved peers are long
// gone. Waiting out seedDialTimeout keeps dead peers that refuse at once from
// ending the sweep before the live peers dialed alongside them finish
// connecting.
func (s *seeding) dialAll(ctx context.Context, peers []peer.AddrInfo) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	slots := make(chan struct{}, seedDialConcurrency)
	var wg sync.WaitGroup
	var completed atomic.Int64
	for _, ai := range peers {
		if !acquire(ctx, slots) {
			break
		}
		dialed := make(chan struct{})
		wg.Go(func() {
			defer close(dialed)
			dialCtx, dialCancel := context.WithTimeout(ctx, seedDialTimeout)
			err := s.dial(dialCtx, ai)
			dialCancel()

			if err == nil {
				s.answered.Add(1)
			}
			if completed.Add(1) >= 2*seedDialConcurrency && s.answered.Load() == 0 && time.Since(start) >= seedDialTimeout {
				cancel()
			}
		})
		wg.Go(func() {
			select {
			case <-dialed:
			case <-time.After(seedDialSoftTimeout):
			}
			<-slots
		})
	}
	wg.Wait()
	logger.Debugw("dialed saved routing table peers", "completed", completed.Load(), "answered", s.answered.Load(), "saved", len(peers))
}

// acquire takes a slot, and reports false once ctx is done.
func acquire(ctx context.Context, slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	if ctx.Err() != nil {
		<-slots
		return false
	}
	return true
}
