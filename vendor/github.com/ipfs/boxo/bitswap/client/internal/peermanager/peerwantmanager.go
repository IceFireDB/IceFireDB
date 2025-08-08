package peermanager

import (
	"fmt"
	"strings"
	"sync"

	"github.com/ipfs/boxo/peering"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
	manet "github.com/multiformats/go-multiaddr/net"
)

// Gauge can be used to keep track of a metric that increases and decreases
// incrementally. It is used by the peerWantManager to track the number of
// want-blocks that are active (ie sent but no response received)
type Gauge interface {
	Inc()
	Dec()
}

// BroadcastControl configures broadcast control functionality.
type BroadcastControl struct {
	// Enable enables or disables broadcast control.
	Enable bool
	// Host is the libp2p host used to get peer information.
	Host host.Host
	// MaxPeers is the hard limit on the number of peers to send broadcasts
	// to. A value of 0 means no broadcasts are sent. A value of -1 means there
	// is no limit.
	MaxPeers int
	// LocalPeers enables or disables broadcast control for peers on the local
	// network. If false, than always broadcast to peers on the local network.
	// If true, apply broadcast reduction to local peers.
	LocalPeers bool
	// PeeredPeers enables or disables broadcast reduction for peers configured
	// for peering. If false, than always broadcast to peers configured for
	// peering. If true, apply broadcast reduction to peered peers.
	// false (always broadcast to peered peers).
	PeeredPeers bool
	// MaxRandomPeers is the number of peers to broadcast to anyway, even
	// though broadcast reduction logic has determined that they are not
	// broadcast targets. Setting this to a non-zero value ensures at least
	// this number of random peers receives a broadcast. This may be helpful in
	// cases where peers that are not receiving broadcasts may have wanted
	// blocks.
	MaxRandomPeers int
	// SendToPendingPeers, when true, sends broadcasts to any peers that already
	// have a pending message to send.
	SendToPendingPeers bool
	// SkipGauge overrides the Gauge that tracks the number of broadcasts
	// skipped by broadcast reduction logic.
	SkipGauge Gauge
}

// NeedHost returns true if the Host is required to support the configuration.
func (bc BroadcastControl) NeedHost() bool {
	return bc.MaxPeers != 0 && !bc.LocalPeers && !bc.PeeredPeers
}

// peerWantManager keeps track of which want-haves and want-blocks have been
// sent to each peer, so that the PeerManager doesn't send duplicates.
type peerWantManager struct {
	// peerWants maps peers to outstanding wants.
	// A peer's wants is the _union_ of the broadcast wants and the wants in
	// this list.
	peerWants map[peer.ID]*peerWant

	// Reverse index of all wants in peerWants.
	wantPeers map[cid.Cid]map[peer.ID]struct{}

	// broadcastWants tracks all the current broadcast wants.
	broadcastWants *cid.Set

	// Keeps track of the number of active want-haves & want-blocks
	wantGauge Gauge
	// Keeps track of the number of active want-blocks
	wantBlockGauge Gauge

	bcastControl BroadcastControl
	bcastMutex   sync.Mutex
	bcastTargets map[peer.ID]struct{}
	remotePeers  map[peer.ID]struct{}
}

type peerWant struct {
	wantBlocks *cid.Set
	wantHaves  *cid.Set
	peerQueue  PeerQueue
}

// New creates a new peerWantManager with a Gauge that keeps track of the
// number of active want-blocks (ie sent but no response received)
func newPeerWantManager(wantGauge, wantBlockGauge Gauge, bcastControl BroadcastControl) *peerWantManager {
	pwm := &peerWantManager{
		broadcastWants: cid.NewSet(),
		peerWants:      make(map[peer.ID]*peerWant),
		wantPeers:      make(map[cid.Cid]map[peer.ID]struct{}),
		wantGauge:      wantGauge,
		wantBlockGauge: wantBlockGauge,
	}

	if bcastControl.Enable {
		if bcastControl.Host == nil && bcastControl.NeedHost() {
			panic("Host missing from BroadcastControl")
		}

		pwm.bcastControl = bcastControl
		pwm.bcastTargets = make(map[peer.ID]struct{})
		pwm.remotePeers = make(map[peer.ID]struct{})
	}

	return pwm
}

// addPeer adds a peer whose wants we need to keep track of. It sends the
// current list of broadcast wants to the peer.
func (pwm *peerWantManager) addPeer(peerQueue PeerQueue, p peer.ID) {
	if _, ok := pwm.peerWants[p]; ok {
		return
	}

	pwm.peerWants[p] = &peerWant{
		wantBlocks: cid.NewSet(),
		wantHaves:  cid.NewSet(),
		peerQueue:  peerQueue,
	}

	// Broadcast any live want-haves to the newly connected peer
	if pwm.broadcastWants.Len() > 0 {
		wants := pwm.broadcastWants.Keys()
		peerQueue.AddBroadcastWantHaves(wants)
	}
}

// RemovePeer removes a peer and its associated wants from tracking
func (pwm *peerWantManager) removePeer(p peer.ID) {
	pws, ok := pwm.peerWants[p]
	if !ok {
		return
	}

	// Clean up want-blocks
	_ = pws.wantBlocks.ForEach(func(c cid.Cid) error {
		// Clean up want-blocks from the reverse index
		pwm.reverseIndexRemove(c, p)

		// Decrement the gauges by the number of pending want-blocks to the peer
		peerCounts := pwm.wantPeerCounts(c)
		if peerCounts.wantBlock == 0 {
			pwm.wantBlockGauge.Dec()
		}
		if !peerCounts.wanted() {
			pwm.wantGauge.Dec()
		}

		return nil
	})

	// Clean up want-haves
	_ = pws.wantHaves.ForEach(func(c cid.Cid) error {
		// Clean up want-haves from the reverse index
		pwm.reverseIndexRemove(c, p)

		// Decrement the gauge by the number of pending want-haves to the peer
		peerCounts := pwm.wantPeerCounts(c)
		if !peerCounts.wanted() {
			pwm.wantGauge.Dec()
		}
		return nil
	})

	delete(pwm.peerWants, p)
	delete(pwm.remotePeers, p)

	if pwm.bcastTargets != nil {
		pwm.bcastMutex.Lock()
		delete(pwm.bcastTargets, p)
		pwm.bcastMutex.Unlock()
	}
}

// broadcastWantHaves sends want-haves to any peers that have not yet been sent them.
func (pwm *peerWantManager) broadcastWantHaves(wantHaves []cid.Cid) {
	var reduce bool
	var maxPeers int
	var randosToSend int

	// If broadcast reduction logic enabled.
	if pwm.bcastControl.Enable {
		if pwm.bcastControl.MaxPeers == 0 {
			// broadcasts are completely disabled
			return
		}
		reduce = true
		maxPeers = pwm.bcastControl.MaxPeers
		randosToSend = pwm.bcastControl.MaxRandomPeers
	}

	unsent := make([]cid.Cid, 0, len(wantHaves))
	for _, c := range wantHaves {
		if pwm.broadcastWants.Has(c) {
			// Already a broadcast want, skip it.
			continue
		}
		pwm.broadcastWants.Add(c)
		unsent = append(unsent, c)

		// If no peer has a pending want for the key
		if _, ok := pwm.wantPeers[c]; !ok {
			// Increment the total wants gauge
			pwm.wantGauge.Inc()
		}
	}

	if len(unsent) == 0 {
		return
	}

	// Allocate a single buffer to filter broadcast wants for each peer
	bcstWantsBuffer := make([]cid.Cid, 0, len(unsent))

	// Send broadcast wants to each peer
	for p, pws := range pwm.peerWants {
		var sentRando bool
		if reduce && pwm.skipBroadcast(p, pws.peerQueue) {
			if randosToSend == 0 {
				pwm.bcastControl.SkipGauge.Inc()
				continue
			}
			// Send this random peer, that is not a broadcast target, a
			// broadcast.
			//
			// The source of randomness is changes to the peerWants map.
			sentRando = true
		}

		peerUnsent := bcstWantsBuffer[:0]
		for _, c := range unsent {
			// If we've already sent a want to this peer, skip them.
			if !pws.wantBlocks.Has(c) && !pws.wantHaves.Has(c) {
				peerUnsent = append(peerUnsent, c)
			}
		}

		if len(peerUnsent) == 0 {
			continue
		}

		pws.peerQueue.AddBroadcastWantHaves(peerUnsent)

		if sentRando {
			randosToSend--
		}

		if maxPeers > 0 {
			maxPeers--
			if maxPeers == 0 {
				break
			}
		}
	}
}

func (pwm *peerWantManager) skipBroadcast(peerID peer.ID, peerQueue PeerQueue) bool {
	// Broadcast to peer from which block(s) have been previously received.
	pwm.bcastMutex.Lock()
	_, ok := pwm.bcastTargets[peerID]
	pwm.bcastMutex.Unlock()
	if ok {
		return false
	}

	// Broadcast to peers on local network if they are not subject to broadcast reduction.
	if !pwm.bcastControl.LocalPeers && pwm.isLocalPeer(peerID) {
		// Add local peer to broadcast targets to avoid next isLocalPeer check.
		pwm.bcastMutex.Lock()
		pwm.bcastTargets[peerID] = struct{}{}
		pwm.bcastMutex.Unlock()
		return false
	}

	// Broadcast to peers that are configured for peering if they are not subject to broadcast reduction.
	if !pwm.bcastControl.PeeredPeers {
		connMgr := pwm.bcastControl.Host.ConnManager()
		if connMgr != nil && connMgr.IsProtected(peerID, peering.ConnmgrTag) {
			// Add peered peer to broadcast targets to avoid future connection tag lookup.
			pwm.bcastMutex.Lock()
			pwm.bcastTargets[peerID] = struct{}{}
			pwm.bcastMutex.Unlock()
			return false
		}
	}

	// Broadcast to peers that have a pending message to piggyback on.
	if pwm.bcastControl.SendToPendingPeers && peerQueue.HasMessage() {
		return false
	}
	return true
}

func (pwm *peerWantManager) markBroadcastTarget(peerID peer.ID) {
	if pwm.bcastTargets == nil {
		return
	}
	pwm.bcastMutex.Lock()
	pwm.bcastTargets[peerID] = struct{}{}
	pwm.bcastMutex.Unlock()
}

func (pwm *peerWantManager) isLocalPeer(peerID peer.ID) bool {
	if _, ok := pwm.remotePeers[peerID]; ok {
		return false
	}
	peerStore := pwm.bcastControl.Host.Peerstore()
	if peerStore == nil {
		return false
	}
	addrs := peerStore.Addrs(peerID)
	// Assume local if peer has no addresses.
	if len(addrs) == 0 {
		return true
	}
	for _, addr := range addrs {
		if manet.IsPrivateAddr(addr) || manet.IsIPLoopback(addr) {
			return true
		}
	}
	pwm.remotePeers[peerID] = struct{}{}
	return false
}

// sendWants only sends the peer the want-blocks and want-haves that have not
// already been sent to it.
func (pwm *peerWantManager) sendWants(p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	// Get the existing want-blocks and want-haves for the peer
	pws, ok := pwm.peerWants[p]
	if !ok {
		// In practice this should never happen
		log.Errorf("sendWants() called with peer %s but peer not found in peerWantManager", string(p))
		return
	}

	fltWantBlks := make([]cid.Cid, 0, len(wantBlocks))

	// Iterate over the requested want-blocks
	for _, c := range wantBlocks {
		// If the want-block hasn't been sent to the peer
		if pws.wantBlocks.Has(c) {
			continue
		}

		// Increment the want gauges
		peerCounts := pwm.wantPeerCounts(c)
		if peerCounts.wantBlock == 0 {
			pwm.wantBlockGauge.Inc()
		}
		if !peerCounts.wanted() {
			pwm.wantGauge.Inc()
		}

		// Make sure the CID is no longer recorded as a want-have
		pws.wantHaves.Remove(c)

		// Record that the CID was sent as a want-block
		pws.wantBlocks.Add(c)

		// Add the CID to the results
		fltWantBlks = append(fltWantBlks, c)

		// Update the reverse index
		pwm.reverseIndexAdd(c, p)
	}

	fltWantHvs := make([]cid.Cid, 0, len(wantHaves))

	// Iterate over the requested want-haves
	for _, c := range wantHaves {
		// If we've already broadcasted this want, don't bother with a
		// want-have.
		if pwm.broadcastWants.Has(c) {
			continue
		}

		// If the CID has not been sent as a want-block or want-have
		if !pws.wantBlocks.Has(c) && !pws.wantHaves.Has(c) {
			// Increment the total wants gauge
			peerCounts := pwm.wantPeerCounts(c)
			if !peerCounts.wanted() {
				pwm.wantGauge.Inc()
			}

			// Record that the CID was sent as a want-have
			pws.wantHaves.Add(c)

			// Add the CID to the results
			fltWantHvs = append(fltWantHvs, c)

			// Update the reverse index
			pwm.reverseIndexAdd(c, p)
		}
	}

	// Send the want-blocks and want-haves to the peer
	pws.peerQueue.AddWants(fltWantBlks, fltWantHvs)
}

// sendCancels sends a cancel to each peer to which a corresponding want was
// sent
func (pwm *peerWantManager) sendCancels(cancelKs []cid.Cid) {
	if len(cancelKs) == 0 {
		return
	}

	// Track cancellation state: peerCounts tracks per-CID want counts across
	// peers, while broadcastCancels collects CIDs wants that were broadcasted
	// and need cancellation across all peers
	peerCounts := make(map[cid.Cid]wantPeerCnts, len(cancelKs))
	broadcastCancels := make([]cid.Cid, 0, len(cancelKs))
	for _, c := range cancelKs {
		peerCounts[c] = pwm.wantPeerCounts(c)
		if peerCounts[c].isBroadcast {
			broadcastCancels = append(broadcastCancels, c)
		}
	}

	// Send cancels to a particular peer
	send := func(pws *peerWant) {
		// Start from the broadcast cancels
		toCancel := broadcastCancels

		// For each key to be cancelled
		for _, c := range cancelKs {
			// Check if a want was sent for the key
			if !pws.wantBlocks.Has(c) && !pws.wantHaves.Has(c) {
				continue
			}

			// Unconditionally remove from the want lists.
			pws.wantBlocks.Remove(c)
			pws.wantHaves.Remove(c)

			// If it's a broadcast want, we've already added it to
			// the peer cancels.
			if !pwm.broadcastWants.Has(c) {
				toCancel = append(toCancel, c)
			}
		}

		// Send cancels to the peer
		if len(toCancel) > 0 {
			pws.peerQueue.AddCancels(toCancel)
		}
	}

	clearWantsForCID := func(c cid.Cid) {
		peerCnts := peerCounts[c]
		// If there were any peers that had a pending want-block for the key
		if peerCnts.wantBlock > 0 {
			// Decrement the want-block gauge
			pwm.wantBlockGauge.Dec()
		}
		// If there was a peer that had a pending want or it was a broadcast want
		if peerCnts.wanted() {
			// Decrement the total wants gauge
			pwm.wantGauge.Dec()
		}
		delete(pwm.wantPeers, c)
	}

	if len(broadcastCancels) > 0 {
		// If a broadcast want is being cancelled, send the cancel to all
		// peers
		for _, pws := range pwm.peerWants {
			send(pws)
		}

		// Remove cancelled broadcast wants
		for _, c := range broadcastCancels {
			pwm.broadcastWants.Remove(c)
		}

		for _, c := range cancelKs {
			clearWantsForCID(c)
		}
	} else {
		// Only send cancels to peers that received a corresponding want
		for _, c := range cancelKs {
			for p := range pwm.wantPeers[c] {
				pws, ok := pwm.peerWants[p]
				if !ok {
					// Should never happen but check just in case
					log.Errorf("sendCancels - peerWantManager index missing peer %s", p)
					continue
				}
				send(pws)
			}

			clearWantsForCID(c)
		}
	}
}

// wantPeerCnts stores the number of peers that have pending wants for a CID
type wantPeerCnts struct {
	// number of peers that have a pending want-block for the CID
	wantBlock int
	// number of peers that have a pending want-have for the CID
	wantHave int
	// whether the CID is a broadcast want
	isBroadcast bool
}

// wanted returns true if any peer wants the CID or it's a broadcast want
func (pwm *wantPeerCnts) wanted() bool {
	return pwm.wantBlock > 0 || pwm.wantHave > 0 || pwm.isBroadcast
}

// wantPeerCounts counts how many peers have a pending want-block and want-have
// for the given CID
func (pwm *peerWantManager) wantPeerCounts(c cid.Cid) wantPeerCnts {
	blockCount := 0
	haveCount := 0
	for p := range pwm.wantPeers[c] {
		pws, ok := pwm.peerWants[p]
		if !ok {
			log.Errorf("reverse index has extra peer %s for key %s in peerWantManager", string(p), c)
			continue
		}

		if pws.wantBlocks.Has(c) {
			blockCount++
		} else if pws.wantHaves.Has(c) {
			haveCount++
		}
	}

	return wantPeerCnts{blockCount, haveCount, pwm.broadcastWants.Has(c)}
}

// Add the peer to the list of peers that have sent a want with the cid
func (pwm *peerWantManager) reverseIndexAdd(c cid.Cid, p peer.ID) bool {
	peers, ok := pwm.wantPeers[c]
	if !ok {
		peers = make(map[peer.ID]struct{}, 10)
		pwm.wantPeers[c] = peers
	}
	peers[p] = struct{}{}
	return !ok
}

// Remove the peer from the list of peers that have sent a want with the cid
func (pwm *peerWantManager) reverseIndexRemove(c cid.Cid, p peer.ID) {
	if peers, ok := pwm.wantPeers[c]; ok {
		delete(peers, p)
		if len(peers) == 0 {
			delete(pwm.wantPeers, c)
		}
	}
}

// GetWantBlocks returns the set of all want-blocks sent to all peers
func (pwm *peerWantManager) getWantBlocks() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all known peers
	for _, pws := range pwm.peerWants {
		// Iterate over all want-blocks
		_ = pws.wantBlocks.ForEach(func(c cid.Cid) error {
			// Add the CID to the results
			res.Add(c)
			return nil
		})
	}

	return res.Keys()
}

// GetWantHaves returns the set of all want-haves sent to all peers
func (pwm *peerWantManager) getWantHaves() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all peers with active wants.
	for _, pws := range pwm.peerWants {
		// Iterate over all want-haves
		_ = pws.wantHaves.ForEach(func(c cid.Cid) error {
			// Add the CID to the results
			res.Add(c)
			return nil
		})
	}
	_ = pwm.broadcastWants.ForEach(func(c cid.Cid) error {
		res.Add(c)
		return nil
	})

	return res.Keys()
}

// GetWants returns the set of all wants (both want-blocks and want-haves).
func (pwm *peerWantManager) getWants() []cid.Cid {
	res := pwm.broadcastWants.Keys()

	// Iterate over all targeted wants, removing ones that are also in the
	// broadcast list.
	for c := range pwm.wantPeers {
		if pwm.broadcastWants.Has(c) {
			continue
		}
		res = append(res, c)
	}

	return res
}

func (pwm *peerWantManager) String() string {
	var b strings.Builder
	for p, ws := range pwm.peerWants {
		b.WriteString(fmt.Sprintf("Peer %s: %d want-have / %d want-block:\n", p, ws.wantHaves.Len(), ws.wantBlocks.Len()))
		for _, c := range ws.wantHaves.Keys() {
			b.WriteString(fmt.Sprintf("  want-have  %s\n", c))
		}
		for _, c := range ws.wantBlocks.Keys() {
			b.WriteString(fmt.Sprintf("  want-block %s\n", c))
		}
	}
	return b.String()
}
