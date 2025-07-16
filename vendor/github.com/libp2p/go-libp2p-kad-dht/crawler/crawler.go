package crawler

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-msgio/pbio"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
)

var (
	logger = logging.Logger("dht-crawler")

	_ Crawler = (*DefaultCrawler)(nil)
)

type (
	// Crawler connects to hosts in the DHT to track routing tables of peers.
	Crawler interface {
		// Run crawls the DHT starting from the startingPeers, and calls either handleSuccess or handleFail depending on whether a peer was successfully contacted or not.
		Run(ctx context.Context, startingPeers []*peer.AddrInfo, handleSuccess HandleQueryResult, handleFail HandleQueryFail)
	}
	// DefaultCrawler provides a default implementation of Crawler.
	DefaultCrawler struct {
		parallelism    int
		connectTimeout time.Duration
		queryTimeout   time.Duration
		host           host.Host
		dhtRPC         *pb.ProtocolMessenger
	}
)

// NewDefaultCrawler creates a new DefaultCrawler
func NewDefaultCrawler(host host.Host, opts ...Option) (*DefaultCrawler, error) {
	o := new(options)
	if err := defaults(o); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}

	pm, err := pb.NewProtocolMessenger(&messageSender{h: host, protocols: o.protocols, timeout: o.perMsgTimeout})
	if err != nil {
		return nil, err
	}

	return &DefaultCrawler{
		parallelism:    o.parallelism,
		connectTimeout: o.connectTimeout,
		queryTimeout:   3 * o.connectTimeout,
		host:           host,
		dhtRPC:         pm,
	}, nil
}

// MessageSender handles sending wire protocol messages to a given peer
type messageSender struct {
	h         host.Host
	protocols []protocol.ID
	timeout   time.Duration
}

// SendRequest sends a peer a message and waits for its response
func (ms *messageSender) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	tctx, cancel := context.WithTimeout(ctx, ms.timeout)
	defer cancel()

	s, err := ms.h.NewStream(tctx, p, ms.protocols...)
	if err != nil {
		return nil, err
	}

	w := pbio.NewDelimitedWriter(s)
	if err := w.WriteMsg(pmes); err != nil {
		return nil, err
	}

	r := pbio.NewDelimitedReader(s, network.MessageSizeMax)
	defer func() { _ = s.Close() }()

	msg := new(pb.Message)
	if err := ctxReadMsg(tctx, r, msg); err != nil {
		_ = s.Reset()
		return nil, err
	}

	return msg, nil
}

func ctxReadMsg(ctx context.Context, rc pbio.ReadCloser, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r pbio.ReadCloser) {
		defer close(errc)
		err := r.ReadMsg(mes)
		errc <- err
	}(rc)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SendMessage sends a peer a message without waiting on a response
func (ms *messageSender) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	s, err := ms.h.NewStream(ctx, p, ms.protocols...)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	w := pbio.NewDelimitedWriter(s)
	return w.WriteMsg(pmes)
}

// HandleQueryResult is a callback on successful peer query
type HandleQueryResult func(p peer.ID, rtPeers []*peer.AddrInfo)

// HandleQueryFail is a callback on failed peer query
type HandleQueryFail func(p peer.ID, err error)

type peerAddrs struct {
	peers map[peer.ID]map[string]ma.Multiaddr
	lk    *sync.RWMutex
}

func newPeerAddrs() peerAddrs {
	return peerAddrs{
		peers: make(map[peer.ID]map[string]ma.Multiaddr),
		lk:    new(sync.RWMutex),
	}
}

func (ps peerAddrs) RemoveSourceAndAddPeers(source peer.ID, peers map[peer.ID][]ma.Multiaddr) {
	ps.lk.Lock()
	defer ps.lk.Unlock()

	// remove source from peerstore
	delete(ps.peers, source)
	// add peers to peerstore
	ps.addAddrsNoLock(peers)
}

func (ps peerAddrs) addAddrsNoLock(peers map[peer.ID][]ma.Multiaddr) {
	for p, addrs := range peers {
		ps.addPeerAddrsNoLock(p, addrs)
	}
}

func (ps peerAddrs) addPeerAddrsNoLock(p peer.ID, addrs []ma.Multiaddr) {
	if _, ok := ps.peers[p]; !ok {
		ps.peers[p] = make(map[string]ma.Multiaddr)
	}
	for _, addr := range addrs {
		ps.peers[p][string(addr.Bytes())] = addr
	}
}

func (ps peerAddrs) PeerInfo(p peer.ID) peer.AddrInfo {
	ps.lk.RLock()
	defer ps.lk.RUnlock()

	addrs := make([]ma.Multiaddr, 0, len(ps.peers[p]))
	for _, addr := range ps.peers[p] {
		addrs = append(addrs, addr)
	}
	return peer.AddrInfo{ID: p, Addrs: addrs}
}

// Run crawls dht peers from an initial seed of `startingPeers`
func (c *DefaultCrawler) Run(ctx context.Context, startingPeers []*peer.AddrInfo, handleSuccess HandleQueryResult, handleFail HandleQueryFail) {
	jobs := make(chan peer.ID, 1)
	results := make(chan *queryResult, 1)

	peerAddrs := newPeerAddrs()

	// Start worker goroutines
	var wg sync.WaitGroup
	wg.Add(c.parallelism)
	for i := 0; i < c.parallelism; i++ {
		go func() {
			defer wg.Done()
			for p := range jobs {
				qctx, cancel := context.WithTimeout(ctx, c.queryTimeout)
				ai := peerAddrs.PeerInfo(p)
				res := c.queryPeer(qctx, ai)
				cancel() // do not defer, cleanup after each job
				results <- res
			}
		}()
	}

	defer wg.Wait()
	defer close(jobs)

	var toDial []*peer.AddrInfo
	peersSeen := make(map[peer.ID]struct{})

	numSkipped := 0
	peerAddrs.lk.Lock()
	for _, ai := range startingPeers {
		extendAddrs := c.host.Peerstore().Addrs(ai.ID)
		if len(ai.Addrs) > 0 {
			extendAddrs = append(extendAddrs, ai.Addrs...)
		}
		if len(extendAddrs) == 0 {
			numSkipped++
			continue
		}
		peerAddrs.addPeerAddrsNoLock(ai.ID, extendAddrs)

		toDial = append(toDial, ai)
		peersSeen[ai.ID] = struct{}{}
	}
	peerAddrs.lk.Unlock()

	if numSkipped > 0 {
		logger.Infof("%d starting peers were skipped due to lack of addresses. Starting crawl with %d peers", numSkipped, len(toDial))
	}

	peersQueried := make(map[peer.ID]struct{})
	outstanding := 0

	for len(toDial) > 0 || outstanding > 0 {
		var jobCh chan peer.ID
		var nextPeerID peer.ID
		if len(toDial) > 0 {
			jobCh = jobs
			nextPeerID = toDial[0].ID
		}

		select {
		case res := <-results:
			if len(res.data) > 0 {
				logger.Debugf("peer %v had %d peers", res.peer, len(res.data))
				rtPeers := make([]*peer.AddrInfo, 0, len(res.data))
				addrsToUpdate := make(map[peer.ID][]ma.Multiaddr)
				for p, ai := range res.data {
					if _, ok := peersQueried[p]; !ok {
						addrsToUpdate[p] = ai.Addrs
					}
					if _, ok := peersSeen[p]; !ok {
						peersSeen[p] = struct{}{}
						toDial = append(toDial, ai)
					}
					rtPeers = append(rtPeers, ai)
				}
				peersQueried[res.peer] = struct{}{}
				peerAddrs.RemoveSourceAndAddPeers(res.peer, addrsToUpdate)

				if handleSuccess != nil {
					handleSuccess(res.peer, rtPeers)
				}
			} else if handleFail != nil {
				handleFail(res.peer, res.err)
			}
			outstanding--
		case jobCh <- nextPeerID:
			outstanding++
			toDial = toDial[1:]
			logger.Debugf("starting %d out of %d", len(peersQueried)+1, len(peersSeen))
		}
	}
}

type queryResult struct {
	peer peer.ID
	data map[peer.ID]*peer.AddrInfo
	err  error
}

func (c *DefaultCrawler) queryPeer(ctx context.Context, nextPeer peer.AddrInfo) *queryResult {
	tmpRT, err := kbucket.NewRoutingTable(20, kbucket.ConvertPeerID(nextPeer.ID), time.Hour, c.host.Peerstore(), time.Hour, nil)
	if err != nil {
		logger.Errorf("error creating rt for peer %v : %v", nextPeer.ID, err)
		return &queryResult{nextPeer.ID, nil, err}
	}

	connCtx, cancel := context.WithTimeout(ctx, c.connectTimeout)
	defer cancel()
	err = c.host.Connect(connCtx, nextPeer)
	if err != nil {
		logger.Debugf("could not connect to peer %v: %v", nextPeer.ID, err)
		return &queryResult{nextPeer.ID, nil, err}
	}

	localPeers := make(map[peer.ID]*peer.AddrInfo)
	var retErr error
	for cpl := 0; cpl <= 15; cpl++ {
		generatePeer, err := tmpRT.GenRandPeerID(uint(cpl))
		if err != nil {
			panic(err)
		}
		peers, err := c.dhtRPC.GetClosestPeers(ctx, nextPeer.ID, generatePeer)
		if err != nil {
			logger.Debugf("error finding data on peer %v with cpl %d : %v", nextPeer.ID, cpl, err)
			retErr = err
			break
		}
		for _, ai := range peers {
			if _, ok := localPeers[ai.ID]; !ok {
				localPeers[ai.ID] = ai
			}
		}
	}

	if retErr != nil {
		return &queryResult{nextPeer.ID, nil, retErr}
	}

	return &queryResult{nextPeer.ID, localPeers, retErr}
}
