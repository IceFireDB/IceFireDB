package pubsub

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/record"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
)

const (
	// GossipSubID_v10 is the protocol ID for version 1.0.0 of the GossipSub protocol.
	// It is advertised along with GossipSubID_v11 and GossipSubID_v12 for backwards compatibility.
	GossipSubID_v10 = protocol.ID("/meshsub/1.0.0")

	// GossipSubID_v11 is the protocol ID for version 1.1.0 of the GossipSub protocol.
	// It is advertised along with GossipSubID_v12 for backwards compatibility.
	// See the spec for details about how v1.1.0 compares to v1.0.0:
	// https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md
	GossipSubID_v11 = protocol.ID("/meshsub/1.1.0")

	// GossipSubID_v12 is the protocol ID for version 1.2.0 of the GossipSub protocol.
	// See the spec for details about how v1.2.0 compares to v1.1.0:
	// https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.2.md
	GossipSubID_v12 = protocol.ID("/meshsub/1.2.0")
)

// Defines the default gossipsub parameters.
var (
	GossipSubD                                = 6
	GossipSubDlo                              = 5
	GossipSubDhi                              = 12
	GossipSubDscore                           = 4
	GossipSubDout                             = 2
	GossipSubHistoryLength                    = 5
	GossipSubHistoryGossip                    = 3
	GossipSubDlazy                            = 6
	GossipSubGossipFactor                     = 0.25
	GossipSubGossipRetransmission             = 3
	GossipSubHeartbeatInitialDelay            = 100 * time.Millisecond
	GossipSubHeartbeatInterval                = 1 * time.Second
	GossipSubFanoutTTL                        = 60 * time.Second
	GossipSubPrunePeers                       = 16
	GossipSubPruneBackoff                     = time.Minute
	GossipSubUnsubscribeBackoff               = 10 * time.Second
	GossipSubConnectors                       = 8
	GossipSubMaxPendingConnections            = 128
	GossipSubConnectionTimeout                = 30 * time.Second
	GossipSubDirectConnectTicks        uint64 = 300
	GossipSubDirectConnectInitialDelay        = time.Second
	GossipSubOpportunisticGraftTicks   uint64 = 60
	GossipSubOpportunisticGraftPeers          = 2
	GossipSubGraftFloodThreshold              = 10 * time.Second
	GossipSubMaxIHaveLength                   = 5000
	GossipSubMaxIHaveMessages                 = 10
	GossipSubMaxIDontWantMessages             = 1000
	GossipSubIWantFollowupTime                = 3 * time.Second
	GossipSubIDontWantMessageThreshold        = 1024 // 1KB
	GossipSubIDontWantMessageTTL              = 3    // 3 heartbeats
)

type checksum struct {
	payload [32]byte
	length  uint8
}

// GossipSubParams defines all the gossipsub specific parameters.
type GossipSubParams struct {
	// overlay parameters.

	// D sets the optimal degree for a GossipSub topic mesh. For example, if D == 6,
	// each peer will want to have about six peers in their mesh for each topic they're subscribed to.
	// D should be set somewhere between Dlo and Dhi.
	D int

	// Dlo sets the lower bound on the number of peers we keep in a GossipSub topic mesh.
	// If we have fewer than Dlo peers, we will attempt to graft some more into the mesh at
	// the next heartbeat.
	Dlo int

	// Dhi sets the upper bound on the number of peers we keep in a GossipSub topic mesh.
	// If we have more than Dhi peers, we will select some to prune from the mesh at the next heartbeat.
	Dhi int

	// Dscore affects how peers are selected when pruning a mesh due to over subscription.
	// At least Dscore of the retained peers will be high-scoring, while the remainder are
	// chosen randomly.
	Dscore int

	// Dout sets the quota for the number of outbound connections to maintain in a topic mesh.
	// When the mesh is pruned due to over subscription, we make sure that we have outbound connections
	// to at least Dout of the survivor peers. This prevents sybil attackers from overwhelming
	// our mesh with incoming connections.
	//
	// Dout must be set below Dlo, and must not exceed D / 2.
	Dout int

	// gossip parameters

	// HistoryLength controls the size of the message cache used for gossip.
	// The message cache will remember messages for HistoryLength heartbeats.
	HistoryLength int

	// HistoryGossip controls how many cached message ids we will advertise in
	// IHAVE gossip messages. When asked for our seen message IDs, we will return
	// only those from the most recent HistoryGossip heartbeats. The slack between
	// HistoryGossip and HistoryLength allows us to avoid advertising messages
	// that will be expired by the time they're requested.
	//
	// HistoryGossip must be less than or equal to HistoryLength to
	// avoid a runtime panic.
	HistoryGossip int

	// Dlazy affects how many peers we will emit gossip to at each heartbeat.
	// We will send gossip to at least Dlazy peers outside our mesh. The actual
	// number may be more, depending on GossipFactor and how many peers we're
	// connected to.
	Dlazy int

	// GossipFactor affects how many peers we will emit gossip to at each heartbeat.
	// We will send gossip to GossipFactor * (total number of non-mesh peers), or
	// Dlazy, whichever is greater.
	GossipFactor float64

	// GossipRetransmission controls how many times we will allow a peer to request
	// the same message id through IWANT gossip before we start ignoring them. This is designed
	// to prevent peers from spamming us with requests and wasting our resources.
	GossipRetransmission int

	// heartbeat interval

	// HeartbeatInitialDelay is the short delay before the heartbeat timer begins
	// after the router is initialized.
	HeartbeatInitialDelay time.Duration

	// HeartbeatInterval controls the time between heartbeats.
	HeartbeatInterval time.Duration

	// SlowHeartbeatWarning is the duration threshold for heartbeat processing before emitting
	// a warning; this would be indicative of an overloaded peer.
	SlowHeartbeatWarning float64

	// FanoutTTL controls how long we keep track of the fanout state. If it's been
	// FanoutTTL since we've published to a topic that we're not subscribed to,
	// we'll delete the fanout map for that topic.
	FanoutTTL time.Duration

	// PrunePeers controls the number of peers to include in prune Peer eXchange.
	// When we prune a peer that's eligible for PX (has a good score, etc), we will try to
	// send them signed peer records for up to PrunePeers other peers that we
	// know of.
	PrunePeers int

	// PruneBackoff controls the backoff time for pruned peers. This is how long
	// a peer must wait before attempting to graft into our mesh again after being pruned.
	// When pruning a peer, we send them our value of PruneBackoff so they know
	// the minimum time to wait. Peers running older versions may not send a backoff time,
	// so if we receive a prune message without one, we will wait at least PruneBackoff
	// before attempting to re-graft.
	PruneBackoff time.Duration

	// UnsubscribeBackoff controls the backoff time to use when unsuscribing
	// from a topic. A peer should not resubscribe to this topic before this
	// duration.
	UnsubscribeBackoff time.Duration

	// Connectors controls the number of active connection attempts for peers obtained through PX.
	Connectors int

	// MaxPendingConnections sets the maximum number of pending connections for peers attempted through px.
	MaxPendingConnections int

	// ConnectionTimeout controls the timeout for connection attempts.
	ConnectionTimeout time.Duration

	// DirectConnectTicks is the number of heartbeat ticks for attempting to reconnect direct peers
	// that are not currently connected.
	DirectConnectTicks uint64

	// DirectConnectInitialDelay is the initial delay before opening connections to direct peers
	DirectConnectInitialDelay time.Duration

	// OpportunisticGraftTicks is the number of heartbeat ticks for attempting to improve the mesh
	// with opportunistic grafting. Every OpportunisticGraftTicks we will attempt to select some
	// high-scoring mesh peers to replace lower-scoring ones, if the median score of our mesh peers falls
	// below a threshold (see https://godoc.org/github.com/libp2p/go-libp2p-pubsub#PeerScoreThresholds).
	OpportunisticGraftTicks uint64

	// OpportunisticGraftPeers is the number of peers to opportunistically graft.
	OpportunisticGraftPeers int

	// If a GRAFT comes before GraftFloodThreshold has elapsed since the last PRUNE,
	// then there is an extra score penalty applied to the peer through P7.
	GraftFloodThreshold time.Duration

	// MaxIHaveLength is the maximum number of messages to include in an IHAVE message.
	// Also controls the maximum number of IHAVE ids we will accept and request with IWANT from a
	// peer within a heartbeat, to protect from IHAVE floods. You should adjust this value from the
	// default if your system is pushing more than 5000 messages in HistoryGossip heartbeats;
	// with the defaults this is 1666 messages/s.
	MaxIHaveLength int

	// MaxIHaveMessages is the maximum number of IHAVE messages to accept from a peer within a heartbeat.
	MaxIHaveMessages int

	// MaxIDontWantMessages is the maximum number of IDONTWANT messages to accept from a peer within a heartbeat.
	MaxIDontWantMessages int

	// Time to wait for a message requested through IWANT following an IHAVE advertisement.
	// If the message is not received within this window, a broken promise is declared and
	// the router may apply bahavioural penalties.
	IWantFollowupTime time.Duration

	// IDONTWANT is only sent for messages larger than the threshold. This should be greater than
	// D_high * the size of the message id. Otherwise, the attacker can do the amplication attack by sending
	// small messages while the receiver replies back with larger IDONTWANT messages.
	IDontWantMessageThreshold int

	// IDONTWANT is cleared when it's older than the TTL.
	IDontWantMessageTTL int
}

// NewGossipSub returns a new PubSub object using the default GossipSubRouter as the router.
func NewGossipSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := DefaultGossipSubRouter(h)
	opts = append(opts, WithRawTracer(rt.tagTracer))
	return NewGossipSubWithRouter(ctx, h, rt, opts...)
}

// NewGossipSubWithRouter returns a new PubSub object using the given router.
func NewGossipSubWithRouter(ctx context.Context, h host.Host, rt PubSubRouter, opts ...Option) (*PubSub, error) {
	return NewPubSub(ctx, h, rt, opts...)
}

// DefaultGossipSubRouter returns a new GossipSubRouter with default parameters.
func DefaultGossipSubRouter(h host.Host) *GossipSubRouter {
	params := DefaultGossipSubParams()
	return &GossipSubRouter{
		peers:        make(map[peer.ID]protocol.ID),
		mesh:         make(map[string]map[peer.ID]struct{}),
		fanout:       make(map[string]map[peer.ID]struct{}),
		lastpub:      make(map[string]int64),
		gossip:       make(map[peer.ID][]*pb.ControlIHave),
		control:      make(map[peer.ID]*pb.ControlMessage),
		backoff:      make(map[string]map[peer.ID]time.Time),
		peerhave:     make(map[peer.ID]int),
		peerdontwant: make(map[peer.ID]int),
		unwanted:     make(map[peer.ID]map[checksum]int),
		iasked:       make(map[peer.ID]int),
		outbound:     make(map[peer.ID]bool),
		connect:      make(chan connectInfo, params.MaxPendingConnections),
		cab:          pstoremem.NewAddrBook(),
		mcache:       NewMessageCache(params.HistoryGossip, params.HistoryLength),
		protos:       GossipSubDefaultProtocols,
		feature:      GossipSubDefaultFeatures,
		tagTracer:    newTagTracer(h.ConnManager()),
		params:       params,
	}
}

// DefaultGossipSubParams returns the default gossip sub parameters
// as a config.
func DefaultGossipSubParams() GossipSubParams {
	return GossipSubParams{
		D:                         GossipSubD,
		Dlo:                       GossipSubDlo,
		Dhi:                       GossipSubDhi,
		Dscore:                    GossipSubDscore,
		Dout:                      GossipSubDout,
		HistoryLength:             GossipSubHistoryLength,
		HistoryGossip:             GossipSubHistoryGossip,
		Dlazy:                     GossipSubDlazy,
		GossipFactor:              GossipSubGossipFactor,
		GossipRetransmission:      GossipSubGossipRetransmission,
		HeartbeatInitialDelay:     GossipSubHeartbeatInitialDelay,
		HeartbeatInterval:         GossipSubHeartbeatInterval,
		FanoutTTL:                 GossipSubFanoutTTL,
		PrunePeers:                GossipSubPrunePeers,
		PruneBackoff:              GossipSubPruneBackoff,
		UnsubscribeBackoff:        GossipSubUnsubscribeBackoff,
		Connectors:                GossipSubConnectors,
		MaxPendingConnections:     GossipSubMaxPendingConnections,
		ConnectionTimeout:         GossipSubConnectionTimeout,
		DirectConnectTicks:        GossipSubDirectConnectTicks,
		DirectConnectInitialDelay: GossipSubDirectConnectInitialDelay,
		OpportunisticGraftTicks:   GossipSubOpportunisticGraftTicks,
		OpportunisticGraftPeers:   GossipSubOpportunisticGraftPeers,
		GraftFloodThreshold:       GossipSubGraftFloodThreshold,
		MaxIHaveLength:            GossipSubMaxIHaveLength,
		MaxIHaveMessages:          GossipSubMaxIHaveMessages,
		MaxIDontWantMessages:      GossipSubMaxIDontWantMessages,
		IWantFollowupTime:         GossipSubIWantFollowupTime,
		IDontWantMessageThreshold: GossipSubIDontWantMessageThreshold,
		IDontWantMessageTTL:       GossipSubIDontWantMessageTTL,
		SlowHeartbeatWarning:      0.1,
	}
}

// WithPeerScore is a gossipsub router option that enables peer scoring.
func WithPeerScore(params *PeerScoreParams, thresholds *PeerScoreThresholds) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		// sanity check: validate the score parameters
		err := params.validate()
		if err != nil {
			return err
		}

		// sanity check: validate the threshold values
		err = thresholds.validate()
		if err != nil {
			return err
		}

		gs.score = newPeerScore(params)
		gs.gossipThreshold = thresholds.GossipThreshold
		gs.publishThreshold = thresholds.PublishThreshold
		gs.graylistThreshold = thresholds.GraylistThreshold
		gs.acceptPXThreshold = thresholds.AcceptPXThreshold
		gs.opportunisticGraftThreshold = thresholds.OpportunisticGraftThreshold

		gs.gossipTracer = newGossipTracer()

		// hook the tracer
		if ps.tracer != nil {
			ps.tracer.raw = append(ps.tracer.raw, gs.score, gs.gossipTracer)
		} else {
			ps.tracer = &pubsubTracer{
				raw:   []RawTracer{gs.score, gs.gossipTracer},
				pid:   ps.host.ID(),
				idGen: ps.idGen,
			}
		}

		return nil
	}
}

// WithFloodPublish is a gossipsub router option that enables flood publishing.
// When this is enabled, published messages are forwarded to all peers with score >=
// to publishThreshold
func WithFloodPublish(floodPublish bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		gs.floodPublish = floodPublish

		return nil
	}
}

// WithPeerExchange is a gossipsub router option that enables Peer eXchange on PRUNE.
// This should generally be enabled in bootstrappers and well connected/trusted nodes
// used for bootstrapping.
func WithPeerExchange(doPX bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		gs.doPX = doPX

		return nil
	}
}

// WithDirectPeers is a gossipsub router option that specifies peers with direct
// peering agreements. These peers are connected outside of the mesh, with all (valid)
// message unconditionally forwarded to them. The router will maintain open connections
// to these peers. Note that the peering agreement should be reciprocal with direct peers
// symmetrically configured at both ends.
func WithDirectPeers(pis []peer.AddrInfo) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		direct := make(map[peer.ID]struct{})
		for _, pi := range pis {
			direct[pi.ID] = struct{}{}
			ps.host.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.PermanentAddrTTL)
		}

		gs.direct = direct

		if gs.tagTracer != nil {
			gs.tagTracer.direct = direct
		}

		return nil
	}
}

// WithDirectConnectTicks is a gossipsub router option that sets the number of
// heartbeat ticks between attempting to reconnect direct peers that are not
// currently connected. A "tick" is based on the heartbeat interval, which is
// 1s by default. The default value for direct connect ticks is 300.
func WithDirectConnectTicks(t uint64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}
		gs.params.DirectConnectTicks = t
		return nil
	}
}

// WithGossipSubParams is a gossip sub router option that allows a custom
// config to be set when instantiating the gossipsub router.
func WithGossipSubParams(cfg GossipSubParams) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}
		// Overwrite current config and associated variables in the router.
		gs.params = cfg
		gs.connect = make(chan connectInfo, cfg.MaxPendingConnections)
		gs.mcache = NewMessageCache(cfg.HistoryGossip, cfg.HistoryLength)

		return nil
	}
}

// GossipSubRouter is a router that implements the gossipsub protocol.
// For each topic we have joined, we maintain an overlay through which
// messages flow; this is the mesh map.
// For each topic we publish to without joining, we maintain a list of peers
// to use for injecting our messages in the overlay with stable routes; this
// is the fanout map. Fanout peer lists are expired if we don't publish any
// messages to their topic for GossipSubFanoutTTL.
type GossipSubRouter struct {
	p            *PubSub
	peers        map[peer.ID]protocol.ID          // peer protocols
	direct       map[peer.ID]struct{}             // direct peers
	mesh         map[string]map[peer.ID]struct{}  // topic meshes
	fanout       map[string]map[peer.ID]struct{}  // topic fanout
	lastpub      map[string]int64                 // last publish time for fanout topics
	gossip       map[peer.ID][]*pb.ControlIHave   // pending gossip
	control      map[peer.ID]*pb.ControlMessage   // pending control messages
	peerhave     map[peer.ID]int                  // number of IHAVEs received from peer in the last heartbeat
	peerdontwant map[peer.ID]int                  // number of IDONTWANTs received from peer in the last heartbeat
	unwanted     map[peer.ID]map[checksum]int     // TTL of the message ids peers don't want
	iasked       map[peer.ID]int                  // number of messages we have asked from peer in the last heartbeat
	outbound     map[peer.ID]bool                 // connection direction cache, marks peers with outbound connections
	backoff      map[string]map[peer.ID]time.Time // prune backoff
	connect      chan connectInfo                 // px connection requests
	cab          peerstore.AddrBook

	protos  []protocol.ID
	feature GossipSubFeatureTest

	mcache       *MessageCache
	tracer       *pubsubTracer
	score        *peerScore
	gossipTracer *gossipTracer
	tagTracer    *tagTracer
	gate         *peerGater

	// config for gossipsub parameters
	params GossipSubParams

	// whether PX is enabled; this should be enabled in bootstrappers and other well connected/trusted
	// nodes.
	doPX bool

	// threshold for accepting PX from a peer; this should be positive and limited to scores
	// attainable by bootstrappers and trusted nodes
	acceptPXThreshold float64

	// threshold for peer score to emit/accept gossip
	// If the peer score is below this threshold, we won't emit or accept gossip from the peer.
	// When there is no score, this value is 0.
	gossipThreshold float64

	// flood publish score threshold; we only publish to peers with score >= to the threshold
	// when using flood publishing or the peer is a fanout or floodsub peer.
	publishThreshold float64

	// threshold for peer score before we graylist the peer and silently ignore its RPCs
	graylistThreshold float64

	// threshold for median peer score before triggering opportunistic grafting
	opportunisticGraftThreshold float64

	// whether to use flood publishing
	floodPublish bool

	// number of heartbeats since the beginning of time; this allows us to amortize some resource
	// clean up -- eg backoff clean up.
	heartbeatTicks uint64
}

type connectInfo struct {
	p   peer.ID
	spr *record.Envelope
}

func (gs *GossipSubRouter) Protocols() []protocol.ID {
	return gs.protos
}

func (gs *GossipSubRouter) Attach(p *PubSub) {
	gs.p = p
	gs.tracer = p.tracer

	// start the scoring
	gs.score.Start(gs)

	// and the gossip tracing
	gs.gossipTracer.Start(gs)

	// and the tracer for connmgr tags
	gs.tagTracer.Start(gs)

	// start using the same msg ID function as PubSub for caching messages.
	gs.mcache.SetMsgIdFn(p.idGen.ID)

	// start the heartbeat
	go gs.heartbeatTimer()

	// start the PX connectors
	for i := 0; i < gs.params.Connectors; i++ {
		go gs.connector()
	}

	// Manage our address book from events emitted by libp2p
	go gs.manageAddrBook()

	// connect to direct peers
	if len(gs.direct) > 0 {
		go func() {
			if gs.params.DirectConnectInitialDelay > 0 {
				time.Sleep(gs.params.DirectConnectInitialDelay)
			}
			for p := range gs.direct {
				gs.connect <- connectInfo{p: p}
			}
		}()
	}
}

func (gs *GossipSubRouter) manageAddrBook() {
	sub, err := gs.p.host.EventBus().Subscribe([]interface{}{
		&event.EvtPeerIdentificationCompleted{},
		&event.EvtPeerConnectednessChanged{},
	})
	if err != nil {
		log.Errorf("failed to subscribe to peer identification events: %v", err)
		return
	}
	defer sub.Close()

	for {
		select {
		case <-gs.p.ctx.Done():
			cabCloser, ok := gs.cab.(io.Closer)
			if ok {
				errClose := cabCloser.Close()
				if errClose != nil {
					log.Warnf("failed to close addr book: %v", errClose)
				}
			}
			return
		case ev := <-sub.Out():
			switch ev := ev.(type) {
			case event.EvtPeerIdentificationCompleted:
				if ev.SignedPeerRecord != nil {
					cab, ok := peerstore.GetCertifiedAddrBook(gs.cab)
					if ok {
						ttl := peerstore.RecentlyConnectedAddrTTL
						if gs.p.host.Network().Connectedness(ev.Peer) == network.Connected {
							ttl = peerstore.ConnectedAddrTTL
						}
						_, err := cab.ConsumePeerRecord(ev.SignedPeerRecord, ttl)
						if err != nil {
							log.Warnf("failed to consume signed peer record: %v", err)
						}
					}
				}
			case event.EvtPeerConnectednessChanged:
				if ev.Connectedness != network.Connected {
					gs.cab.UpdateAddrs(ev.Peer, peerstore.ConnectedAddrTTL, peerstore.RecentlyConnectedAddrTTL)
				}
			}
		}
	}
}

func (gs *GossipSubRouter) AddPeer(p peer.ID, proto protocol.ID) {
	log.Debugf("PEERUP: Add new peer %s using %s", p, proto)
	gs.tracer.AddPeer(p, proto)
	gs.peers[p] = proto

	// track the connection direction
	outbound := false
	conns := gs.p.host.Network().ConnsToPeer(p)
loop:
	for _, c := range conns {
		stat := c.Stat()

		if stat.Limited {
			continue
		}

		if stat.Direction == network.DirOutbound {
			// only count the connection if it has a pubsub stream
			for _, s := range c.GetStreams() {
				if s.Protocol() == proto {
					outbound = true
					break loop
				}
			}
		}
	}
	gs.outbound[p] = outbound
}

func (gs *GossipSubRouter) RemovePeer(p peer.ID) {
	log.Debugf("PEERDOWN: Remove disconnected peer %s", p)
	gs.tracer.RemovePeer(p)
	delete(gs.peers, p)
	for _, peers := range gs.mesh {
		delete(peers, p)
	}
	for _, peers := range gs.fanout {
		delete(peers, p)
	}
	delete(gs.gossip, p)
	delete(gs.control, p)
	delete(gs.outbound, p)
}

func (gs *GossipSubRouter) EnoughPeers(topic string, suggested int) bool {
	// check all peers in the topic
	tmap, ok := gs.p.topics[topic]
	if !ok {
		return false
	}

	fsPeers, gsPeers := 0, 0
	// floodsub peers
	for p := range tmap {
		if !gs.feature(GossipSubFeatureMesh, gs.peers[p]) {
			fsPeers++
		}
	}

	// gossipsub peers
	gsPeers = len(gs.mesh[topic])

	if suggested == 0 {
		suggested = gs.params.Dlo
	}

	if fsPeers+gsPeers >= suggested || gsPeers >= gs.params.Dhi {
		return true
	}

	return false
}

func (gs *GossipSubRouter) AcceptFrom(p peer.ID) AcceptStatus {
	_, direct := gs.direct[p]
	if direct {
		return AcceptAll
	}

	if gs.score.Score(p) < gs.graylistThreshold {
		return AcceptNone
	}

	return gs.gate.AcceptFrom(p)
}

// PreValidation sends the IDONTWANT control messages to all the mesh
// peers. They need to be sent right before the validation because they
// should be seen by the peers as soon as possible.
func (gs *GossipSubRouter) PreValidation(msgs []*Message) {
	tmids := make(map[string][]string)
	for _, msg := range msgs {
		if len(msg.GetData()) < gs.params.IDontWantMessageThreshold {
			continue
		}
		topic := msg.GetTopic()
		tmids[topic] = append(tmids[topic], gs.p.idGen.ID(msg))
	}
	for topic, mids := range tmids {
		if len(mids) == 0 {
			continue
		}
		// shuffle the messages got from the RPC envelope
		shuffleStrings(mids)
		// send IDONTWANT to all the mesh peers
		for p := range gs.mesh[topic] {
			// send to only peers that support IDONTWANT
			if gs.feature(GossipSubFeatureIdontwant, gs.peers[p]) {
				idontwant := []*pb.ControlIDontWant{{MessageIDs: mids}}
				out := rpcWithControl(nil, nil, nil, nil, nil, idontwant)
				gs.sendRPC(p, out, true)
			}
		}
	}
}

func (gs *GossipSubRouter) HandleRPC(rpc *RPC) {
	ctl := rpc.GetControl()
	if ctl == nil {
		return
	}

	iwant := gs.handleIHave(rpc.from, ctl)
	ihave := gs.handleIWant(rpc.from, ctl)
	prune := gs.handleGraft(rpc.from, ctl)
	gs.handlePrune(rpc.from, ctl)
	gs.handleIDontWant(rpc.from, ctl)

	if len(iwant) == 0 && len(ihave) == 0 && len(prune) == 0 {
		return
	}

	out := rpcWithControl(ihave, nil, iwant, nil, prune, nil)
	gs.sendRPC(rpc.from, out, false)
}

func (gs *GossipSubRouter) handleIHave(p peer.ID, ctl *pb.ControlMessage) []*pb.ControlIWant {
	// we ignore IHAVE gossip from any peer whose score is below the gossip threshold
	score := gs.score.Score(p)
	if score < gs.gossipThreshold {
		log.Debugf("IHAVE: ignoring peer %s with score below threshold [score = %f]", p, score)
		return nil
	}

	// IHAVE flood protection
	gs.peerhave[p]++
	if gs.peerhave[p] > gs.params.MaxIHaveMessages {
		log.Debugf("IHAVE: peer %s has advertised too many times (%d) within this heartbeat interval; ignoring", p, gs.peerhave[p])
		return nil
	}
	if gs.iasked[p] >= gs.params.MaxIHaveLength {
		log.Debugf("IHAVE: peer %s has already advertised too many messages (%d); ignoring", p, gs.iasked[p])
		return nil
	}

	iwant := make(map[string]struct{})
	for _, ihave := range ctl.GetIhave() {
		topic := ihave.GetTopicID()
		_, ok := gs.mesh[topic]
		if !ok {
			continue
		}

		if !gs.p.peerFilter(p, topic) {
			continue
		}

	checkIwantMsgsLoop:
		for msgIdx, mid := range ihave.GetMessageIDs() {
			// prevent remote peer from sending too many msg_ids on a single IHAVE message
			if msgIdx >= gs.params.MaxIHaveLength {
				log.Debugf("IHAVE: peer %s has sent IHAVE on topic %s with too many messages (%d); ignoring remaining msgs", p, topic, len(ihave.MessageIDs))
				break checkIwantMsgsLoop
			}

			if gs.p.seenMessage(mid) {
				continue
			}
			iwant[mid] = struct{}{}
		}
	}

	if len(iwant) == 0 {
		return nil
	}

	iask := len(iwant)
	if iask+gs.iasked[p] > gs.params.MaxIHaveLength {
		iask = gs.params.MaxIHaveLength - gs.iasked[p]
	}

	log.Debugf("IHAVE: Asking for %d out of %d messages from %s", iask, len(iwant), p)

	iwantlst := make([]string, 0, len(iwant))
	for mid := range iwant {
		iwantlst = append(iwantlst, mid)
	}

	// ask in random order
	shuffleStrings(iwantlst)

	// truncate to the messages we are actually asking for and update the iasked counter
	iwantlst = iwantlst[:iask]
	gs.iasked[p] += iask

	gs.gossipTracer.AddPromise(p, iwantlst)

	return []*pb.ControlIWant{{MessageIDs: iwantlst}}
}

func (gs *GossipSubRouter) handleIWant(p peer.ID, ctl *pb.ControlMessage) []*pb.Message {
	// we don't respond to IWANT requests from any peer whose score is below the gossip threshold
	score := gs.score.Score(p)
	if score < gs.gossipThreshold {
		log.Debugf("IWANT: ignoring peer %s with score below threshold [score = %f]", p, score)
		return nil
	}

	ihave := make(map[string]*pb.Message)
	for _, iwant := range ctl.GetIwant() {
		for _, mid := range iwant.GetMessageIDs() {
			msg, count, ok := gs.mcache.GetForPeer(mid, p)
			if !ok {
				continue
			}

			if !gs.p.peerFilter(p, msg.GetTopic()) {
				continue
			}

			if count > gs.params.GossipRetransmission {
				log.Debugf("IWANT: Peer %s has asked for message %s too many times; ignoring request", p, mid)
				continue
			}

			ihave[mid] = msg.Message
		}
	}

	if len(ihave) == 0 {
		return nil
	}

	log.Debugf("IWANT: Sending %d messages to %s", len(ihave), p)

	msgs := make([]*pb.Message, 0, len(ihave))
	for _, msg := range ihave {
		msgs = append(msgs, msg)
	}

	return msgs
}

func (gs *GossipSubRouter) handleGraft(p peer.ID, ctl *pb.ControlMessage) []*pb.ControlPrune {
	var prune []string

	doPX := gs.doPX
	score := gs.score.Score(p)
	now := time.Now()

	for _, graft := range ctl.GetGraft() {
		topic := graft.GetTopicID()

		if !gs.p.peerFilter(p, topic) {
			continue
		}

		peers, ok := gs.mesh[topic]
		if !ok {
			// don't do PX when there is an unknown topic to avoid leaking our peers
			doPX = false
			// spam hardening: ignore GRAFTs for unknown topics
			continue
		}

		// check if it is already in the mesh; if so do nothing (we might have concurrent grafting)
		_, inMesh := peers[p]
		if inMesh {
			continue
		}

		// we don't GRAFT to/from direct peers; complain loudly if this happens
		_, direct := gs.direct[p]
		if direct {
			log.Warnf("GRAFT: ignoring request from direct peer %s", p)
			// this is possibly a bug from non-reciprocal configuration; send a PRUNE
			prune = append(prune, topic)
			// but don't PX
			doPX = false
			continue
		}

		// make sure we are not backing off that peer
		expire, backoff := gs.backoff[topic][p]
		if backoff && now.Before(expire) {
			log.Debugf("GRAFT: ignoring backed off peer %s", p)
			// add behavioural penalty
			gs.score.AddPenalty(p, 1)
			// no PX
			doPX = false
			// check the flood cutoff -- is the GRAFT coming too fast?
			floodCutoff := expire.Add(gs.params.GraftFloodThreshold - gs.params.PruneBackoff)
			if now.Before(floodCutoff) {
				// extra penalty
				gs.score.AddPenalty(p, 1)
			}
			// refresh the backoff
			gs.addBackoff(p, topic, false)
			prune = append(prune, topic)
			continue
		}

		// check the score
		if score < 0 {
			// we don't GRAFT peers with negative score
			log.Debugf("GRAFT: ignoring peer %s with negative score [score = %f, topic = %s]", p, score, topic)
			// we do send them PRUNE however, because it's a matter of protocol correctness
			prune = append(prune, topic)
			// but we won't PX to them
			doPX = false
			// add/refresh backoff so that we don't reGRAFT too early even if the score decays back up
			gs.addBackoff(p, topic, false)
			continue
		}

		// check the number of mesh peers; if it is at (or over) Dhi, we only accept grafts
		// from peers with outbound connections; this is a defensive check to restrict potential
		// mesh takeover attacks combined with love bombing
		if len(peers) >= gs.params.Dhi && !gs.outbound[p] {
			prune = append(prune, topic)
			gs.addBackoff(p, topic, false)
			continue
		}

		log.Debugf("GRAFT: add mesh link from %s in %s", p, topic)
		gs.tracer.Graft(p, topic)
		peers[p] = struct{}{}
	}

	if len(prune) == 0 {
		return nil
	}

	cprune := make([]*pb.ControlPrune, 0, len(prune))
	for _, topic := range prune {
		cprune = append(cprune, gs.makePrune(p, topic, doPX, false))
	}

	return cprune
}

func (gs *GossipSubRouter) handlePrune(p peer.ID, ctl *pb.ControlMessage) {
	score := gs.score.Score(p)

	for _, prune := range ctl.GetPrune() {
		topic := prune.GetTopicID()
		peers, ok := gs.mesh[topic]
		if !ok {
			continue
		}

		log.Debugf("PRUNE: Remove mesh link to %s in %s", p, topic)
		gs.tracer.Prune(p, topic)
		delete(peers, p)
		// is there a backoff specified by the peer? if so obey it.
		backoff := prune.GetBackoff()
		if backoff > 0 {
			gs.doAddBackoff(p, topic, time.Duration(backoff)*time.Second)
		} else {
			gs.addBackoff(p, topic, false)
		}

		px := prune.GetPeers()
		if len(px) > 0 {
			// we ignore PX from peers with insufficient score
			if score < gs.acceptPXThreshold {
				log.Debugf("PRUNE: ignoring PX from peer %s with insufficient score [score = %f, topic = %s]", p, score, topic)
				continue
			}

			gs.pxConnect(px)
		}
	}
}

func (gs *GossipSubRouter) handleIDontWant(p peer.ID, ctl *pb.ControlMessage) {
	if gs.unwanted[p] == nil {
		gs.unwanted[p] = make(map[checksum]int)
	}

	// IDONTWANT flood protection
	if gs.peerdontwant[p] >= gs.params.MaxIDontWantMessages {
		log.Debugf("IDONWANT: peer %s has advertised too many times (%d) within this heartbeat interval; ignoring", p, gs.peerdontwant[p])
		return
	}
	gs.peerdontwant[p]++

	// Remember all the unwanted message ids
	for _, idontwant := range ctl.GetIdontwant() {
		for _, mid := range idontwant.GetMessageIDs() {
			gs.unwanted[p][computeChecksum(mid)] = gs.params.IDontWantMessageTTL
		}
	}
}

func (gs *GossipSubRouter) addBackoff(p peer.ID, topic string, isUnsubscribe bool) {
	backoff := gs.params.PruneBackoff
	if isUnsubscribe {
		backoff = gs.params.UnsubscribeBackoff
	}
	gs.doAddBackoff(p, topic, backoff)
}

func (gs *GossipSubRouter) doAddBackoff(p peer.ID, topic string, interval time.Duration) {
	backoff, ok := gs.backoff[topic]
	if !ok {
		backoff = make(map[peer.ID]time.Time)
		gs.backoff[topic] = backoff
	}
	expire := time.Now().Add(interval)
	if backoff[p].Before(expire) {
		backoff[p] = expire
	}
}

func (gs *GossipSubRouter) pxConnect(peers []*pb.PeerInfo) {
	if len(peers) > gs.params.PrunePeers {
		shufflePeerInfo(peers)
		peers = peers[:gs.params.PrunePeers]
	}

	toconnect := make([]connectInfo, 0, len(peers))

	for _, pi := range peers {
		p := peer.ID(pi.PeerID)

		_, connected := gs.peers[p]
		if connected {
			continue
		}

		var spr *record.Envelope
		if pi.SignedPeerRecord != nil {
			// the peer sent us a signed record; ensure that it is valid
			envelope, r, err := record.ConsumeEnvelope(pi.SignedPeerRecord, peer.PeerRecordEnvelopeDomain)
			if err != nil {
				log.Warnf("error unmarshalling peer record obtained through px: %s", err)
				continue
			}
			rec, ok := r.(*peer.PeerRecord)
			if !ok {
				log.Warnf("bogus peer record obtained through px: envelope payload is not PeerRecord")
				continue
			}
			if rec.PeerID != p {
				log.Warnf("bogus peer record obtained through px: peer ID %s doesn't match expected peer %s", rec.PeerID, p)
				continue
			}
			spr = envelope
		}

		toconnect = append(toconnect, connectInfo{p, spr})
	}

	if len(toconnect) == 0 {
		return
	}

	for _, ci := range toconnect {
		select {
		case gs.connect <- ci:
		default:
			log.Debugf("ignoring peer connection attempt; too many pending connections")
		}
	}
}

func (gs *GossipSubRouter) connector() {
	for {
		select {
		case ci := <-gs.connect:
			if gs.p.host.Network().Connectedness(ci.p) == network.Connected {
				continue
			}

			log.Debugf("connecting to %s", ci.p)
			cab, ok := peerstore.GetCertifiedAddrBook(gs.cab)
			if ok && ci.spr != nil {
				_, err := cab.ConsumePeerRecord(ci.spr, peerstore.TempAddrTTL)
				if err != nil {
					log.Debugf("error processing peer record: %s", err)
				}
			}

			ctx, cancel := context.WithTimeout(gs.p.ctx, gs.params.ConnectionTimeout)
			err := gs.p.host.Connect(ctx, peer.AddrInfo{ID: ci.p, Addrs: gs.cab.Addrs(ci.p)})
			cancel()
			if err != nil {
				log.Debugf("error connecting to %s: %s", ci.p, err)
			}

		case <-gs.p.ctx.Done():
			return
		}
	}
}

func (gs *GossipSubRouter) Publish(msg *Message) {
	gs.mcache.Put(msg)

	from := msg.ReceivedFrom
	topic := msg.GetTopic()

	tosend := make(map[peer.ID]struct{})

	// any peers in the topic?
	tmap, ok := gs.p.topics[topic]
	if !ok {
		return
	}

	if gs.floodPublish && from == gs.p.host.ID() {
		for p := range tmap {
			_, direct := gs.direct[p]
			if direct || gs.score.Score(p) >= gs.publishThreshold {
				tosend[p] = struct{}{}
			}
		}
	} else {
		// direct peers
		for p := range gs.direct {
			_, inTopic := tmap[p]
			if inTopic {
				tosend[p] = struct{}{}
			}
		}

		// floodsub peers
		for p := range tmap {
			if !gs.feature(GossipSubFeatureMesh, gs.peers[p]) && gs.score.Score(p) >= gs.publishThreshold {
				tosend[p] = struct{}{}
			}
		}

		// gossipsub peers
		gmap, ok := gs.mesh[topic]
		if !ok {
			// we are not in the mesh for topic, use fanout peers
			gmap, ok = gs.fanout[topic]
			if !ok || len(gmap) == 0 {
				// we don't have any, pick some with score above the publish threshold
				peers := gs.getPeers(topic, gs.params.D, func(p peer.ID) bool {
					_, direct := gs.direct[p]
					return !direct && gs.score.Score(p) >= gs.publishThreshold
				})

				if len(peers) > 0 {
					gmap = peerListToMap(peers)
					gs.fanout[topic] = gmap
				}
			}
			gs.lastpub[topic] = time.Now().UnixNano()
		}

		for p := range gmap {
			mid := gs.p.idGen.ID(msg)
			// Check if it has already received an IDONTWANT for the message.
			// If so, don't send it to the peer
			if _, ok := gs.unwanted[p][computeChecksum(mid)]; ok {
				continue
			}
			tosend[p] = struct{}{}
		}
	}

	out := rpcWithMessages(msg.Message)
	for pid := range tosend {
		if pid == from || pid == peer.ID(msg.GetFrom()) {
			continue
		}

		gs.sendRPC(pid, out, false)
	}
}

func (gs *GossipSubRouter) Join(topic string) {
	gmap, ok := gs.mesh[topic]
	if ok {
		return
	}

	log.Debugf("JOIN %s", topic)
	gs.tracer.Join(topic)

	gmap, ok = gs.fanout[topic]
	if ok {
		backoff := gs.backoff[topic]
		// these peers have a score above the publish threshold, which may be negative
		// so drop the ones with a negative score
		for p := range gmap {
			_, doBackOff := backoff[p]
			if gs.score.Score(p) < 0 || doBackOff {
				delete(gmap, p)
			}
		}

		if len(gmap) < gs.params.D {
			// we need more peers; eager, as this would get fixed in the next heartbeat
			more := gs.getPeers(topic, gs.params.D-len(gmap), func(p peer.ID) bool {
				// filter our current peers, direct peers, peers we are backing off, and
				// peers with negative scores
				_, inMesh := gmap[p]
				_, direct := gs.direct[p]
				_, doBackOff := backoff[p]
				return !inMesh && !direct && !doBackOff && gs.score.Score(p) >= 0
			})
			for _, p := range more {
				gmap[p] = struct{}{}
			}
		}
		gs.mesh[topic] = gmap
		delete(gs.fanout, topic)
		delete(gs.lastpub, topic)
	} else {
		backoff := gs.backoff[topic]
		peers := gs.getPeers(topic, gs.params.D, func(p peer.ID) bool {
			// filter direct peers, peers we are backing off and peers with negative score
			_, direct := gs.direct[p]
			_, doBackOff := backoff[p]
			return !direct && !doBackOff && gs.score.Score(p) >= 0
		})
		gmap = peerListToMap(peers)
		gs.mesh[topic] = gmap
	}

	for p := range gmap {
		log.Debugf("JOIN: Add mesh link to %s in %s", p, topic)
		gs.tracer.Graft(p, topic)
		gs.sendGraft(p, topic)
	}
}

func (gs *GossipSubRouter) Leave(topic string) {
	gmap, ok := gs.mesh[topic]
	if !ok {
		return
	}

	log.Debugf("LEAVE %s", topic)
	gs.tracer.Leave(topic)

	delete(gs.mesh, topic)

	for p := range gmap {
		log.Debugf("LEAVE: Remove mesh link to %s in %s", p, topic)
		gs.tracer.Prune(p, topic)
		gs.sendPrune(p, topic, true)
		// Add a backoff to this peer to prevent us from eagerly
		// re-grafting this peer into our mesh if we rejoin this
		// topic before the backoff period ends.
		gs.addBackoff(p, topic, true)
	}
}

func (gs *GossipSubRouter) sendGraft(p peer.ID, topic string) {
	graft := []*pb.ControlGraft{{TopicID: &topic}}
	out := rpcWithControl(nil, nil, nil, graft, nil, nil)
	gs.sendRPC(p, out, false)
}

func (gs *GossipSubRouter) sendPrune(p peer.ID, topic string, isUnsubscribe bool) {
	prune := []*pb.ControlPrune{gs.makePrune(p, topic, gs.doPX, isUnsubscribe)}
	out := rpcWithControl(nil, nil, nil, nil, prune, nil)
	gs.sendRPC(p, out, false)
}

func (gs *GossipSubRouter) sendRPC(p peer.ID, out *RPC, urgent bool) {
	// do we own the RPC?
	own := false

	// piggyback control message retries
	ctl, ok := gs.control[p]
	if ok {
		out = copyRPC(out)
		own = true
		gs.piggybackControl(p, out, ctl)
		delete(gs.control, p)
	}

	// piggyback gossip
	ihave, ok := gs.gossip[p]
	if ok {
		if !own {
			out = copyRPC(out)
			own = true
		}
		gs.piggybackGossip(p, out, ihave)
		delete(gs.gossip, p)
	}

	q, ok := gs.p.peers[p]
	if !ok {
		return
	}

	// If we're below the max message size, go ahead and send
	if out.Size() < gs.p.maxMessageSize {
		gs.doSendRPC(out, p, q, urgent)
		return
	}

	// Potentially split the RPC into multiple RPCs that are below the max message size
	outRPCs := appendOrMergeRPC(nil, gs.p.maxMessageSize, *out)
	for _, rpc := range outRPCs {
		if rpc.Size() > gs.p.maxMessageSize {
			// This should only happen if a single message/control is above the maxMessageSize.
			gs.doDropRPC(out, p, fmt.Sprintf("Dropping oversized RPC. Size: %d, limit: %d. (Over by %d bytes)", rpc.Size(), gs.p.maxMessageSize, rpc.Size()-gs.p.maxMessageSize))
			continue
		}
		gs.doSendRPC(rpc, p, q, urgent)
	}
}

func (gs *GossipSubRouter) doDropRPC(rpc *RPC, p peer.ID, reason string) {
	log.Debugf("dropping message to peer %s: %s", p, reason)
	gs.tracer.DropRPC(rpc, p)
	// push control messages that need to be retried
	ctl := rpc.GetControl()
	if ctl != nil {
		gs.pushControl(p, ctl)
	}
}

func (gs *GossipSubRouter) doSendRPC(rpc *RPC, p peer.ID, q *rpcQueue, urgent bool) {
	var err error
	if urgent {
		err = q.UrgentPush(rpc, false)
	} else {
		err = q.Push(rpc, false)
	}
	if err != nil {
		gs.doDropRPC(rpc, p, "queue full")
		return
	}
	gs.tracer.SendRPC(rpc, p)
}

// appendOrMergeRPC appends the given RPCs to the slice, merging them if possible.
// If any elem is too large to fit in a single RPC, it will be split into multiple RPCs.
// If an RPC is too large and can't be split further (e.g. Message data is
// bigger than the RPC limit), then it will be returned as an oversized RPC.
// The caller should filter out oversized RPCs.
func appendOrMergeRPC(slice []*RPC, limit int, elems ...RPC) []*RPC {
	if len(elems) == 0 {
		return slice
	}

	if len(slice) == 0 && len(elems) == 1 && elems[0].Size() < limit {
		// Fast path: no merging needed and only one element
		return append(slice, &elems[0])
	}

	out := slice
	if len(out) == 0 {
		out = append(out, &RPC{RPC: pb.RPC{}})
		out[0].from = elems[0].from
	}

	for _, elem := range elems {
		lastRPC := out[len(out)-1]

		// Merge/Append publish messages
		// TODO: Never merge messages. The current behavior is the same as the
		// old behavior. In the future let's not merge messages. Since,
		// it may increase message latency.
		for _, msg := range elem.GetPublish() {
			if lastRPC.Publish = append(lastRPC.Publish, msg); lastRPC.Size() > limit {
				lastRPC.Publish = lastRPC.Publish[:len(lastRPC.Publish)-1]
				lastRPC = &RPC{RPC: pb.RPC{}, from: elem.from}
				lastRPC.Publish = append(lastRPC.Publish, msg)
				out = append(out, lastRPC)
			}
		}

		// Merge/Append Subscriptions
		for _, sub := range elem.GetSubscriptions() {
			if lastRPC.Subscriptions = append(lastRPC.Subscriptions, sub); lastRPC.Size() > limit {
				lastRPC.Subscriptions = lastRPC.Subscriptions[:len(lastRPC.Subscriptions)-1]
				lastRPC = &RPC{RPC: pb.RPC{}, from: elem.from}
				lastRPC.Subscriptions = append(lastRPC.Subscriptions, sub)
				out = append(out, lastRPC)
			}
		}

		// Merge/Append Control messages
		if ctl := elem.GetControl(); ctl != nil {
			if lastRPC.Control == nil {
				lastRPC.Control = &pb.ControlMessage{}
				if lastRPC.Size() > limit {
					lastRPC.Control = nil
					lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{}}, from: elem.from}
					out = append(out, lastRPC)
				}
			}

			for _, graft := range ctl.GetGraft() {
				if lastRPC.Control.Graft = append(lastRPC.Control.Graft, graft); lastRPC.Size() > limit {
					lastRPC.Control.Graft = lastRPC.Control.Graft[:len(lastRPC.Control.Graft)-1]
					lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{}}, from: elem.from}
					lastRPC.Control.Graft = append(lastRPC.Control.Graft, graft)
					out = append(out, lastRPC)
				}
			}

			for _, prune := range ctl.GetPrune() {
				if lastRPC.Control.Prune = append(lastRPC.Control.Prune, prune); lastRPC.Size() > limit {
					lastRPC.Control.Prune = lastRPC.Control.Prune[:len(lastRPC.Control.Prune)-1]
					lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{}}, from: elem.from}
					lastRPC.Control.Prune = append(lastRPC.Control.Prune, prune)
					out = append(out, lastRPC)
				}
			}

			for _, iwant := range ctl.GetIwant() {
				if len(lastRPC.Control.Iwant) == 0 {
					// Initialize with a single IWANT.
					// For IWANTs we don't need more than a single one,
					// since there are no topic IDs here.
					newIWant := &pb.ControlIWant{}
					if lastRPC.Control.Iwant = append(lastRPC.Control.Iwant, newIWant); lastRPC.Size() > limit {
						lastRPC.Control.Iwant = lastRPC.Control.Iwant[:len(lastRPC.Control.Iwant)-1]
						lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
							Iwant: []*pb.ControlIWant{newIWant},
						}}, from: elem.from}
						out = append(out, lastRPC)
					}
				}
				for _, msgID := range iwant.GetMessageIDs() {
					if lastRPC.Control.Iwant[0].MessageIDs = append(lastRPC.Control.Iwant[0].MessageIDs, msgID); lastRPC.Size() > limit {
						lastRPC.Control.Iwant[0].MessageIDs = lastRPC.Control.Iwant[0].MessageIDs[:len(lastRPC.Control.Iwant[0].MessageIDs)-1]
						lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
							Iwant: []*pb.ControlIWant{{MessageIDs: []string{msgID}}},
						}}, from: elem.from}
						out = append(out, lastRPC)
					}
				}
			}

			for _, ihave := range ctl.GetIhave() {
				if len(lastRPC.Control.Ihave) == 0 ||
					lastRPC.Control.Ihave[len(lastRPC.Control.Ihave)-1].TopicID != ihave.TopicID {
					// Start a new IHAVE if we are referencing a new topic ID
					newIhave := &pb.ControlIHave{TopicID: ihave.TopicID}
					if lastRPC.Control.Ihave = append(lastRPC.Control.Ihave, newIhave); lastRPC.Size() > limit {
						lastRPC.Control.Ihave = lastRPC.Control.Ihave[:len(lastRPC.Control.Ihave)-1]
						lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
							Ihave: []*pb.ControlIHave{newIhave},
						}}, from: elem.from}
						out = append(out, lastRPC)
					}
				}
				for _, msgID := range ihave.GetMessageIDs() {
					lastIHave := lastRPC.Control.Ihave[len(lastRPC.Control.Ihave)-1]
					if lastIHave.MessageIDs = append(lastIHave.MessageIDs, msgID); lastRPC.Size() > limit {
						lastIHave.MessageIDs = lastIHave.MessageIDs[:len(lastIHave.MessageIDs)-1]
						lastRPC = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
							Ihave: []*pb.ControlIHave{{TopicID: ihave.TopicID, MessageIDs: []string{msgID}}},
						}}, from: elem.from}
						out = append(out, lastRPC)
					}
				}
			}
		}
	}

	return out
}

func (gs *GossipSubRouter) heartbeatTimer() {
	time.Sleep(gs.params.HeartbeatInitialDelay)
	select {
	case gs.p.eval <- gs.heartbeat:
	case <-gs.p.ctx.Done():
		return
	}

	ticker := time.NewTicker(gs.params.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case gs.p.eval <- gs.heartbeat:
			case <-gs.p.ctx.Done():
				return
			}
		case <-gs.p.ctx.Done():
			return
		}
	}
}

func (gs *GossipSubRouter) heartbeat() {
	start := time.Now()
	defer func() {
		if gs.params.SlowHeartbeatWarning > 0 {
			slowWarning := time.Duration(gs.params.SlowHeartbeatWarning * float64(gs.params.HeartbeatInterval))
			if dt := time.Since(start); dt > slowWarning {
				log.Warnw("slow heartbeat", "took", dt)
			}
		}
	}()

	gs.heartbeatTicks++

	tograft := make(map[peer.ID][]string)
	toprune := make(map[peer.ID][]string)
	noPX := make(map[peer.ID]bool)

	// clean up expired backoffs
	gs.clearBackoff()

	// clean up iasked counters
	gs.clearIHaveCounters()

	// clean up IDONTWANT counters
	gs.clearIDontWantCounters()

	// apply IWANT request penalties
	gs.applyIwantPenalties()

	// ensure direct peers are connected
	gs.directConnect()

	// cache scores throughout the heartbeat
	scores := make(map[peer.ID]float64)
	score := func(p peer.ID) float64 {
		s, ok := scores[p]
		if !ok {
			s = gs.score.Score(p)
			scores[p] = s
		}
		return s
	}

	// maintain the mesh for topics we have joined
	for topic, peers := range gs.mesh {
		prunePeer := func(p peer.ID) {
			gs.tracer.Prune(p, topic)
			delete(peers, p)
			gs.addBackoff(p, topic, false)
			topics := toprune[p]
			toprune[p] = append(topics, topic)
		}

		graftPeer := func(p peer.ID) {
			log.Debugf("HEARTBEAT: Add mesh link to %s in %s", p, topic)
			gs.tracer.Graft(p, topic)
			peers[p] = struct{}{}
			topics := tograft[p]
			tograft[p] = append(topics, topic)
		}

		// drop all peers with negative score, without PX
		for p := range peers {
			if score(p) < 0 {
				log.Debugf("HEARTBEAT: Prune peer %s with negative score [score = %f, topic = %s]", p, score(p), topic)
				prunePeer(p)
				noPX[p] = true
			}
		}

		// do we have enough peers?
		if l := len(peers); l < gs.params.Dlo {
			backoff := gs.backoff[topic]
			ineed := gs.params.D - l
			plst := gs.getPeers(topic, ineed, func(p peer.ID) bool {
				// filter our current and direct peers, peers we are backing off, and peers with negative score
				_, inMesh := peers[p]
				_, doBackoff := backoff[p]
				_, direct := gs.direct[p]
				return !inMesh && !doBackoff && !direct && score(p) >= 0
			})

			for _, p := range plst {
				graftPeer(p)
			}
		}

		// do we have too many peers?
		if len(peers) > gs.params.Dhi {
			plst := peerMapToList(peers)

			// sort by score (but shuffle first for the case we don't use the score)
			shufflePeers(plst)
			sort.Slice(plst, func(i, j int) bool {
				return score(plst[i]) > score(plst[j])
			})

			// We keep the first D_score peers by score and the remaining up to D randomly
			// under the constraint that we keep D_out peers in the mesh (if we have that many)
			shufflePeers(plst[gs.params.Dscore:])

			// count the outbound peers we are keeping
			outbound := 0
			for _, p := range plst[:gs.params.D] {
				if gs.outbound[p] {
					outbound++
				}
			}

			// if it's less than D_out, bubble up some outbound peers from the random selection
			if outbound < gs.params.Dout {
				rotate := func(i int) {
					// rotate the plst to the right and put the ith peer in the front
					p := plst[i]
					for j := i; j > 0; j-- {
						plst[j] = plst[j-1]
					}
					plst[0] = p
				}

				// first bubble up all outbound peers already in the selection to the front
				if outbound > 0 {
					ihave := outbound
					for i := 1; i < gs.params.D && ihave > 0; i++ {
						p := plst[i]
						if gs.outbound[p] {
							rotate(i)
							ihave--
						}
					}
				}

				// now bubble up enough outbound peers outside the selection to the front
				ineed := gs.params.Dout - outbound
				for i := gs.params.D; i < len(plst) && ineed > 0; i++ {
					p := plst[i]
					if gs.outbound[p] {
						rotate(i)
						ineed--
					}
				}
			}

			// prune the excess peers
			for _, p := range plst[gs.params.D:] {
				log.Debugf("HEARTBEAT: Remove mesh link to %s in %s", p, topic)
				prunePeer(p)
			}
		}

		// do we have enough outboud peers?
		if len(peers) >= gs.params.Dlo {
			// count the outbound peers we have
			outbound := 0
			for p := range peers {
				if gs.outbound[p] {
					outbound++
				}
			}

			// if it's less than D_out, select some peers with outbound connections and graft them
			if outbound < gs.params.Dout {
				ineed := gs.params.Dout - outbound
				backoff := gs.backoff[topic]
				plst := gs.getPeers(topic, ineed, func(p peer.ID) bool {
					// filter our current and direct peers, peers we are backing off, and peers with negative score
					_, inMesh := peers[p]
					_, doBackoff := backoff[p]
					_, direct := gs.direct[p]
					return !inMesh && !doBackoff && !direct && gs.outbound[p] && score(p) >= 0
				})

				for _, p := range plst {
					graftPeer(p)
				}
			}
		}

		// should we try to improve the mesh with opportunistic grafting?
		if gs.heartbeatTicks%gs.params.OpportunisticGraftTicks == 0 && len(peers) > 1 {
			// Opportunistic grafting works as follows: we check the median score of peers in the
			// mesh; if this score is below the opportunisticGraftThreshold, we select a few peers at
			// random with score over the median.
			// The intention is to (slowly) improve an underperforming mesh by introducing good
			// scoring peers that may have been gossiping at us. This allows us to get out of sticky
			// situations where we are stuck with poor peers and also recover from churn of good peers.

			// now compute the median peer score in the mesh
			plst := peerMapToList(peers)
			sort.Slice(plst, func(i, j int) bool {
				return score(plst[i]) < score(plst[j])
			})
			medianIndex := len(peers) / 2
			medianScore := scores[plst[medianIndex]]

			// if the median score is below the threshold, select a better peer (if any) and GRAFT
			if medianScore < gs.opportunisticGraftThreshold {
				backoff := gs.backoff[topic]
				plst = gs.getPeers(topic, gs.params.OpportunisticGraftPeers, func(p peer.ID) bool {
					_, inMesh := peers[p]
					_, doBackoff := backoff[p]
					_, direct := gs.direct[p]
					return !inMesh && !doBackoff && !direct && score(p) > medianScore
				})

				for _, p := range plst {
					log.Debugf("HEARTBEAT: Opportunistically graft peer %s on topic %s", p, topic)
					graftPeer(p)
				}
			}
		}

		// 2nd arg are mesh peers excluded from gossip. We already push
		// messages to them, so its redundant to gossip IHAVEs.
		gs.emitGossip(topic, peers)
	}

	// expire fanout for topics we haven't published to in a while
	now := time.Now().UnixNano()
	for topic, lastpub := range gs.lastpub {
		if lastpub+int64(gs.params.FanoutTTL) < now {
			delete(gs.fanout, topic)
			delete(gs.lastpub, topic)
		}
	}

	// maintain our fanout for topics we are publishing but we have not joined
	for topic, peers := range gs.fanout {
		// check whether our peers are still in the topic and have a score above the publish threshold
		for p := range peers {
			_, ok := gs.p.topics[topic][p]
			if !ok || score(p) < gs.publishThreshold {
				delete(peers, p)
			}
		}

		// do we need more peers?
		if len(peers) < gs.params.D {
			ineed := gs.params.D - len(peers)
			plst := gs.getPeers(topic, ineed, func(p peer.ID) bool {
				// filter our current and direct peers and peers with score above the publish threshold
				_, inFanout := peers[p]
				_, direct := gs.direct[p]
				return !inFanout && !direct && score(p) >= gs.publishThreshold
			})

			for _, p := range plst {
				peers[p] = struct{}{}
			}
		}

		// 2nd arg are fanout peers excluded from gossip. We already push
		// messages to them, so its redundant to gossip IHAVEs.
		gs.emitGossip(topic, peers)
	}

	// send coalesced GRAFT/PRUNE messages (will piggyback gossip)
	gs.sendGraftPrune(tograft, toprune, noPX)

	// flush all pending gossip that wasn't piggybacked above
	gs.flush()

	// advance the message history window
	gs.mcache.Shift()
}

func (gs *GossipSubRouter) clearIHaveCounters() {
	if len(gs.peerhave) > 0 {
		// throw away the old map and make a new one
		gs.peerhave = make(map[peer.ID]int)
	}

	if len(gs.iasked) > 0 {
		// throw away the old map and make a new one
		gs.iasked = make(map[peer.ID]int)
	}
}

func (gs *GossipSubRouter) clearIDontWantCounters() {
	if len(gs.peerdontwant) > 0 {
		// throw away the old map and make a new one
		gs.peerdontwant = make(map[peer.ID]int)
	}

	// decrement TTLs of all the IDONTWANTs and delete it from the cache when it reaches zero
	for _, mids := range gs.unwanted {
		for mid := range mids {
			mids[mid]--
			if mids[mid] == 0 {
				delete(mids, mid)
			}
		}
	}
}

func (gs *GossipSubRouter) applyIwantPenalties() {
	for p, count := range gs.gossipTracer.GetBrokenPromises() {
		log.Infof("peer %s didn't follow up in %d IWANT requests; adding penalty", p, count)
		gs.score.AddPenalty(p, count)
	}
}

func (gs *GossipSubRouter) clearBackoff() {
	// we only clear once every 15 ticks to avoid iterating over the map(s) too much
	if gs.heartbeatTicks%15 != 0 {
		return
	}

	now := time.Now()
	for topic, backoff := range gs.backoff {
		for p, expire := range backoff {
			// add some slack time to the expiration
			// https://github.com/libp2p/specs/pull/289
			if expire.Add(2 * GossipSubHeartbeatInterval).Before(now) {
				delete(backoff, p)
			}
		}
		if len(backoff) == 0 {
			delete(gs.backoff, topic)
		}
	}
}

func (gs *GossipSubRouter) directConnect() {
	// we donly do this every some ticks to allow pending connections to complete and account
	// for restarts/downtime
	if gs.heartbeatTicks%gs.params.DirectConnectTicks != 0 {
		return
	}

	var toconnect []peer.ID
	for p := range gs.direct {
		_, connected := gs.peers[p]
		if !connected {
			toconnect = append(toconnect, p)
		}
	}

	if len(toconnect) > 0 {
		go func() {
			for _, p := range toconnect {
				gs.connect <- connectInfo{p: p}
			}
		}()
	}
}

func (gs *GossipSubRouter) sendGraftPrune(tograft, toprune map[peer.ID][]string, noPX map[peer.ID]bool) {
	for p, topics := range tograft {
		graft := make([]*pb.ControlGraft, 0, len(topics))
		for _, topic := range topics {
			// copy topic string here since
			// the reference to the string
			// topic here changes with every
			// iteration of the slice.
			copiedID := topic
			graft = append(graft, &pb.ControlGraft{TopicID: &copiedID})
		}

		var prune []*pb.ControlPrune
		pruning, ok := toprune[p]
		if ok {
			delete(toprune, p)
			prune = make([]*pb.ControlPrune, 0, len(pruning))
			for _, topic := range pruning {
				prune = append(prune, gs.makePrune(p, topic, gs.doPX && !noPX[p], false))
			}
		}

		out := rpcWithControl(nil, nil, nil, graft, prune, nil)
		gs.sendRPC(p, out, false)
	}

	for p, topics := range toprune {
		prune := make([]*pb.ControlPrune, 0, len(topics))
		for _, topic := range topics {
			prune = append(prune, gs.makePrune(p, topic, gs.doPX && !noPX[p], false))
		}

		out := rpcWithControl(nil, nil, nil, nil, prune, nil)
		gs.sendRPC(p, out, false)
	}
}

// emitGossip emits IHAVE gossip advertising items in the message cache window
// of this topic.
func (gs *GossipSubRouter) emitGossip(topic string, exclude map[peer.ID]struct{}) {
	mids := gs.mcache.GetGossipIDs(topic)
	if len(mids) == 0 {
		return
	}

	// shuffle to emit in random order
	shuffleStrings(mids)

	// if we are emitting more than GossipSubMaxIHaveLength mids, truncate the list
	if len(mids) > gs.params.MaxIHaveLength {
		// we do the truncation (with shuffling) per peer below
		log.Debugf("too many messages for gossip; will truncate IHAVE list (%d messages)", len(mids))
	}

	// Send gossip to GossipFactor peers above threshold, with a minimum of D_lazy.
	// First we collect the peers above gossipThreshold that are not in the exclude set
	// and then randomly select from that set.
	// We also exclude direct peers, as there is no reason to emit gossip to them.
	peers := make([]peer.ID, 0, len(gs.p.topics[topic]))
	for p := range gs.p.topics[topic] {
		_, inExclude := exclude[p]
		_, direct := gs.direct[p]
		if !inExclude && !direct && gs.feature(GossipSubFeatureMesh, gs.peers[p]) && gs.score.Score(p) >= gs.gossipThreshold {
			peers = append(peers, p)
		}
	}

	target := gs.params.Dlazy
	factor := int(gs.params.GossipFactor * float64(len(peers)))
	if factor > target {
		target = factor
	}

	if target > len(peers) {
		target = len(peers)
	} else {
		shufflePeers(peers)
	}
	peers = peers[:target]

	// Emit the IHAVE gossip to the selected peers.
	for _, p := range peers {
		peerMids := mids
		if len(mids) > gs.params.MaxIHaveLength {
			// we do this per peer so that we emit a different set for each peer.
			// we have enough redundancy in the system that this will significantly increase the message
			// coverage when we do truncate.
			peerMids = make([]string, gs.params.MaxIHaveLength)
			shuffleStrings(mids)
			copy(peerMids, mids)
		}
		gs.enqueueGossip(p, &pb.ControlIHave{TopicID: &topic, MessageIDs: peerMids})
	}
}

func (gs *GossipSubRouter) flush() {
	// send gossip first, which will also piggyback pending control
	for p, ihave := range gs.gossip {
		delete(gs.gossip, p)
		out := rpcWithControl(nil, ihave, nil, nil, nil, nil)
		gs.sendRPC(p, out, false)
	}

	// send the remaining control messages that wasn't merged with gossip
	for p, ctl := range gs.control {
		delete(gs.control, p)
		out := rpcWithControl(nil, nil, nil, ctl.Graft, ctl.Prune, nil)
		gs.sendRPC(p, out, false)
	}
}

func (gs *GossipSubRouter) enqueueGossip(p peer.ID, ihave *pb.ControlIHave) {
	gossip := gs.gossip[p]
	gossip = append(gossip, ihave)
	gs.gossip[p] = gossip
}

func (gs *GossipSubRouter) piggybackGossip(p peer.ID, out *RPC, ihave []*pb.ControlIHave) {
	ctl := out.GetControl()
	if ctl == nil {
		ctl = &pb.ControlMessage{}
		out.Control = ctl
	}

	ctl.Ihave = ihave
}

func (gs *GossipSubRouter) pushControl(p peer.ID, ctl *pb.ControlMessage) {
	// remove IHAVE/IWANT/IDONTWANT from control message, gossip is not retried
	ctl.Ihave = nil
	ctl.Iwant = nil
	ctl.Idontwant = nil
	if ctl.Graft != nil || ctl.Prune != nil {
		gs.control[p] = ctl
	}
}

func (gs *GossipSubRouter) piggybackControl(p peer.ID, out *RPC, ctl *pb.ControlMessage) {
	// check control message for staleness first
	var tograft []*pb.ControlGraft
	var toprune []*pb.ControlPrune

	for _, graft := range ctl.GetGraft() {
		topic := graft.GetTopicID()
		peers, ok := gs.mesh[topic]
		if !ok {
			continue
		}
		_, ok = peers[p]
		if ok {
			tograft = append(tograft, graft)
		}
	}

	for _, prune := range ctl.GetPrune() {
		topic := prune.GetTopicID()
		peers, ok := gs.mesh[topic]
		if !ok {
			toprune = append(toprune, prune)
			continue
		}
		_, ok = peers[p]
		if !ok {
			toprune = append(toprune, prune)
		}
	}

	if len(tograft) == 0 && len(toprune) == 0 {
		return
	}

	xctl := out.Control
	if xctl == nil {
		xctl = &pb.ControlMessage{}
		out.Control = xctl
	}

	if len(tograft) > 0 {
		xctl.Graft = append(xctl.Graft, tograft...)
	}
	if len(toprune) > 0 {
		xctl.Prune = append(xctl.Prune, toprune...)
	}
}

func (gs *GossipSubRouter) makePrune(p peer.ID, topic string, doPX bool, isUnsubscribe bool) *pb.ControlPrune {
	if !gs.feature(GossipSubFeaturePX, gs.peers[p]) {
		// GossipSub v1.0 -- no peer exchange, the peer won't be able to parse it anyway
		return &pb.ControlPrune{TopicID: &topic}
	}

	backoff := uint64(gs.params.PruneBackoff / time.Second)
	if isUnsubscribe {
		backoff = uint64(gs.params.UnsubscribeBackoff / time.Second)
	}

	var px []*pb.PeerInfo
	if doPX {
		// select peers for Peer eXchange
		peers := gs.getPeers(topic, gs.params.PrunePeers, func(xp peer.ID) bool {
			return p != xp && gs.score.Score(xp) >= 0
		})

		cab, ok := peerstore.GetCertifiedAddrBook(gs.cab)
		px = make([]*pb.PeerInfo, 0, len(peers))
		for _, p := range peers {
			// see if we have a signed peer record to send back; if we don't, just send
			// the peer ID and let the pruned peer find them in the DHT -- we can't trust
			// unsigned address records through px anyway.
			var recordBytes []byte
			if ok {
				spr := cab.GetPeerRecord(p)
				var err error
				if spr != nil {
					recordBytes, err = spr.Marshal()
					if err != nil {
						log.Warnf("error marshaling signed peer record for %s: %s", p, err)
					}
				}
			}
			px = append(px, &pb.PeerInfo{PeerID: []byte(p), SignedPeerRecord: recordBytes})
		}
	}

	return &pb.ControlPrune{TopicID: &topic, Peers: px, Backoff: &backoff}
}

func (gs *GossipSubRouter) getPeers(topic string, count int, filter func(peer.ID) bool) []peer.ID {
	tmap, ok := gs.p.topics[topic]
	if !ok {
		return nil
	}

	peers := make([]peer.ID, 0, len(tmap))
	for p := range tmap {
		if gs.feature(GossipSubFeatureMesh, gs.peers[p]) && filter(p) && gs.p.peerFilter(p, topic) {
			peers = append(peers, p)
		}
	}

	shufflePeers(peers)

	if count > 0 && len(peers) > count {
		peers = peers[:count]
	}

	return peers
}

// WithDefaultTagTracer returns the tag tracer of the GossipSubRouter as a PubSub option.
// This is useful for cases where the GossipSubRouter is instantiated externally, and is
// injected into the GossipSub constructor as a dependency. This allows the tag tracer to be
// also injected into the GossipSub constructor as a PubSub option dependency.
func (gs *GossipSubRouter) WithDefaultTagTracer() Option {
	return WithRawTracer(gs.tagTracer)
}

func peerListToMap(peers []peer.ID) map[peer.ID]struct{} {
	pmap := make(map[peer.ID]struct{})
	for _, p := range peers {
		pmap[p] = struct{}{}
	}
	return pmap
}

func peerMapToList(peers map[peer.ID]struct{}) []peer.ID {
	plst := make([]peer.ID, 0, len(peers))
	for p := range peers {
		plst = append(plst, p)
	}
	return plst
}

func shufflePeers(peers []peer.ID) {
	for i := range peers {
		j := rand.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func shufflePeerInfo(peers []*pb.PeerInfo) {
	for i := range peers {
		j := rand.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func shuffleStrings(lst []string) {
	for i := range lst {
		j := rand.Intn(i + 1)
		lst[i], lst[j] = lst[j], lst[i]
	}
}

func computeChecksum(mid string) checksum {
	var cs checksum
	if len(mid) > 32 || len(mid) == 0 {
		cs.payload = sha256.Sum256([]byte(mid))
	} else {
		cs.length = uint8(copy(cs.payload[:], mid))
	}
	return cs
}
