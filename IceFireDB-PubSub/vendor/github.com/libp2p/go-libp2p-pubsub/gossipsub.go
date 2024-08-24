package pubsub

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/record"
)

const (
	// GossipSubID_v10 is the protocol ID for version 1.0.0 of the GossipSub protocol.
	// It is advertised along with GossipSubID_v11 for backwards compatibility.
	GossipSubID_v10 = protocol.ID("/meshsub/1.0.0")

	// GossipSubID_v11 is the protocol ID for version 1.1.0 of the GossipSub protocol.
	// See the spec for details about how v1.1.0 compares to v1.0.0:
	// https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md
	GossipSubID_v11 = protocol.ID("/meshsub/1.1.0")
)

var (
	// overlay parameters.

	// GossipSubD sets the optimal degree for a GossipSub topic mesh. For example, if GossipSubD == 6,
	// each peer will want to have about six peers in their mesh for each topic they're subscribed to.
	// GossipSubD should be set somewhere between GossipSubDlo and GossipSubDhi.
	GossipSubD = 6

	// GossipSubDlo sets the lower bound on the number of peers we keep in a GossipSub topic mesh.
	// If we have fewer than GossipSubDlo peers, we will attempt to graft some more into the mesh at
	// the next heartbeat.
	GossipSubDlo = 5

	// GossipSubDhi sets the upper bound on the number of peers we keep in a GossipSub topic mesh.
	// If we have more than GossipSubDhi peers, we will select some to prune from the mesh at the next heartbeat.
	GossipSubDhi = 12

	// GossipSubDscore affects how peers are selected when pruning a mesh due to over subscription.
	// At least GossipSubDscore of the retained peers will be high-scoring, while the remainder are
	// chosen randomly.
	GossipSubDscore = 4

	// GossipSubDout sets the quota for the number of outbound connections to maintain in a topic mesh.
	// When the mesh is pruned due to over subscription, we make sure that we have outbound connections
	// to at least GossipSubDout of the survivor peers. This prevents sybil attackers from overwhelming
	// our mesh with incoming connections.
	//
	// GossipSubDout must be set below GossipSubDlo, and must not exceed GossipSubD / 2.
	GossipSubDout = 2

	// gossip parameters

	// GossipSubHistoryLength controls the size of the message cache used for gossip.
	// The message cache will remember messages for GossipSubHistoryLength heartbeats.
	GossipSubHistoryLength = 5

	// GossipSubHistoryGossip controls how many cached message ids we will advertise in
	// IHAVE gossip messages. When asked for our seen message IDs, we will return
	// only those from the most recent GossipSubHistoryGossip heartbeats. The slack between
	// GossipSubHistoryGossip and GossipSubHistoryLength allows us to avoid advertising messages
	// that will be expired by the time they're requested.
	//
	// GossipSubHistoryGossip must be less than or equal to GossipSubHistoryLength to
	// avoid a runtime panic.
	GossipSubHistoryGossip = 3

	// GossipSubDlazy affects how many peers we will emit gossip to at each heartbeat.
	// We will send gossip to at least GossipSubDlazy peers outside our mesh. The actual
	// number may be more, depending on GossipSubGossipFactor and how many peers we're
	// connected to.
	GossipSubDlazy = 6

	// GossipSubGossipFactor affects how many peers we will emit gossip to at each heartbeat.
	// We will send gossip to GossipSubGossipFactor * (total number of non-mesh peers), or
	// GossipSubDlazy, whichever is greater.
	GossipSubGossipFactor = 0.25

	// GossipSubGossipRetransmission controls how many times we will allow a peer to request
	// the same message id through IWANT gossip before we start ignoring them. This is designed
	// to prevent peers from spamming us with requests and wasting our resources.
	GossipSubGossipRetransmission = 3

	// heartbeat interval

	// GossipSubHeartbeatInitialDelay is the short delay before the heartbeat timer begins
	// after the router is initialized.
	GossipSubHeartbeatInitialDelay = 100 * time.Millisecond

	// GossipSubHeartbeatInterval controls the time between heartbeats.
	GossipSubHeartbeatInterval = 1 * time.Second

	// GossipSubFanoutTTL controls how long we keep track of the fanout state. If it's been
	// GossipSubFanoutTTL since we've published to a topic that we're not subscribed to,
	// we'll delete the fanout map for that topic.
	GossipSubFanoutTTL = 60 * time.Second

	// GossipSubPrunePeers controls the number of peers to include in prune Peer eXchange.
	// When we prune a peer that's eligible for PX (has a good score, etc), we will try to
	// send them signed peer records for up to GossipSubPrunePeers other peers that we
	// know of.
	GossipSubPrunePeers = 16

	// GossipSubPruneBackoff controls the backoff time for pruned peers. This is how long
	// a peer must wait before attempting to graft into our mesh again after being pruned.
	// When pruning a peer, we send them our value of GossipSubPruneBackoff so they know
	// the minimum time to wait. Peers running older versions may not send a backoff time,
	// so if we receive a prune message without one, we will wait at least GossipSubPruneBackoff
	// before attempting to re-graft.
	GossipSubPruneBackoff = time.Minute

	// GossipSubConnectors controls the number of active connection attempts for peers obtained through PX.
	GossipSubConnectors = 8

	// GossipSubMaxPendingConnections sets the maximum number of pending connections for peers attempted through px.
	GossipSubMaxPendingConnections = 128

	// GossipSubConnectionTimeout controls the timeout for connection attempts.
	GossipSubConnectionTimeout = 30 * time.Second

	// GossipSubDirectConnectTicks is the number of heartbeat ticks for attempting to reconnect direct peers
	// that are not currently connected.
	GossipSubDirectConnectTicks uint64 = 300

	// GossipSubDirectConnectInitialDelay is the initial delay before opening connections to direct peers
	GossipSubDirectConnectInitialDelay = time.Second

	// GossipSubOpportunisticGraftTicks is the number of heartbeat ticks for attempting to improve the mesh
	// with opportunistic grafting. Every GossipSubOpportunisticGraftTicks we will attempt to select some
	// high-scoring mesh peers to replace lower-scoring ones, if the median score of our mesh peers falls
	// below a threshold (see https://godoc.org/github.com/libp2p/go-libp2p-pubsub#PeerScoreThresholds).
	GossipSubOpportunisticGraftTicks uint64 = 60

	// GossipSubOpportunisticGraftPeers is the number of peers to opportunistically graft.
	GossipSubOpportunisticGraftPeers = 2

	// If a GRAFT comes before GossipSubGraftFloodThreshold has elapsed since the last PRUNE,
	// then there is an extra score penalty applied to the peer through P7.
	GossipSubGraftFloodThreshold = 10 * time.Second

	// GossipSubMaxIHaveLength is the maximum number of messages to include in an IHAVE message.
	// Also controls the maximum number of IHAVE ids we will accept and request with IWANT from a
	// peer within a heartbeat, to protect from IHAVE floods. You should adjust this value from the
	// default if your system is pushing more than 5000 messages in GossipSubHistoryGossip heartbeats;
	// with the defaults this is 1666 messages/s.
	GossipSubMaxIHaveLength = 5000

	// GossipSubMaxIHaveMessages is the maximum number of IHAVE messages to accept from a peer within a heartbeat.
	GossipSubMaxIHaveMessages = 10

	// Time to wait for a message requested through IWANT following an IHAVE advertisement.
	// If the message is not received within this window, a broken promise is declared and
	// the router may apply bahavioural penalties.
	GossipSubIWantFollowupTime = 3 * time.Second
)

// NewGossipSub returns a new PubSub object using GossipSubRouter as the router.
func NewGossipSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := &GossipSubRouter{
		peers:    make(map[peer.ID]protocol.ID),
		mesh:     make(map[string]map[peer.ID]struct{}),
		fanout:   make(map[string]map[peer.ID]struct{}),
		lastpub:  make(map[string]int64),
		gossip:   make(map[peer.ID][]*pb.ControlIHave),
		control:  make(map[peer.ID]*pb.ControlMessage),
		backoff:  make(map[string]map[peer.ID]time.Time),
		peerhave: make(map[peer.ID]int),
		iasked:   make(map[peer.ID]int),
		outbound: make(map[peer.ID]bool),
		connect:  make(chan connectInfo, GossipSubMaxPendingConnections),
		mcache:   NewMessageCache(GossipSubHistoryGossip, GossipSubHistoryLength),

		// these are configured per router to allow variation in tests
		D:      GossipSubD,
		Dlo:    GossipSubDlo,
		Dhi:    GossipSubDhi,
		Dscore: GossipSubDscore,
		Dout:   GossipSubDout,
		Dlazy:  GossipSubDlazy,

		// these must be pulled in to resolve races in tests... sigh.
		directConnectTicks:      GossipSubDirectConnectTicks,
		opportunisticGraftTicks: GossipSubOpportunisticGraftTicks,

		fanoutTTL: GossipSubFanoutTTL,

		tagTracer: newTagTracer(h.ConnManager()),
	}

	// use the withInternalTracer option to hook up the tag tracer
	opts = append(opts, withInternalTracer(rt.tagTracer))
	return NewPubSub(ctx, h, rt, opts...)
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
			ps.tracer.internal = append(ps.tracer.internal, gs.score, gs.gossipTracer)
		} else {
			ps.tracer = &pubsubTracer{
				internal: []internalTracer{gs.score, gs.gossipTracer},
				pid:      ps.host.ID(),
				msgID:    ps.msgID,
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
		gs.directConnectTicks = t
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
	p        *PubSub
	peers    map[peer.ID]protocol.ID          // peer protocols
	direct   map[peer.ID]struct{}             // direct peers
	mesh     map[string]map[peer.ID]struct{}  // topic meshes
	fanout   map[string]map[peer.ID]struct{}  // topic fanout
	lastpub  map[string]int64                 // last publish time for fanout topics
	gossip   map[peer.ID][]*pb.ControlIHave   // pending gossip
	control  map[peer.ID]*pb.ControlMessage   // pending control messages
	peerhave map[peer.ID]int                  // number of IHAVEs received from peer in the last heartbeat
	iasked   map[peer.ID]int                  // number of messages we have asked from peer in the last heartbeat
	outbound map[peer.ID]bool                 // connection direction cache, marks peers with outbound connections
	backoff  map[string]map[peer.ID]time.Time // prune backoff
	connect  chan connectInfo                 // px connection requests

	mcache       *MessageCache
	tracer       *pubsubTracer
	score        *peerScore
	gossipTracer *gossipTracer
	tagTracer    *tagTracer
	gate         *peerGater

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

	// overly parameter "constants"
	// these are pulled from their global value or else the race detector is angry on travis
	// it also allows us to change them per peer in tests, which is a plus
	D, Dlo, Dhi, Dscore, Dout, Dlazy int

	// tick "constants" for triggering direct connect and opportunistic grafting
	// these are pulled from their global value or else the race detector is angry on travis
	directConnectTicks, opportunisticGraftTicks uint64

	// fanout expiry ttl "constant"
	// this is pulled from its global value or else the race detector is angry on travis
	fanoutTTL time.Duration
}

type connectInfo struct {
	p   peer.ID
	spr *record.Envelope
}

func (gs *GossipSubRouter) Protocols() []protocol.ID {
	return []protocol.ID{GossipSubID_v11, GossipSubID_v10, FloodSubID}
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
	gs.mcache.SetMsgIdFn(p.msgID)

	// start the heartbeat
	go gs.heartbeatTimer()

	// start the PX connectors
	for i := 0; i < GossipSubConnectors; i++ {
		go gs.connector()
	}

	// connect to direct peers
	if len(gs.direct) > 0 {
		go func() {
			if GossipSubDirectConnectInitialDelay > 0 {
				time.Sleep(GossipSubDirectConnectInitialDelay)
			}
			for p := range gs.direct {
				gs.connect <- connectInfo{p: p}
			}
		}()
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
		if c.Stat().Direction == network.DirOutbound {
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
		if gs.peers[p] == FloodSubID {
			fsPeers++
		}
	}

	// gossipsub peers
	gsPeers = len(gs.mesh[topic])

	if suggested == 0 {
		suggested = gs.Dlo
	}

	if fsPeers+gsPeers >= suggested || gsPeers >= gs.Dhi {
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

func (gs *GossipSubRouter) HandleRPC(rpc *RPC) {
	ctl := rpc.GetControl()
	if ctl == nil {
		return
	}

	iwant := gs.handleIHave(rpc.from, ctl)
	ihave := gs.handleIWant(rpc.from, ctl)
	prune := gs.handleGraft(rpc.from, ctl)
	gs.handlePrune(rpc.from, ctl)

	if len(iwant) == 0 && len(ihave) == 0 && len(prune) == 0 {
		return
	}

	out := rpcWithControl(ihave, nil, iwant, nil, prune)
	gs.sendRPC(rpc.from, out)
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
	if gs.peerhave[p] > GossipSubMaxIHaveMessages {
		log.Debugf("IHAVE: peer %s has advertised too many times (%d) within this heartbeat interval; ignoring", p, gs.peerhave[p])
		return nil
	}

	if gs.iasked[p] >= GossipSubMaxIHaveLength {
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

		for _, mid := range ihave.GetMessageIDs() {
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
	if iask+gs.iasked[p] > GossipSubMaxIHaveLength {
		iask = GossipSubMaxIHaveLength - gs.iasked[p]
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

	return []*pb.ControlIWant{&pb.ControlIWant{MessageIDs: iwantlst}}
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

			if count > GossipSubGossipRetransmission {
				log.Debugf("IWANT: Peer %s has asked for message %s too many times; ignoring request", p, mid)
				continue
			}

			ihave[mid] = msg
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
			floodCutoff := expire.Add(GossipSubGraftFloodThreshold - GossipSubPruneBackoff)
			if now.Before(floodCutoff) {
				// extra penalty
				gs.score.AddPenalty(p, 1)
			}
			// refresh the backoff
			gs.addBackoff(p, topic)
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
			gs.addBackoff(p, topic)
			continue
		}

		// check the number of mesh peers; if it is at (or over) Dhi, we only accept grafts
		// from peers with outbound connections; this is a defensive check to restrict potential
		// mesh takeover attacks combined with love bombing
		if len(peers) >= gs.Dhi && !gs.outbound[p] {
			prune = append(prune, topic)
			gs.addBackoff(p, topic)
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
		cprune = append(cprune, gs.makePrune(p, topic, doPX))
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
			gs.addBackoff(p, topic)
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

func (gs *GossipSubRouter) addBackoff(p peer.ID, topic string) {
	gs.doAddBackoff(p, topic, GossipSubPruneBackoff)
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
	if len(peers) > GossipSubPrunePeers {
		shufflePeerInfo(peers)
		peers = peers[:GossipSubPrunePeers]
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
			break
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
			cab, ok := peerstore.GetCertifiedAddrBook(gs.p.host.Peerstore())
			if ok && ci.spr != nil {
				_, err := cab.ConsumePeerRecord(ci.spr, peerstore.TempAddrTTL)
				if err != nil {
					log.Debugf("error processing peer record: %s", err)
				}
			}

			ctx, cancel := context.WithTimeout(gs.p.ctx, GossipSubConnectionTimeout)
			err := gs.p.host.Connect(ctx, peer.AddrInfo{ID: ci.p})
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
	gs.mcache.Put(msg.Message)

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
			if gs.peers[p] == FloodSubID && gs.score.Score(p) >= gs.publishThreshold {
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
				peers := gs.getPeers(topic, gs.D, func(p peer.ID) bool {
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
			tosend[p] = struct{}{}
		}
	}

	out := rpcWithMessages(msg.Message)
	for pid := range tosend {
		if pid == from || pid == peer.ID(msg.GetFrom()) {
			continue
		}

		gs.sendRPC(pid, out)
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
		// these peers have a score above the publish threshold, which may be negative
		// so drop the ones with a negative score
		for p := range gmap {
			if gs.score.Score(p) < 0 {
				delete(gmap, p)
			}
		}

		if len(gmap) < gs.D {
			// we need more peers; eager, as this would get fixed in the next heartbeat
			more := gs.getPeers(topic, gs.D-len(gmap), func(p peer.ID) bool {
				// filter our current peers, direct peers, and peers with negative scores
				_, inMesh := gmap[p]
				_, direct := gs.direct[p]
				return !inMesh && !direct && gs.score.Score(p) >= 0
			})
			for _, p := range more {
				gmap[p] = struct{}{}
			}
		}
		gs.mesh[topic] = gmap
		delete(gs.fanout, topic)
		delete(gs.lastpub, topic)
	} else {
		peers := gs.getPeers(topic, gs.D, func(p peer.ID) bool {
			// filter direct peers and peers with negative score
			_, direct := gs.direct[p]
			return !direct && gs.score.Score(p) >= 0
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
		gs.sendPrune(p, topic)
	}
}

func (gs *GossipSubRouter) sendGraft(p peer.ID, topic string) {
	graft := []*pb.ControlGraft{&pb.ControlGraft{TopicID: &topic}}
	out := rpcWithControl(nil, nil, nil, graft, nil)
	gs.sendRPC(p, out)
}

func (gs *GossipSubRouter) sendPrune(p peer.ID, topic string) {
	prune := []*pb.ControlPrune{gs.makePrune(p, topic, gs.doPX)}
	out := rpcWithControl(nil, nil, nil, nil, prune)
	gs.sendRPC(p, out)
}

func (gs *GossipSubRouter) sendRPC(p peer.ID, out *RPC) {
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

	mch, ok := gs.p.peers[p]
	if !ok {
		return
	}

	// If we're below the max message size, go ahead and send
	if out.Size() < gs.p.maxMessageSize {
		gs.doSendRPC(out, p, mch)
		return
	}

	// If we're too big, fragment into multiple RPCs and send each sequentially
	outRPCs, err := fragmentRPC(out, gs.p.maxMessageSize)
	if err != nil {
		gs.doDropRPC(out, p, fmt.Sprintf("unable to fragment RPC: %s", err))
		return
	}

	for _, rpc := range outRPCs {
		gs.doSendRPC(rpc, p, mch)
	}
}

func (gs *GossipSubRouter) doDropRPC(rpc *RPC, p peer.ID, reason string) {
	log.Debugf("dropping message to peer %s: %s", p.Pretty(), reason)
	gs.tracer.DropRPC(rpc, p)
	// push control messages that need to be retried
	ctl := rpc.GetControl()
	if ctl != nil {
		gs.pushControl(p, ctl)
	}
}

func (gs *GossipSubRouter) doSendRPC(rpc *RPC, p peer.ID, mch chan *RPC) {
	select {
	case mch <- rpc:
		gs.tracer.SendRPC(rpc, p)
	default:
		gs.doDropRPC(rpc, p, "queue full")
	}
}

func fragmentRPC(rpc *RPC, limit int) ([]*RPC, error) {
	if rpc.Size() < limit {
		return []*RPC{rpc}, nil
	}

	c := (rpc.Size() / limit) + 1
	rpcs := make([]*RPC, 1, c)
	rpcs[0] = &RPC{RPC: pb.RPC{}, from: rpc.from}

	// outRPC returns the current RPC message if it will fit sizeToAdd more bytes
	// otherwise, it will create a new RPC message and add it to the list.
	// if withCtl is true, the returned message will have a non-nil empty Control message.
	outRPC := func(sizeToAdd int, withCtl bool) *RPC {
		current := rpcs[len(rpcs)-1]
		// check if we can fit the new data, plus an extra byte for the protobuf field tag
		if current.Size()+sizeToAdd+1 < limit {
			if withCtl && current.Control == nil {
				current.Control = &pb.ControlMessage{}
			}
			return current
		}
		var ctl *pb.ControlMessage
		if withCtl {
			ctl = &pb.ControlMessage{}
		}
		next := &RPC{RPC: pb.RPC{Control: ctl}, from: rpc.from}
		rpcs = append(rpcs, next)
		return next
	}

	for _, msg := range rpc.GetPublish() {
		s := msg.Size()
		// if an individual message is too large, we can't fragment it and have to fail entirely
		if s > limit {
			return nil, fmt.Errorf("message with len=%d exceeds limit %d", s, limit)
		}
		out := outRPC(s, false)
		out.Publish = append(out.Publish, msg)
	}

	for _, sub := range rpc.GetSubscriptions() {
		out := outRPC(sub.Size(), false)
		out.Subscriptions = append(out.Subscriptions, sub)
	}

	ctl := rpc.GetControl()
	if ctl == nil {
		// if there were no control messages, we're done
		return rpcs, nil
	}
	// if all the control messages fit into one RPC, we just add it to the end and return
	ctlOut := &RPC{RPC: pb.RPC{Control: ctl}, from: rpc.from}
	if ctlOut.Size() < limit {
		rpcs = append(rpcs, ctlOut)
		return rpcs, nil
	}

	// we need to split up the control messages into multiple RPCs
	for _, graft := range ctl.Graft {
		out := outRPC(graft.Size(), true)
		out.Control.Graft = append(out.Control.Graft, graft)
	}
	for _, prune := range ctl.Prune {
		out := outRPC(prune.Size(), true)
		out.Control.Prune = append(out.Control.Prune, prune)
	}

	// An individual IWANT or IHAVE message could be larger than the limit if we have
	// a lot of message IDs. fragmentMessageIds will split them into buckets that
	// fit within the limit, with some overhead for the control messages themselves
	for _, iwant := range ctl.Iwant {
		const protobufOverhead = 6
		idBuckets := fragmentMessageIds(iwant.MessageIDs, limit-protobufOverhead)
		for _, ids := range idBuckets {
			iwant := &pb.ControlIWant{MessageIDs: ids}
			out := outRPC(iwant.Size(), true)
			out.Control.Iwant = append(out.Control.Iwant, iwant)
		}
	}
	for _, ihave := range ctl.Ihave {
		const protobufOverhead = 6
		idBuckets := fragmentMessageIds(ihave.MessageIDs, limit-protobufOverhead)
		for _, ids := range idBuckets {
			ihave := &pb.ControlIHave{MessageIDs: ids}
			out := outRPC(ihave.Size(), true)
			out.Control.Ihave = append(out.Control.Ihave, ihave)
		}
	}
	return rpcs, nil
}

func fragmentMessageIds(msgIds []string, limit int) [][]string {
	// account for two bytes of protobuf overhead per array element
	const protobufOverhead = 2

	out := [][]string{{}}
	var currentBucket int
	var bucketLen int
	for i := 0; i < len(msgIds); i++ {
		size := len(msgIds[i]) + protobufOverhead
		if size > limit {
			// pathological case where a single message ID exceeds the limit.
			log.Warnf("message ID length %d exceeds limit %d, removing from outgoing gossip", size, limit)
			continue
		}
		bucketLen += size
		if bucketLen > limit {
			out = append(out, []string{})
			currentBucket++
			bucketLen = size
		}
		out[currentBucket] = append(out[currentBucket], msgIds[i])
	}
	return out
}

func (gs *GossipSubRouter) heartbeatTimer() {
	time.Sleep(GossipSubHeartbeatInitialDelay)
	select {
	case gs.p.eval <- gs.heartbeat:
	case <-gs.p.ctx.Done():
		return
	}

	ticker := time.NewTicker(GossipSubHeartbeatInterval)
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
	defer log.EventBegin(gs.p.ctx, "heartbeat").Done()

	gs.heartbeatTicks++

	tograft := make(map[peer.ID][]string)
	toprune := make(map[peer.ID][]string)
	noPX := make(map[peer.ID]bool)

	// clean up expired backoffs
	gs.clearBackoff()

	// clean up iasked counters
	gs.clearIHaveCounters()

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
			gs.addBackoff(p, topic)
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
		if l := len(peers); l < gs.Dlo {
			backoff := gs.backoff[topic]
			ineed := gs.D - l
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
		if len(peers) > gs.Dhi {
			plst := peerMapToList(peers)

			// sort by score (but shuffle first for the case we don't use the score)
			shufflePeers(plst)
			sort.Slice(plst, func(i, j int) bool {
				return score(plst[i]) > score(plst[j])
			})

			// We keep the first D_score peers by score and the remaining up to D randomly
			// under the constraint that we keep D_out peers in the mesh (if we have that many)
			shufflePeers(plst[gs.Dscore:])

			// count the outbound peers we are keeping
			outbound := 0
			for _, p := range plst[:gs.D] {
				if gs.outbound[p] {
					outbound++
				}
			}

			// if it's less than D_out, bubble up some outbound peers from the random selection
			if outbound < gs.Dout {
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
					for i := 1; i < gs.D && ihave > 0; i++ {
						p := plst[i]
						if gs.outbound[p] {
							rotate(i)
							ihave--
						}
					}
				}

				// now bubble up enough outbound peers outside the selection to the front
				ineed := gs.Dout - outbound
				for i := gs.D; i < len(plst) && ineed > 0; i++ {
					p := plst[i]
					if gs.outbound[p] {
						rotate(i)
						ineed--
					}
				}
			}

			// prune the excess peers
			for _, p := range plst[gs.D:] {
				log.Debugf("HEARTBEAT: Remove mesh link to %s in %s", p, topic)
				prunePeer(p)
			}
		}

		// do we have enough outboud peers?
		if len(peers) >= gs.Dlo {
			// count the outbound peers we have
			outbound := 0
			for p := range peers {
				if gs.outbound[p] {
					outbound++
				}
			}

			// if it's less than D_out, select some peers with outbound connections and graft them
			if outbound < gs.Dout {
				ineed := gs.Dout - outbound
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
		if gs.heartbeatTicks%gs.opportunisticGraftTicks == 0 && len(peers) > 1 {
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
				plst = gs.getPeers(topic, GossipSubOpportunisticGraftPeers, func(p peer.ID) bool {
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
		if lastpub+int64(gs.fanoutTTL) < now {
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
		if len(peers) < gs.D {
			ineed := gs.D - len(peers)
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
	if gs.heartbeatTicks%gs.directConnectTicks != 0 {
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
				prune = append(prune, gs.makePrune(p, topic, gs.doPX && !noPX[p]))
			}
		}

		out := rpcWithControl(nil, nil, nil, graft, prune)
		gs.sendRPC(p, out)
	}

	for p, topics := range toprune {
		prune := make([]*pb.ControlPrune, 0, len(topics))
		for _, topic := range topics {
			prune = append(prune, gs.makePrune(p, topic, gs.doPX && !noPX[p]))
		}

		out := rpcWithControl(nil, nil, nil, nil, prune)
		gs.sendRPC(p, out)
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
	if len(mids) > GossipSubMaxIHaveLength {
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
		if !inExclude && !direct && (gs.peers[p] == GossipSubID_v10 || gs.peers[p] == GossipSubID_v11) && gs.score.Score(p) >= gs.gossipThreshold {
			peers = append(peers, p)
		}
	}

	target := gs.Dlazy
	factor := int(GossipSubGossipFactor * float64(len(peers)))
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
		if len(mids) > GossipSubMaxIHaveLength {
			// we do this per peer so that we emit a different set for each peer.
			// we have enough redundancy in the system that this will significantly increase the message
			// coverage when we do truncate.
			peerMids = make([]string, GossipSubMaxIHaveLength)
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
		out := rpcWithControl(nil, ihave, nil, nil, nil)
		gs.sendRPC(p, out)
	}

	// send the remaining control messages that wasn't merged with gossip
	for p, ctl := range gs.control {
		delete(gs.control, p)
		out := rpcWithControl(nil, nil, nil, ctl.Graft, ctl.Prune)
		gs.sendRPC(p, out)
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
	// remove IHAVE/IWANT from control message, gossip is not retried
	ctl.Ihave = nil
	ctl.Iwant = nil
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

func (gs *GossipSubRouter) makePrune(p peer.ID, topic string, doPX bool) *pb.ControlPrune {
	if gs.peers[p] == GossipSubID_v10 {
		// GossipSub v1.0 -- no peer exchange, the peer won't be able to parse it anyway
		return &pb.ControlPrune{TopicID: &topic}
	}

	backoff := uint64(GossipSubPruneBackoff / time.Second)
	var px []*pb.PeerInfo
	if doPX {
		// select peers for Peer eXchange
		peers := gs.getPeers(topic, GossipSubPrunePeers, func(xp peer.ID) bool {
			return p != xp && gs.score.Score(xp) >= 0
		})

		cab, ok := peerstore.GetCertifiedAddrBook(gs.p.host.Peerstore())
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
		if (gs.peers[p] == GossipSubID_v10 || gs.peers[p] == GossipSubID_v11) && filter(p) {
			peers = append(peers, p)
		}
	}

	shufflePeers(peers)

	if count > 0 && len(peers) > count {
		peers = peers[:count]
	}

	return peers
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
