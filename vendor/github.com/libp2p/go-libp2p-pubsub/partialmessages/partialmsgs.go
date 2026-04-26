package partialmessages

import (
	"errors"
	"iter"
	"log/slog"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

const minGroupTTL = 3

// defaultPeerInitiatedGroupLimitPerTopic limits the total number (per topic) of
// *partialMessageStatePerTopicGroup we create in response to a incoming RPC.
// This only applies to groups that we haven't published for yet.
const defaultPeerInitiatedGroupLimitPerTopic = 255

const defaultPeerInitiatedGroupLimitPerTopicPerPeer = 8

// PartsMetadata returns metadata about the parts this partial message
// contains and, possibly implicitly, the parts it wants.
type PartsMetadata []byte

type PeerInfo struct {
	// If RequestedPartialMessage is false, the peer does not want encoded
	// partial messages. The encoded message in a PublishAction MUST be
	// empty for this peer. The implementation SHOULD still send relevant
	// partsMetadataToSend.
	RequestedPartialMessage bool
}

type PublishAction struct {
	// EncodedPartialMessage is the encoded PartialMessage that will be sent to
	// this peer.
	EncodedPartialMessage []byte
	// EncodedPartsMetadata is the PartsMetadata that will be sent to this peer.
	EncodedPartsMetadata []byte

	// Err signals an error that will be bubbled up to the caller of
	// PublishPartial. Returning an error does not prevent other PublishActions
	// from executing or affect the PeerState map.
	Err error
}

type partialMessageStatePerGroupPerTopic[P any] struct {
	peerState   map[peer.ID]P
	groupTTL    int
	initiatedBy peer.ID // zero value if we initiated the group
}

func newPartialMessageStatePerTopicGroup[P any](groupTTL int) *partialMessageStatePerGroupPerTopic[P] {
	return &partialMessageStatePerGroupPerTopic[P]{
		peerState: make(map[peer.ID]P),
		groupTTL:  max(groupTTL, minGroupTTL),
	}
}

func (s *partialMessageStatePerGroupPerTopic[P]) remotePeerInitiated() bool {
	return s.initiatedBy != ""
}

type PartialMessagesExtension[PeerState any] struct {
	Logger *slog.Logger

	// OnEmitGossip is called when the application should send gossip to the given
	// peers. The application SHOULD call PublishPartial to send partial
	// messages to these peers.
	//
	// The Application may persist some state in the peerStates map. The peers
	// to gossip to will have a zero-value PeerState in the map.
	OnEmitGossip func(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]PeerState)

	// OnIncomingRPC is called whenever we receive an encoded
	// partial message from a peer. This function MUST be fast and non-blocking.
	// If you need to do slow work (e.g. validation), dispatch the work to your
	// own goroutine.
	//
	// This function SHOULD update the peer's PeerState with the received
	// rpc.PartsMetadata and rpc.PartialMessage.
	//
	// An implementation may be able to infer some peer state from the
	// rpc.PartialMessage, for example:
	//  - peer's received state (we can infer they have a part if they have given it us)
	//  - our last update to a peer (they can infer an update if they have given us a part)
	//
	// If the rpc should be ignored, the application can leave peerStates unmodified
	OnIncomingRPC func(from peer.ID, peerStates map[peer.ID]PeerState, rpc *pb.PartialMessagesExtension) error

	// PeerInitiatedGroupLimitPerTopic limits the number of Group states all
	// peers can initialize per topic. A group state is initialized by a peer if
	// the peer's message marks the first time we've seen a group id.
	PeerInitiatedGroupLimitPerTopic int

	// PeerInitiatedGroupLimitPerTopicPerPeer limits the number of Group states
	// a single peer can initialize per topic. A group state is initialized by a
	// peer if the peer's message marks the first time we've seen a group id.
	PeerInitiatedGroupLimitPerTopicPerPeer int

	// GroupTTLByHeatbeat is how many heartbeats we store Group state for after
	// publishing a partial message for the group.
	GroupTTLByHeatbeat int

	// map topic -> TopicState
	statePerTopicPerGroup map[string]map[string]*partialMessageStatePerGroupPerTopic[PeerState]

	// map[topic]counter
	peerInitiatedGroupCounter map[string]*peerInitiatedGroupCounterState

	router Router
}

type Router interface {
	SendRPC(p peer.ID, r *pb.PartialMessagesExtension, urgent bool)
	MeshPeers(topic string) iter.Seq[peer.ID]
	PeerRequestsPartial(peer peer.ID, topic string) bool
}

func (e *PartialMessagesExtension[PeerState]) groupState(topic string, groupID []byte, peerInitiated bool, from peer.ID) (*partialMessageStatePerGroupPerTopic[PeerState], error) {
	tState, ok := e.statePerTopicPerGroup[topic]
	if !ok {
		tState = make(map[string]*partialMessageStatePerGroupPerTopic[PeerState])
		e.statePerTopicPerGroup[topic] = tState
	}
	if _, ok := e.peerInitiatedGroupCounter[topic]; !ok {
		e.peerInitiatedGroupCounter[topic] = &peerInitiatedGroupCounterState{}
	}
	gState, ok := tState[string(groupID)]
	if !ok {
		if peerInitiated {
			err := e.peerInitiatedGroupCounter[topic].Inc(e.PeerInitiatedGroupLimitPerTopic, e.PeerInitiatedGroupLimitPerTopicPerPeer, from)
			if err != nil {
				return nil, err
			}
		}

		gState = newPartialMessageStatePerTopicGroup[PeerState](e.GroupTTLByHeatbeat)
		tState[string(groupID)] = gState
		gState.initiatedBy = from
	}
	if !peerInitiated && gState.remotePeerInitiated() {
		// We've tried to initiate this state as well, so it's no longer peer initiated.
		e.peerInitiatedGroupCounter[topic].Dec(gState.initiatedBy)
		gState.initiatedBy = ""
	}
	return gState, nil
}

func (e *PartialMessagesExtension[PeerState]) Init(router Router) error {
	e.router = router
	if e.Logger == nil {
		return errors.New("field Logger must be set")
	}
	if e.OnIncomingRPC == nil {
		return errors.New("field OnIncomingRPC must be set")
	}
	if e.OnEmitGossip == nil {
		return errors.New("field OnEmitGossip must be set")
	}

	if e.PeerInitiatedGroupLimitPerTopic == 0 {
		e.PeerInitiatedGroupLimitPerTopic = defaultPeerInitiatedGroupLimitPerTopic
	}
	if e.PeerInitiatedGroupLimitPerTopicPerPeer == 0 {
		e.PeerInitiatedGroupLimitPerTopicPerPeer = defaultPeerInitiatedGroupLimitPerTopicPerPeer
	}

	e.statePerTopicPerGroup = make(map[string]map[string]*partialMessageStatePerGroupPerTopic[PeerState])
	e.peerInitiatedGroupCounter = make(map[string]*peerInitiatedGroupCounterState)

	return nil
}

func (e *PartialMessagesExtension[PeerState]) publish(topic string, groupID []byte, actions iter.Seq2[peer.ID, PublishAction]) error {
	var err error
	for p, action := range actions {
		if action.Err != nil {
			err = errors.Join(err, action.Err)
			continue
		}

		peerRequestedPartial := e.router.PeerRequestsPartial(p, topic)
		if (peerRequestedPartial && len(action.EncodedPartialMessage) > 0) || len(action.EncodedPartsMetadata) > 0 {
			var rpc pb.PartialMessagesExtension
			rpc.TopicID = &topic
			rpc.GroupID = []byte(groupID)
			if peerRequestedPartial {
				rpc.PartialMessage = action.EncodedPartialMessage
			}
			rpc.PartsMetadata = action.EncodedPartsMetadata
			e.sendRPC(p, &rpc)
		}
	}
	return err
}

// PublishActionsFn should return an iterator of PublishActions describing what
// messages to send to whom.
//
// The function SHOULD update the peer's peerState in the map to track the
// parts we've sent to this peer, along with any other peer-specific application
// data.
//
// The function should use the peerRequestsPartial to avoid encoding a partial
// message if the peer did not request one.
//
// A peer's peerState will be the zero value (nil for Pointer types) if this is
// the first time interacting with this peer. Applications can still eagerly
// push data in this case by returning the appropriate PublishAction.
//
// Implementations SHOULD avoid returning duplicate or redundant
// EncodedPartsMetadata i.e. if a previously sent PartsMetadata is up to date,
// implementations SHOULD return `nil` for `EncodedPartsMetadata`.
type PublishActionsFn[PeerState any] func(peerStates map[peer.ID]PeerState, peerRequestsPartial func(peer.ID) bool) iter.Seq2[peer.ID, PublishAction]

func (e *PartialMessagesExtension[PeerState]) PublishPartial(topic string, groupID []byte, publishActionsFn PublishActionsFn[PeerState]) error {
	gState, err := e.groupState(topic, groupID, false, "")
	if err != nil {
		return err
	}

	gState.groupTTL = max(e.GroupTTLByHeatbeat, minGroupTTL)

	e.initPeerState(topic, gState)
	return e.publish(topic, []byte(groupID), publishActionsFn(
		gState.peerState,
		func(p peer.ID) bool {
			return e.router.PeerRequestsPartial(p, topic)
		},
	))
}

// initPeerState initializes the peer state of MeshPeers that aren't already in
// our peerState map.
func (e *PartialMessagesExtension[PeerState]) initPeerState(topic string, gState *partialMessageStatePerGroupPerTopic[PeerState]) {
	for p := range e.router.MeshPeers(topic) {
		if _, ok := gState.peerState[p]; !ok {
			var newState PeerState
			gState.peerState[p] = newState
		}
	}
}

func (e *PartialMessagesExtension[PeerState]) OnClosedOutboundStream(id peer.ID) {
	for topic, tState := range e.statePerTopicPerGroup {
		for _, gState := range tState {
			delete(gState.peerState, id)
		}
		if ctr, ok := e.peerInitiatedGroupCounter[topic]; ok {
			ctr.OnClosedOutboundStream(id)
		}
	}
}

func (e *PartialMessagesExtension[PeerState]) Heartbeat() {
	for topic, tState := range e.statePerTopicPerGroup {
		for group, gState := range tState {
			if gState.groupTTL == 0 || len(gState.peerState) == 0 {
				delete(tState, group)
				if len(tState) == 0 {
					delete(e.statePerTopicPerGroup, topic)
				}
				if gState.remotePeerInitiated() {
					e.peerInitiatedGroupCounter[topic].Dec(gState.initiatedBy)
				}
			} else {
				gState.groupTTL--
			}
		}
	}
}

func (e *PartialMessagesExtension[PeerState]) EmitGossip(topic string, peers []peer.ID) {
	tState, ok := e.statePerTopicPerGroup[topic]
	if !ok {
		return
	}

	for group, gState := range tState {
		if gState.remotePeerInitiated() {
			continue
		}
		untrackedPeers := make([]peer.ID, 0, len(peers))
		for _, p := range peers {
			_, ok := gState.peerState[p]
			if !ok {
				var newState PeerState
				gState.peerState[p] = newState
				untrackedPeers = append(untrackedPeers, p)
			}
		}
		if len(untrackedPeers) > 0 {
			e.OnEmitGossip(topic, []byte(group), untrackedPeers, gState.peerState)
		}
	}
}

func (e *PartialMessagesExtension[PeerState]) sendRPC(to peer.ID, rpc *pb.PartialMessagesExtension) {
	e.Logger.Debug("Sending RPC", "to", to, "rpc", rpc)
	e.router.SendRPC(to, rpc, false)
}

func (e *PartialMessagesExtension[PeerState]) HandleRPC(from peer.ID, rpc *pb.PartialMessagesExtension) error {
	if rpc == nil {
		return nil
	}

	e.Logger.Debug("Received RPC", "from", from, "rpc", rpc)
	topic := rpc.GetTopicID()
	groupID := rpc.GroupID

	state, err := e.groupState(topic, groupID, true, from)
	if err != nil {
		return err
	}

	err = e.OnIncomingRPC(from, state.peerState, rpc)
	if err != nil {
		return err
	}

	return nil
}

type peerInitiatedGroupCounterState struct {
	// total number of peer initiated groups
	total int
	// number of groups initiated per peer
	perPeer map[peer.ID]int
}

var errPeerInitiatedGroupTotalLimitReached = errors.New("too many peer initiated group states")
var errPeerInitiatedGroupLimitReached = errors.New("too many peer initiated group states for this peer")

func (ctr *peerInitiatedGroupCounterState) Inc(totalLimit int, peerLimit int, id peer.ID) error {
	if ctr.total >= totalLimit {
		return errPeerInitiatedGroupTotalLimitReached
	}
	if ctr.perPeer == nil {
		ctr.perPeer = make(map[peer.ID]int)
	}
	if ctr.perPeer[id] >= peerLimit {
		return errPeerInitiatedGroupLimitReached
	}
	ctr.total++
	ctr.perPeer[id]++
	return nil
}

func (ctr *peerInitiatedGroupCounterState) Dec(id peer.ID) {
	if _, ok := ctr.perPeer[id]; ok {
		ctr.total--
		ctr.perPeer[id]--
		if ctr.perPeer[id] == 0 {
			delete(ctr.perPeer, id)
		}
	}
}

func (ctr *peerInitiatedGroupCounterState) OnClosedOutboundStream(id peer.ID) {
	if n, ok := ctr.perPeer[id]; ok {
		ctr.total -= n
		delete(ctr.perPeer, id)
	}
}
