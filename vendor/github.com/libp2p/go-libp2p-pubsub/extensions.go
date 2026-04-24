package pubsub

import (
	"errors"
	"iter"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type PeerExtensions struct {
	TestExtension   bool
	PartialMessages bool
}

type TestExtensionConfig struct {
	OnReceiveTestExtension func(from peer.ID)
}

func WithTestExtension(c TestExtensionConfig) Option {
	return func(ps *PubSub) error {
		if rt, ok := ps.rt.(*GossipSubRouter); ok {
			rt.extensions.testExtension = &testExtension{
				sendRPC:                rt.extensions.sendRPC,
				onReceiveTestExtension: c.OnReceiveTestExtension,
			}
			rt.extensions.myExtensions.TestExtension = true
		}
		return nil
	}
}

func hasPeerExtensions(rpc *RPC) bool {
	if rpc != nil && rpc.Control != nil && rpc.Control.Extensions != nil {
		return true
	}
	return false
}

func peerExtensionsFromRPC(rpc *RPC) PeerExtensions {
	out := PeerExtensions{}
	if hasPeerExtensions(rpc) {
		out.TestExtension = rpc.Control.Extensions.GetTestExtension()
		out.PartialMessages = rpc.Control.Extensions.GetPartialMessages()
	}
	return out
}

func (pe *PeerExtensions) ExtendRPC(rpc *RPC) *RPC {
	if pe.TestExtension {
		if rpc.Control == nil {
			rpc.Control = &pubsub_pb.ControlMessage{}
		}
		if rpc.Control.Extensions == nil {
			rpc.Control.Extensions = &pubsub_pb.ControlExtensions{}
		}
		rpc.Control.Extensions.TestExtension = &pe.TestExtension
	}
	if pe.PartialMessages {
		if rpc.Control == nil {
			rpc.Control = &pubsub_pb.ControlMessage{}
		}
		if rpc.Control.Extensions == nil {
			rpc.Control.Extensions = &pubsub_pb.ControlExtensions{}
		}
		rpc.Control.Extensions.PartialMessages = &pe.PartialMessages
	}
	return rpc
}

// Using an interface type to avoid bubbling up PartialMessage's generics up to
// pubsub.
//
// Purposely not trying to make a generic extension interface as there is only
// one real consumer (partial messages). This may change in the future.
type partialMessageInterface interface {
	OnClosedOutboundStream(peer.ID)
	HandleRPC(from peer.ID, rpc *pubsub_pb.PartialMessagesExtension) error
	Heartbeat()
	EmitGossip(topic string, peers []peer.ID)
}

type extensionsState struct {
	myExtensions      PeerExtensions
	peerExtensions    map[peer.ID]PeerExtensions // peer's extensions
	sentExtensions    map[peer.ID]struct{}
	reportMisbehavior func(peer.ID)
	sendRPC           func(p peer.ID, r *RPC, urgent bool)
	testExtension     *testExtension

	partialMessagesExtension partialMessageInterface
}

func newExtensionsState(myExtensions PeerExtensions, reportMisbehavior func(peer.ID), sendRPC func(peer.ID, *RPC, bool)) *extensionsState {
	return &extensionsState{
		myExtensions:      myExtensions,
		peerExtensions:    make(map[peer.ID]PeerExtensions),
		sentExtensions:    make(map[peer.ID]struct{}),
		reportMisbehavior: reportMisbehavior,
		sendRPC:           sendRPC,
		testExtension:     nil,
	}
}

func (es *extensionsState) HandleRPC(rpc *RPC) error {
	if _, ok := es.peerExtensions[rpc.from]; !ok {
		// We know this is the first message because we didn't have extensions
		// for this peer, and we always set extensions on the first rpc.
		es.peerExtensions[rpc.from] = peerExtensionsFromRPC(rpc)
		if _, ok := es.sentExtensions[rpc.from]; ok {
			// We just finished both sending and receiving the extensions
			// control message.
			es.extensionsOnNewOutboundStream(rpc.from)
		}
	} else {
		// We already have an extension for this peer. If they send us another
		// extensions control message, that is a protocol error. We should
		// down score them because they are misbehaving.
		if hasPeerExtensions(rpc) {
			es.reportMisbehavior(rpc.from)
		}
	}

	return es.extensionsHandleRPC(rpc)
}

func (es *extensionsState) OnNewIncomingStream(peer.ID, protocol.ID) {
}

func (es *extensionsState) OnClosedIncomingStream(id peer.ID, _ protocol.ID) {
	delete(es.peerExtensions, id)
	if len(es.peerExtensions) == 0 {
		es.peerExtensions = make(map[peer.ID]PeerExtensions)
	}
}

func (es *extensionsState) OnNewOutboundStream(id peer.ID, helloPacket *RPC) *RPC {
	// Send our extensions as the first message.
	helloPacket = es.myExtensions.ExtendRPC(helloPacket)

	es.sentExtensions[id] = struct{}{}
	if _, ok := es.peerExtensions[id]; ok {
		// We've just finished sending and receiving the extensions control
		// message.
		es.extensionsOnNewOutboundStream(id)
	}
	return helloPacket
}

func (es *extensionsState) OnClosedOutboundStream(id peer.ID) {
	_, recvdExt := es.peerExtensions[id]
	_, sentExt := es.sentExtensions[id]
	if recvdExt && sentExt {
		// Add peer was previously called, so we need to call remove peer
		es.extensionsOnClosedOutboundStream(id)
	}
	delete(es.sentExtensions, id)
	if len(es.sentExtensions) == 0 {
		es.sentExtensions = make(map[peer.ID]struct{})
	}
}

// extensionsOnNewOutboundStream is only called once we've both sent and received the
// extensions control message.
func (es *extensionsState) extensionsOnNewOutboundStream(id peer.ID) {
	if es.myExtensions.TestExtension && es.peerExtensions[id].TestExtension {
		es.testExtension.OnNewOutboundStream(id)
	}
}

// extensionsOnClosedOutboundStream is always called after extensionsOnNewOutboundStream.
func (es *extensionsState) extensionsOnClosedOutboundStream(id peer.ID) {
	if es.myExtensions.PartialMessages && es.peerExtensions[id].PartialMessages {
		es.partialMessagesExtension.OnClosedOutboundStream(id)
	}
}

func (es *extensionsState) extensionsHandleRPC(rpc *RPC) error {
	if es.myExtensions.TestExtension && es.peerExtensions[rpc.from].TestExtension {
		es.testExtension.HandleRPC(rpc.from, rpc.TestExtension)
	}

	if es.myExtensions.PartialMessages && es.peerExtensions[rpc.from].PartialMessages && rpc.Partial != nil {
		err := es.partialMessagesExtension.HandleRPC(rpc.from, rpc.Partial)
		if err != nil {
			return err
		}
	}

	return nil
}

func (es *extensionsState) Heartbeat() {
	if es.myExtensions.PartialMessages {
		es.partialMessagesExtension.Heartbeat()
	}
}

func WithPartialMessagesExtension[PeerState any](pm *partialmessages.PartialMessagesExtension[PeerState]) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return errors.New("pubsub router is not gossipsub")
		}
		err := pm.Init(partialMessageRouter{gs})
		if err != nil {
			return err
		}

		gs.extensions.myExtensions.PartialMessages = true
		gs.extensions.partialMessagesExtension = pm
		return nil
	}
}

// PublishPartial uses the given PubSub instance to publish partial messages.
// This is a standalone function rather a method on PubSub due to the generic
// type parameter.
func PublishPartial[PeerState any](ps *PubSub, topic string, groupID []byte, publishActionsFn partialmessages.PublishActionsFn[PeerState]) error {
	resp := make(chan error, 1)
	select {
	case <-ps.ctx.Done():
		return ps.ctx.Err()
	case ps.eval <- func() {
		defer close(resp)

		rt, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			resp <- errors.New("partial publishing is only supported by the GossipSub router")
			return
		}

		if rt.extensions.partialMessagesExtension == nil {
			resp <- errors.New("partial publishing is not enabled")
			return
		}
		pme, ok := rt.extensions.partialMessagesExtension.(*partialmessages.PartialMessagesExtension[PeerState])
		if !ok {
			resp <- errors.New("incompatible partial messages extension type")
			return
		}

		resp <- pme.PublishPartial(topic, groupID, publishActionsFn)
	}:
	}

	select {
	case <-ps.ctx.Done():
		return ps.ctx.Err()
	case r := <-resp:
		return r
	}
}

type partialMessageRouter struct {
	gs *GossipSubRouter
}

// PeerRequestsPartial returns true if a peer requested partial messages on this topic.
//
// It does not check if we support partial messages on the topic, because we may
// not be subscribed to that topic and thus not have that information.
// Callers should not use this if they don't support partial messages on this topic.
func (r partialMessageRouter) PeerRequestsPartial(peer peer.ID, topic string) bool {
	return r.gs.peerRequestsPartial(peer, topic)
}

// MeshPeers implements partialmessages.Router.
func (r partialMessageRouter) MeshPeers(topic string) iter.Seq[peer.ID] {
	return func(yield func(peer.ID) bool) {
		peerSet, ok := r.gs.mesh[topic]
		if !ok {
			// Possibly a fanout topic
			peerSet = r.gs.getFanoutPeersForPublishing(topic)
		}

		for peer := range peerSet {
			if r.gs.extensions.peerExtensions[peer].PartialMessages &&
				(r.gs.iRequestPartial(topic) && r.gs.peerSupportsSendingPartial(peer, topic)) || (r.gs.iSupportSendingPartial(topic) && r.gs.peerRequestsPartial(peer, topic)) {
				if !yield(peer) {
					return
				}
			}
		}
	}
}

// SendRPC implements partialmessages.Router.
func (r partialMessageRouter) SendRPC(p peer.ID, rpc *pubsub_pb.PartialMessagesExtension, urgent bool) {
	r.gs.sendRPC(p, &RPC{
		RPC: pubsub_pb.RPC{
			Partial: rpc,
		},
	}, urgent)
}

var _ partialmessages.Router = partialMessageRouter{}
