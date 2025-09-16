package pubsub

import (
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerExtensions struct {
	TestExtension bool
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
	}
	return out
}

func (pe *PeerExtensions) ExtendRPC(rpc *RPC) *RPC {
	if pe.TestExtension {
		if rpc.Control == nil {
			rpc.Control = &pubsub_pb.ControlMessage{}
		}
		rpc.Control.Extensions = &pubsub_pb.ControlExtensions{
			TestExtension: &pe.TestExtension,
		}
	}
	return rpc
}

type extensionsState struct {
	myExtensions      PeerExtensions
	peerExtensions    map[peer.ID]PeerExtensions // peer's extensions
	sentExtensions    map[peer.ID]struct{}
	reportMisbehavior func(peer.ID)
	sendRPC           func(p peer.ID, r *RPC, urgent bool)

	testExtension *testExtension
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

func (es *extensionsState) HandleRPC(rpc *RPC) {
	if _, ok := es.peerExtensions[rpc.from]; !ok {
		// We know this is the first message because we didn't have extensions
		// for this peer, and we always set extensions on the first rpc.
		es.peerExtensions[rpc.from] = peerExtensionsFromRPC(rpc)
		if _, ok := es.sentExtensions[rpc.from]; ok {
			// We just finished both sending and receiving the extensions
			// control message.
			es.extensionsAddPeer(rpc.from)
		}
	} else {
		// We already have an extension for this peer. If they send us another
		// extensions control message, that is a protocol error. We should
		// down score them because they are misbehaving.
		if hasPeerExtensions(rpc) {
			es.reportMisbehavior(rpc.from)
		}
	}

	es.extensionsHandleRPC(rpc)
}

func (es *extensionsState) AddPeer(id peer.ID, helloPacket *RPC) *RPC {
	// Send our extensions as the first message.
	helloPacket = es.myExtensions.ExtendRPC(helloPacket)

	es.sentExtensions[id] = struct{}{}
	if _, ok := es.peerExtensions[id]; ok {
		// We've just finished sending and receiving the extensions control
		// message.
		es.extensionsAddPeer(id)
	}
	return helloPacket
}

func (es *extensionsState) RemovePeer(id peer.ID) {
	_, recvdExt := es.peerExtensions[id]
	_, sentExt := es.sentExtensions[id]
	if recvdExt && sentExt {
		// Add peer was previously called, so we need to call remove peer
		es.extensionsRemovePeer(id)
	}
	delete(es.peerExtensions, id)
	if len(es.peerExtensions) == 0 {
		es.peerExtensions = make(map[peer.ID]PeerExtensions)
	}
	delete(es.sentExtensions, id)
	if len(es.sentExtensions) == 0 {
		es.sentExtensions = make(map[peer.ID]struct{})
	}
}

// extensionsAddPeer is only called once we've both sent and received the
// extensions control message.
func (es *extensionsState) extensionsAddPeer(id peer.ID) {
	if es.myExtensions.TestExtension && es.peerExtensions[id].TestExtension {
		es.testExtension.AddPeer(id)
	}
}

// extensionsRemovePeer is always called after extensionsAddPeer.
func (es *extensionsState) extensionsRemovePeer(id peer.ID) {
}

func (es *extensionsState) extensionsHandleRPC(rpc *RPC) {
	if es.myExtensions.TestExtension && es.peerExtensions[rpc.from].TestExtension {
		es.testExtension.HandleRPC(rpc.from, rpc.TestExtension)
	}
}
