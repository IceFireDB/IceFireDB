package pubsub

import (
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

type testExtension struct {
	sendRPC                func(p peer.ID, r *RPC, urgent bool)
	onReceiveTestExtension func(peer.ID)
}

func (e *testExtension) AddPeer(id peer.ID) {
	e.sendRPC(id, &RPC{
		RPC: pubsub_pb.RPC{
			TestExtension: &pubsub_pb.TestExtension{},
		},
	}, false)
}

func (e *testExtension) HandleRPC(from peer.ID, _ *pubsub_pb.TestExtension) {
	if e.onReceiveTestExtension != nil {
		e.onReceiveTestExtension(from)
	}
}
