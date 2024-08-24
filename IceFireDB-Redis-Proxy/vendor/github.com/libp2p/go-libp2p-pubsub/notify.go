package pubsub

import (
	"github.com/libp2p/go-libp2p-core/network"
	ma "github.com/multiformats/go-multiaddr"
)

var _ network.Notifiee = (*PubSubNotif)(nil)

type PubSubNotif PubSub

func (p *PubSubNotif) OpenedStream(n network.Network, s network.Stream) {
}

func (p *PubSubNotif) ClosedStream(n network.Network, s network.Stream) {
}

func (p *PubSubNotif) Connected(n network.Network, c network.Conn) {
	go func() {
		select {
		case p.newPeers <- c.RemotePeer():
		case <-p.ctx.Done():
		}
	}()
}

func (p *PubSubNotif) Disconnected(n network.Network, c network.Conn) {
}

func (p *PubSubNotif) Listen(n network.Network, _ ma.Multiaddr) {
}

func (p *PubSubNotif) ListenClose(n network.Network, _ ma.Multiaddr) {
}

func (p *PubSubNotif) Initialize() {
	for _, pr := range p.host.Network().Peers() {
		select {
		case p.newPeers <- pr:
		case <-p.ctx.Done():
		}
	}
}
