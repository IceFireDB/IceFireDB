package pubsub

import (
	"context"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func (ps *PubSub) watchForNewPeers(ctx context.Context) {
	// We don't bother subscribing to "connectivity" events because we always run identify after
	// every new connection.
	sub, err := ps.host.EventBus().Subscribe([]interface{}{
		&event.EvtPeerIdentificationCompleted{},
		&event.EvtPeerProtocolsUpdated{},
	})
	if err != nil {
		log.Errorf("failed to subscribe to peer identification events: %v", err)
		return
	}
	defer sub.Close()

	ps.newPeersPrioLk.RLock()
	ps.newPeersMx.Lock()
	for _, pid := range ps.host.Network().Peers() {
		if ps.host.Network().Connectedness(pid) != network.Connected {
			continue
		}
		ps.newPeersPend[pid] = struct{}{}
	}
	ps.newPeersMx.Unlock()
	ps.newPeersPrioLk.RUnlock()

	select {
	case ps.newPeers <- struct{}{}:
	default:
	}

	var supportsProtocol func(protocol.ID) bool
	if ps.protoMatchFunc != nil {
		var supportedProtocols []func(protocol.ID) bool
		for _, proto := range ps.rt.Protocols() {

			supportedProtocols = append(supportedProtocols, ps.protoMatchFunc(proto))
		}
		supportsProtocol = func(proto protocol.ID) bool {
			for _, fn := range supportedProtocols {
				if (fn)(proto) {
					return true
				}
			}
			return false
		}
	} else {
		supportedProtocols := make(map[protocol.ID]struct{})
		for _, proto := range ps.rt.Protocols() {
			supportedProtocols[proto] = struct{}{}
		}
		supportsProtocol = func(proto protocol.ID) bool {
			_, ok := supportedProtocols[proto]
			return ok
		}
	}

	for ctx.Err() == nil {
		var ev any
		select {
		case <-ctx.Done():
			return
		case ev = <-sub.Out():
		}

		var protos []protocol.ID
		var peer peer.ID
		switch ev := ev.(type) {
		case event.EvtPeerIdentificationCompleted:
			peer = ev.Peer
			protos = ev.Protocols
		case event.EvtPeerProtocolsUpdated:
			peer = ev.Peer
			protos = ev.Added
		default:
			continue
		}

		// We don't bother checking connectivity (connected and non-"limited") here because
		// we'll check when actually handling the new peer.

		for _, p := range protos {
			if supportsProtocol(p) {
				ps.notifyNewPeer(peer)
				break
			}
		}
	}

}

func (ps *PubSub) notifyNewPeer(peer peer.ID) {
	ps.newPeersPrioLk.RLock()
	ps.newPeersMx.Lock()
	ps.newPeersPend[peer] = struct{}{}
	ps.newPeersMx.Unlock()
	ps.newPeersPrioLk.RUnlock()

	select {
	case ps.newPeers <- struct{}{}:
	default:
	}
}
