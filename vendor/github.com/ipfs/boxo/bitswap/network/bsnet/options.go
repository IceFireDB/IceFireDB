package bsnet

import (
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type NetOpt func(*Settings)

type Settings struct {
	ProtocolPrefix     protocol.ID
	SupportedProtocols []protocol.ID
	connEvtMgr         *network.ConnectEventManager
}

func Prefix(prefix protocol.ID) NetOpt {
	return func(settings *Settings) {
		settings.ProtocolPrefix = prefix
	}
}

func SupportedProtocols(protos []protocol.ID) NetOpt {
	return func(settings *Settings) {
		settings.SupportedProtocols = protos
	}
}

// WithConnectEventManager allows to set the ConnectEventManager. Upon
// Start(), we will run SetListeners(). If not provided, an event manager will
// be created internally. This allows re-using the event manager among several
// Network instances.
func WithConnectEventManager(evm *network.ConnectEventManager) NetOpt {
	return func(settings *Settings) {
		settings.connEvtMgr = evm
	}
}
