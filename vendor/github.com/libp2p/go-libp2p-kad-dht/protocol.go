package dht

import (
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var (
	// ProtocolDHT is the default DHT protocol.
	ProtocolDHT protocol.ID = amino.ProtocolID
	// DefaultProtocols spoken by the DHT.
	DefaultProtocols = amino.Protocols
)
