package network

import (
	"context"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/ipfs/boxo/bitswap/network/internal"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

var (
	// ProtocolBitswapNoVers is equivalent to the legacy bitswap protocol
	ProtocolBitswapNoVers = internal.ProtocolBitswapNoVers
	// ProtocolBitswapOneZero is the prefix for the legacy bitswap protocol
	ProtocolBitswapOneZero = internal.ProtocolBitswapOneZero
	// ProtocolBitswapOneOne is the prefix for version 1.1.0
	ProtocolBitswapOneOne = internal.ProtocolBitswapOneOne
	// ProtocolBitswap is the current version of the bitswap protocol: 1.2.0
	ProtocolBitswap = internal.ProtocolBitswap
)

// BitSwapNetwork provides network connectivity for BitSwap sessions.
type BitSwapNetwork interface {
	Self() peer.ID

	// SendMessage sends a BitSwap message to a peer.
	SendMessage(
		context.Context,
		peer.ID,
		bsmsg.BitSwapMessage) error

	// Start registers the Reciver and starts handling new messages, connectivity events, etc.
	Start(...Receiver)
	// Stop stops the network service.
	Stop()

	Connect(context.Context, peer.AddrInfo) error
	DisconnectFrom(context.Context, peer.ID) error

	NewMessageSender(context.Context, peer.ID, *MessageSenderOpts) (MessageSender, error)

	ConnectionManager() connmgr.ConnManager

	Stats() Stats

	Pinger
}

// MessageSender is an interface for sending a series of messages over the bitswap
// network
type MessageSender interface {
	SendMsg(context.Context, bsmsg.BitSwapMessage) error
	Close() error
	Reset() error
	// Indicates whether the remote peer supports HAVE / DONT_HAVE messages
	SupportsHave() bool
}

type MessageSenderOpts struct {
	MaxRetries       int
	SendTimeout      time.Duration
	SendErrorBackoff time.Duration
}

// Receiver is an interface that can receive messages from the BitSwapNetwork.
type Receiver interface {
	ReceiveMessage(
		ctx context.Context,
		sender peer.ID,
		incoming bsmsg.BitSwapMessage)

	ReceiveError(error)

	// Connected/Disconnected warns bitswap about peer connections.
	PeerConnected(peer.ID)
	PeerDisconnected(peer.ID)
}

// Routing is an interface to providing and finding providers on a bitswap
// network.
type Routing interface {
	routing.ContentDiscovery

	// Provide provides the key to the network.
	Provide(context.Context, cid.Cid) error
}

// Pinger is an interface to ping a peer and get the average latency of all pings
type Pinger interface {
	// Ping a peer
	Ping(context.Context, peer.ID) ping.Result
	// Get the average latency of all pings
	Latency(peer.ID) time.Duration
}

// Stats is a container for statistics about the bitswap network
// the numbers inside are specific to bitswap, and not any other protocols
// using the same underlying network.
type Stats struct {
	MessagesSent  uint64
	MessagesRecvd uint64
}
