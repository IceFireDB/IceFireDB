package network

import (
	"context"
	"fmt"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

type router struct {
	Bitswap   BitSwapNetwork
	HTTP      BitSwapNetwork
	Peerstore peerstore.Peerstore
}

// New returns a BitSwapNetwork supported by underlying IPFS host.
func New(pstore peerstore.Peerstore, bitswap BitSwapNetwork, http BitSwapNetwork) BitSwapNetwork {
	if bitswap == nil && http == nil {
		panic("bad exchange network router initialization: need bitswap or http")
	}

	if http == nil {
		return bitswap
	}

	if bitswap == nil {
		return http
	}

	if http.Self() != bitswap.Self() {
		panic("http and bitswap network report different peer IDs")
	}

	return &router{
		Peerstore: pstore,
		Bitswap:   bitswap,
		HTTP:      http,
	}
}

func (rt *router) Start(receivers ...Receiver) {
	// When using the router, it is best if both Bitswap and HTTP are
	// sharing the same connectionEventManager. Since a connection event
	// manager has the power to remove a peer from everywhere, and the
	// router may in some edge cases or due to bugs route requests wrongly,
	// this may cause connection-state mismatches.
	//
	// The Start() calls call ConnectEventManager.Start(), which makes
	// sure only one worker starts. SetListeners does not have effect if
	// called when the manager has started. We still call Start() on both
	// in case they are using separate connectEventManagers.
	rt.Bitswap.Start(receivers...)
	rt.HTTP.Start(receivers...)
}

func (rt *router) Stop() {
	rt.Bitswap.Stop()
	rt.HTTP.Stop()
}

// Self returns the peer ID of the network.
func (rt *router) Self() peer.ID {
	// Self is used on the bitswap server,
	// and on the client on the message queue
	// and session manager.
	// We ensure during initialization that we are using
	// the same host for both networks.
	return rt.Bitswap.Self()
}

func (rt *router) Ping(ctx context.Context, p peer.ID) ping.Result {
	if rt.HTTP.IsConnectedToPeer(ctx, p) {
		return rt.HTTP.Ping(ctx, p)
	}
	return rt.Bitswap.Ping(ctx, p)
}

func (rt *router) Latency(p peer.ID) time.Duration {
	if rt.HTTP.IsConnectedToPeer(context.Background(), p) {
		return rt.HTTP.Latency(p)
	}
	return rt.Bitswap.Latency(p)
}

func (rt *router) Host() host.Host {
	if rt.Bitswap == nil {
		return rt.HTTP.Host()
	}
	return rt.Bitswap.Host()
}

func (rt *router) SendMessage(ctx context.Context, p peer.ID, msg bsmsg.BitSwapMessage) error {
	// SendMessage is only used by bitswap server on sendBlocks(). We
	// should not be passing a router to the bitswap server but we try to
	// make our best.

	// If the message has blocks, send it via bitswap.
	if len(msg.Blocks()) > 0 {
		return rt.Bitswap.SendMessage(ctx, p, msg)
	}

	// Otherwise, assume it's a wantlist. Follow usual prioritization
	// of HTTP when possible.
	if rt.HTTP.IsConnectedToPeer(ctx, p) {
		return rt.HTTP.SendMessage(ctx, p, msg)
	}
	return rt.Bitswap.SendMessage(ctx, p, msg)
}

// Connect attempts to connect to a peer. It prioritizes HTTP connections over
// bitswap, but does a bitswap connect if HTTP Connect fails (i.e. when there
// are no HTTP addresses.
func (rt *router) Connect(ctx context.Context, p peer.AddrInfo) error {
	htaddrs, bsaddrs := SplitHTTPAddrs(p)
	if len(htaddrs.Addrs) == 0 {
		return rt.Bitswap.Connect(ctx, bsaddrs)
	}

	err := rt.HTTP.Connect(ctx, htaddrs)
	if err != nil {
		return rt.Bitswap.Connect(ctx, bsaddrs)
	}
	return nil
}

func (rt *router) DisconnectFrom(ctx context.Context, p peer.ID) error {
	// DisconnectFrom is only called from bitswap.Server, on failures
	// receiving a bitswap message. On HTTP, we don't "disconnect" unless
	// there are retrieval failures, which we handle internally.
	//
	// Result: only disconnect bitswap, when there are bitswap addresses
	// involved.
	pi := rt.Peerstore.PeerInfo(p)
	_, bsaddrs := SplitHTTPAddrs(pi)
	if len(bsaddrs.Addrs) > 0 {
		return rt.Bitswap.DisconnectFrom(ctx, p)
	}
	return nil
}

// IsConnectedToPeer returns true if HTTP or Bitswap are.
func (rt *router) IsConnectedToPeer(ctx context.Context, p peer.ID) bool {
	return rt.HTTP.IsConnectedToPeer(ctx, p) || rt.Bitswap.IsConnectedToPeer(ctx, p)
}

func (rt *router) Stats() Stats {
	htstats := rt.HTTP.Stats()
	bsstats := rt.Bitswap.Stats()
	return Stats{
		MessagesRecvd: htstats.MessagesRecvd + bsstats.MessagesRecvd,
		MessagesSent:  htstats.MessagesSent + bsstats.MessagesSent,
	}
}

// NewMessageSender returns a MessageSender using the HTTP network when
// connecting over HTTP is possible, and bitswap otherwise.
func (rt *router) NewMessageSender(ctx context.Context, p peer.ID, opts *MessageSenderOpts) (MessageSender, error) {
	// This will error if not connected via HTTP.
	mss, err := rt.HTTP.NewMessageSender(ctx, p, opts)
	if err == nil {
		return mss, nil
	}

	// Before we route to bitswap, check we can connect.  If we let
	// network.Bitswap do this, it will trigger a Disconnect() event on
	// failure, which is annoying because it happens 5 seconds after the
	// fact. That, in turn, causes havoc if it was indeed an HTTP peer and
	// we were just temporally disconnected from it

	// If we can connect or were already connected via Bitswap, then route
	// the request.
	bserr := rt.Bitswap.Connect(ctx, peer.AddrInfo{ID: p})
	if bserr == nil {
		return rt.Bitswap.NewMessageSender(ctx, p, opts)
	}

	// Otherwise, do a final HTTP attempt, because we may have lost some
	// time performing the check above (peer-routing lookup), and we may
	// have connected to HTTP after all in the meantime.
	mss, err = rt.HTTP.NewMessageSender(ctx, p, opts)
	if err != nil {
		// Might still be bad luck or a confirmation this is not an
		// HTTP peer, but since we know bitswap fails, we can feel
		// good about failing here finally.
		return nil, fmt.Errorf("NewMessageSender: cannot connect via HTTP (%s) nor Bitswap (%s)", err, bserr)
	}

	return mss, nil
}

func (rt *router) TagPeer(p peer.ID, tag string, w int) {
	// tag once only if they are the same.
	if rt.HTTP.Self() == rt.Bitswap.Self() {
		rt.HTTP.TagPeer(p, tag, w)
		return
	}

	if rt.HTTP.IsConnectedToPeer(context.Background(), p) {
		rt.HTTP.TagPeer(p, tag, w)
		return
	}
	rt.Bitswap.TagPeer(p, tag, w)
}

func (rt *router) UntagPeer(p peer.ID, tag string) {
	// tag once only if they are the same.
	if rt.HTTP.Self() == rt.Bitswap.Self() {
		rt.HTTP.UntagPeer(p, tag)
		return
	}

	if rt.HTTP.IsConnectedToPeer(context.Background(), p) {
		rt.HTTP.UntagPeer(p, tag)
		return
	}
	rt.Bitswap.UntagPeer(p, tag)
}

func (rt *router) Protect(p peer.ID, tag string) {
	if rt.HTTP.IsConnectedToPeer(context.Background(), p) {
		rt.HTTP.Protect(p, tag)
		return
	}
	rt.Bitswap.Protect(p, tag)
}

func (rt *router) Unprotect(p peer.ID, tag string) bool {
	if rt.HTTP.IsConnectedToPeer(context.Background(), p) {
		return rt.HTTP.Unprotect(p, tag)
	}
	return rt.Bitswap.Unprotect(p, tag)
}
