package bitswap

import (
	"context"
	"fmt"

	"github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/internal/defaults"
	"github.com/ipfs/boxo/bitswap/message"
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/bitswap/server"
	"github.com/ipfs/boxo/bitswap/tracer"
	"github.com/ipfs/go-metrics-interface"

	blockstore "github.com/ipfs/boxo/blockstore"
	exchange "github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"

	"go.uber.org/multierr"
)

var log = logging.Logger("bitswap")

// old interface we are targeting
type bitswap interface {
	Close() error
	GetBlock(ctx context.Context, k cid.Cid) (blocks.Block, error)
	GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error)
	GetWantBlocks() []cid.Cid
	GetWantHaves() []cid.Cid
	GetWantlist() []cid.Cid
	IsOnline() bool
	LedgerForPeer(p peer.ID) *server.Receipt
	NewSession(ctx context.Context) exchange.Fetcher
	NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error
	PeerConnected(p peer.ID)
	PeerDisconnected(p peer.ID)
	ReceiveError(err error)
	ReceiveMessage(ctx context.Context, p peer.ID, incoming message.BitSwapMessage)
	Stat() (*Stat, error)
	WantlistForPeer(p peer.ID) []cid.Cid
}

var (
	_                  exchange.SessionExchange = (*Bitswap)(nil)
	_                  bitswap                  = (*Bitswap)(nil)
	HasBlockBufferSize                          = defaults.HasBlockBufferSize
)

type Bitswap struct {
	*client.Client
	*server.Server

	tracer tracer.Tracer
	net    network.BitSwapNetwork
}

func New(ctx context.Context, net network.BitSwapNetwork, bstore blockstore.Blockstore, options ...Option) *Bitswap {
	bs := &Bitswap{
		net: net,
	}

	var serverOptions []server.Option
	var clientOptions []client.Option

	for _, o := range options {
		switch typedOption := o.v.(type) {
		case server.Option:
			serverOptions = append(serverOptions, typedOption)
		case client.Option:
			clientOptions = append(clientOptions, typedOption)
		case option:
			typedOption(bs)
		default:
			panic(fmt.Errorf("unknown option type passed to bitswap.New, got: %T, %v; expected: %T, %T or %T", typedOption, typedOption, server.Option(nil), client.Option(nil), option(nil)))
		}
	}

	if bs.tracer != nil {
		var tracer tracer.Tracer = nopReceiveTracer{bs.tracer}
		clientOptions = append(clientOptions, client.WithTracer(tracer))
		serverOptions = append(serverOptions, server.WithTracer(tracer))
	}

	if HasBlockBufferSize != defaults.HasBlockBufferSize {
		serverOptions = append(serverOptions, server.HasBlockBufferSize(HasBlockBufferSize))
	}

	ctx = metrics.CtxSubScope(ctx, "bitswap")

	bs.Server = server.New(ctx, net, bstore, serverOptions...)
	bs.Client = client.New(ctx, net, bstore, append(clientOptions, client.WithBlockReceivedNotifier(bs.Server))...)
	net.Start(bs) // use the polyfill receiver to log received errors and trace messages only once

	return bs
}

func (bs *Bitswap) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	return multierr.Combine(
		bs.Client.NotifyNewBlocks(ctx, blks...),
		bs.Server.NotifyNewBlocks(ctx, blks...),
	)
}

type Stat struct {
	Wantlist         []cid.Cid
	Peers            []string
	BlocksReceived   uint64
	DataReceived     uint64
	DupBlksReceived  uint64
	DupDataReceived  uint64
	MessagesReceived uint64
	BlocksSent       uint64
	DataSent         uint64
	ProvideBufLen    int
}

func (bs *Bitswap) Stat() (*Stat, error) {
	cs, err := bs.Client.Stat()
	if err != nil {
		return nil, err
	}
	ss, err := bs.Server.Stat()
	if err != nil {
		return nil, err
	}

	return &Stat{
		Wantlist:         cs.Wantlist,
		BlocksReceived:   cs.BlocksReceived,
		DataReceived:     cs.DataReceived,
		DupBlksReceived:  cs.DupBlksReceived,
		DupDataReceived:  cs.DupDataReceived,
		MessagesReceived: cs.MessagesReceived,
		Peers:            ss.Peers,
		BlocksSent:       ss.BlocksSent,
		DataSent:         ss.DataSent,
		ProvideBufLen:    ss.ProvideBufLen,
	}, nil
}

func (bs *Bitswap) Close() error {
	bs.net.Stop()
	return multierr.Combine(
		bs.Client.Close(),
		bs.Server.Close(),
	)
}

func (bs *Bitswap) WantlistForPeer(p peer.ID) []cid.Cid {
	if p == bs.net.Self() {
		return bs.Client.GetWantlist()
	}
	return bs.Server.WantlistForPeer(p)
}

func (bs *Bitswap) PeerConnected(p peer.ID) {
	bs.Client.PeerConnected(p)
	bs.Server.PeerConnected(p)
}

func (bs *Bitswap) PeerDisconnected(p peer.ID) {
	bs.Client.PeerDisconnected(p)
	bs.Server.PeerDisconnected(p)
}

func (bs *Bitswap) ReceiveError(err error) {
	log.Infof("Bitswap Client ReceiveError: %s", err)
	// TODO log the network error
	// TODO bubble the network error up to the parent context/error logger
}

func (bs *Bitswap) ReceiveMessage(ctx context.Context, p peer.ID, incoming message.BitSwapMessage) {
	if bs.tracer != nil {
		bs.tracer.MessageReceived(p, incoming)
	}

	bs.Client.ReceiveMessage(ctx, p, incoming)
	bs.Server.ReceiveMessage(ctx, p, incoming)
}
