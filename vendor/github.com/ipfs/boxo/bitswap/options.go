package bitswap

import (
	"time"

	"github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/server"
	"github.com/ipfs/boxo/bitswap/tracer"
	delay "github.com/ipfs/go-ipfs-delay"
)

type option func(*Bitswap)

// Option is interface{} of server.Option or client.Option or func(*Bitswap)
// wrapped in a struct to gain strong type checking.
type Option struct {
	v interface{}
}

func EngineBlockstoreWorkerCount(count int) Option {
	return Option{server.EngineBlockstoreWorkerCount(count)}
}

func EngineTaskWorkerCount(count int) Option {
	return Option{server.EngineTaskWorkerCount(count)}
}

func MaxOutstandingBytesPerPeer(count int) Option {
	return Option{server.MaxOutstandingBytesPerPeer(count)}
}

func MaxQueuedWantlistEntriesPerPeer(count uint) Option {
	return Option{server.MaxQueuedWantlistEntriesPerPeer(count)}
}

// MaxCidSize only affects the server.
// If it is 0 no limit is applied.
func MaxCidSize(n uint) Option {
	return Option{server.MaxCidSize(n)}
}

func TaskWorkerCount(count int) Option {
	return Option{server.TaskWorkerCount(count)}
}

func SetSendDontHaves(send bool) Option {
	return Option{server.SetSendDontHaves(send)}
}

func WithPeerBlockRequestFilter(pbrf server.PeerBlockRequestFilter) Option {
	return Option{server.WithPeerBlockRequestFilter(pbrf)}
}

func WithScoreLedger(scoreLedger server.ScoreLedger) Option {
	return Option{server.WithScoreLedger(scoreLedger)}
}

func WithPeerLedger(peerLedger server.PeerLedger) Option {
	return Option{server.WithPeerLedger(peerLedger)}
}

func WithTargetMessageSize(tms int) Option {
	return Option{server.WithTargetMessageSize(tms)}
}

func WithTaskComparator(comparator server.TaskComparator) Option {
	return Option{server.WithTaskComparator(comparator)}
}

// WithWantHaveReplaceSize sets the maximum size of a block in bytes up to
// which the bitswap server will replace a WantHave with a WantBlock response.
// See [server.WithWantHaveReplaceSize] for details.
func WithWantHaveReplaceSize(size int) Option {
	return Option{server.WithWantHaveReplaceSize(size)}
}

func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return Option{client.ProviderSearchDelay(newProvSearchDelay)}
}

func RebroadcastDelay(newRebroadcastDelay delay.D) Option {
	return Option{client.RebroadcastDelay(newRebroadcastDelay)}
}

func SetSimulateDontHavesOnTimeout(send bool) Option {
	return Option{client.SetSimulateDontHavesOnTimeout(send)}
}

func WithBlockReceivedNotifier(brn client.BlockReceivedNotifier) Option {
	return Option{client.WithBlockReceivedNotifier(brn)}
}

func WithoutDuplicatedBlockStats() Option {
	return Option{client.WithoutDuplicatedBlockStats()}
}

func WithTracer(tap tracer.Tracer) Option {
	// Only trace the server, both receive the same messages anyway
	return Option{
		option(func(bs *Bitswap) {
			bs.tracer = tap
		}),
	}
}

func WithClientOption(opt client.Option) Option {
	return Option{opt}
}

func WithServerOption(opt server.Option) Option {
	return Option{opt}
}
