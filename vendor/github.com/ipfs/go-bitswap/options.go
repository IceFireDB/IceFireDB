package bitswap

import (
	"time"

	"github.com/ipfs/go-bitswap/client"
	"github.com/ipfs/go-bitswap/server"
	"github.com/ipfs/go-bitswap/tracer"
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

func TaskWorkerCount(count int) Option {
	return Option{server.TaskWorkerCount(count)}
}

func ProvideEnabled(enabled bool) Option {
	return Option{server.ProvideEnabled(enabled)}
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

func WithTargetMessageSize(tms int) Option {
	return Option{server.WithTargetMessageSize(tms)}
}

func WithTaskComparator(comparator server.TaskComparator) Option {
	return Option{server.WithTaskComparator(comparator)}
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

func WithTracer(tap tracer.Tracer) Option {
	// Only trace the server, both receive the same messages anyway
	return Option{
		func(bs *Bitswap) {
			bs.tracer = tap
		},
	}
}
