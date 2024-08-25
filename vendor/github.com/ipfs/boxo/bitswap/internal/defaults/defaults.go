package defaults

import (
	"encoding/binary"
	"time"
)

const (
	// these requests take at _least_ two minutes at the moment.
	ProvideTimeout  = time.Minute * 3
	ProvSearchDelay = time.Second

	// Number of concurrent workers in decision engine that process requests to the blockstore
	BitswapEngineBlockstoreWorkerCount = 128
	// the total number of simultaneous threads sending outgoing messages
	BitswapTaskWorkerCount = 8
	// how many worker threads to start for decision engine task worker
	BitswapEngineTaskWorkerCount = 8
	// the total amount of bytes that a peer should have outstanding, it is utilized by the decision engine
	BitswapMaxOutstandingBytesPerPeer = 1 << 20
	// the number of bytes we attempt to make each outgoing bitswap message
	BitswapEngineTargetMessageSize = 16 * 1024
	// HasBlockBufferSize is the buffer size of the channel for new blocks
	// that need to be provided. They should get pulled over by the
	// provideCollector even before they are actually provided.
	// TODO: Does this need to be this large givent that?
	HasBlockBufferSize = 256

	// Maximum size of the wantlist we are willing to keep in memory.
	MaxQueuedWantlistEntiresPerPeer = 1024

	// Copied from github.com/ipfs/go-verifcid#maximumHashLength
	// FIXME: expose this in go-verifcid.
	MaximumHashLength = 128
	MaximumAllowedCid = binary.MaxVarintLen64*4 + MaximumHashLength

	// RebroadcastDelay is the default delay to trigger broadcast of
	// random CIDs in the wantlist.
	RebroadcastDelay = time.Minute
)
