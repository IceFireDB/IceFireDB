package defaults

import (
	"encoding/binary"
	"time"
)

const (
	// ProvSearchDelay specifies how long to wait before we start
	// broadcasting outstanding wants for the first time.
	ProvSearchDelay = time.Second

	// Maximum number of providers that are looked up per find request by the
	// default bitswap client. 0 value means unlimited.
	BitswapClientDefaultMaxProviders = 10
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

	// Maximum size of the wantlist we are willing to keep in memory.
	MaxQueuedWantlistEntiresPerPeer = 1024

	// Copied from github.com/ipfs/go-verifcid#maximumHashLength
	// FIXME: expose this in go-verifcid.
	MaximumHashLength = 128
	MaximumAllowedCid = binary.MaxVarintLen64*4 + MaximumHashLength

	// RebroadcastDelay is the default delay to trigger broadcast of
	// random CIDs in the wantlist.
	RebroadcastDelay = time.Minute

	// DefaultWantHaveReplaceSize controls the implicit behavior of WithWantHaveReplaceSize.
	DefaultWantHaveReplaceSize = 1024
)
