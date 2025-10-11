package defaults

import (
	"encoding/binary"
	"time"

	"github.com/ipfs/boxo/verifcid"
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

	// MaximumHashLength is the maximum size for hash digests we accept.
	// This references the default from verifcid for consistency.
	MaximumHashLength = verifcid.DefaultMaxDigestSize

	// MaximumAllowedCid is the maximum total CID size we accept in bitswap messages.
	// Bitswap sends full CIDs (not just multihashes) on the wire, so we must
	// limit the total size to prevent DoS attacks from maliciously large CIDs.
	//
	// The calculation is based on the CID binary format:
	// - CIDv0: Just a multihash (hash type + hash length + hash digest)
	// - CIDv1: <version><multicodec><multihash>
	//   - version: varint (usually 1 byte for version 1)
	//   - multicodec: varint (usually 1-2 bytes for common codecs)
	//   - multihash: <hash-type><hash-length><hash-digest>
	//     - hash-type: varint (usually 1-2 bytes)
	//     - hash-length: varint (usually 1-2 bytes)
	//     - hash-digest: up to MaximumHashLength bytes
	//
	// We use binary.MaxVarintLen64*4 (40 bytes) to accommodate worst-case varint encoding:
	// - 1 varint for CID version (max 10 bytes)
	// - 1 varint for multicodec (max 10 bytes)
	// - 1 varint for multihash type (max 10 bytes)
	// - 1 varint for multihash length (max 10 bytes)
	// Total: 40 bytes overhead + MaximumHashLength (128) = 168 bytes max
	//
	// This prevents peers from sending CIDs with pathologically large varint encodings
	// that could exhaust memory or cause other issues.
	MaximumAllowedCid = binary.MaxVarintLen64*4 + MaximumHashLength

	// RebroadcastDelay is the default delay to trigger broadcast of
	// random CIDs in the wantlist.
	RebroadcastDelay = time.Minute

	// DefaultWantHaveReplaceSize controls the implicit behavior of WithWantHaveReplaceSize.
	DefaultWantHaveReplaceSize = 1024
)
