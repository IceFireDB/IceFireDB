// Package amino provides protocol parameters and suggested default values for the [Amino DHT].
//
// [Amino DHT] is an implementation of the Kademlia distributed hash table (DHT) algorithm,
// originally designed for use in IPFS (InterPlanetary File System) network.
// This package defines key constants and protocol identifiers used in the Amino DHT implementation.
//
// [Amino DHT]: https://probelab.io/ipfs/amino/
package amino

import (
	"time"

	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	// ProtocolPrefix is the base prefix for Amono DHT protocols.
	ProtocolPrefix protocol.ID = "/ipfs"

	// ProtocolID is the latest protocol identifier for the Amino DHT.
	ProtocolID protocol.ID = "/ipfs/kad/1.0.0"

	// DefaultBucketSize is the Amino DHT bucket size (k in the Kademlia paper).
	// It represents the maximum number of peers stored in each
	// k-bucket of the routing table.
	DefaultBucketSize = 20

	// DefaultConcurrency is the suggested number of concurrent requests (alpha
	// in the Kademlia paper) for a given query path in Amino DHT. It
	// determines how many parallel lookups are performed during network
	// traversal.
	DefaultConcurrency = 10

	// DefaultResiliency is the suggested number of peers closest to a target
	// that must have responded in order for a given query path to complete in
	// Amino DHT. This helps ensure reliable results by requiring multiple
	// confirmations.
	DefaultResiliency = 3

	// DefaultProvideValidity is the default time that a Provider Record should
	// last on Amino DHT before it needs to be refreshed or removed. This value
	// is also known as Provider Record Expiration Interval.
	DefaultProvideValidity = 48 * time.Hour

	// DefaultProviderAddrTTL is the TTL to keep the multi addresses of
	// provider peers around. Those addresses are returned alongside provider.
	// After it expires, the returned records will require an extra lookup, to
	// find the multiaddress associated with the returned peer id.
	DefaultProviderAddrTTL = 24 * time.Hour
)

var (
	// Protocols is a slice containing all supported protocol IDs for Amino DHT.
	// Currently, it only includes the main ProtocolID, but it's defined as a slice
	// to allow for potential future protocol versions or variants.
	Protocols = []protocol.ID{ProtocolID}
)
