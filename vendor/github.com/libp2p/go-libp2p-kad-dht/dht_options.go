package dht

import (
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/amino"
	dhtcfg "github.com/libp2p/go-libp2p-kad-dht/internal/config"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	ds "github.com/ipfs/go-datastore"
	ma "github.com/multiformats/go-multiaddr"
)

// ModeOpt describes what mode the dht should operate in
type ModeOpt = dhtcfg.ModeOpt

const (
	// ModeAuto utilizes EvtLocalReachabilityChanged events sent over the event bus to dynamically switch the DHT
	// between Client and Server modes based on network conditions
	ModeAuto ModeOpt = iota
	// ModeClient operates the DHT as a client only, it cannot respond to incoming queries
	ModeClient
	// ModeServer operates the DHT as a server, it can both send and respond to queries
	ModeServer
	// ModeAutoServer operates in the same way as ModeAuto, but acts as a server when reachability is unknown
	ModeAutoServer
)

// DefaultPrefix is the application specific prefix attached to all DHT protocols by default.
const DefaultPrefix protocol.ID = amino.ProtocolPrefix

type Option = dhtcfg.Option

// ProviderStore sets the provider storage manager.
func ProviderStore(ps providers.ProviderStore) Option {
	return func(c *dhtcfg.Config) error {
		c.ProviderStore = ps
		return nil
	}
}

// RoutingTableLatencyTolerance sets the maximum acceptable latency for peers
// in the routing table's cluster.
func RoutingTableLatencyTolerance(latency time.Duration) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.LatencyTolerance = latency
		return nil
	}
}

// RoutingTableRefreshQueryTimeout sets the timeout for routing table refresh
// queries.
func RoutingTableRefreshQueryTimeout(timeout time.Duration) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.RefreshQueryTimeout = timeout
		return nil
	}
}

// RoutingTableRefreshPeriod sets the period for refreshing buckets in the
// routing table. The DHT will refresh buckets every period by:
//
//  1. First searching for nearby peers to figure out how many buckets we should try to fill.
//  1. Then searching for a random key in each bucket that hasn't been queried in
//     the last refresh period.
func RoutingTableRefreshPeriod(period time.Duration) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.RefreshInterval = period
		return nil
	}
}

// Datastore configures the DHT to use the specified datastore.
//
// Defaults to an in-memory (temporary) map.
func Datastore(ds ds.Batching) Option {
	return func(c *dhtcfg.Config) error {
		c.Datastore = ds
		return nil
	}
}

// Mode configures which mode the DHT operates in (Client, Server, Auto).
//
// Defaults to ModeAuto.
func Mode(m ModeOpt) Option {
	return func(c *dhtcfg.Config) error {
		c.Mode = m
		return nil
	}
}

// Validator configures the DHT to use the specified validator.
//
// Defaults to a namespaced validator that can validate both public key (under the "pk"
// namespace) and IPNS records (under the "ipns" namespace). Setting the validator
// implies that the user wants to control the validators and therefore the default
// public key and IPNS validators will not be added.
func Validator(v record.Validator) Option {
	return func(c *dhtcfg.Config) error {
		c.Validator = v
		c.ValidatorChanged = true
		return nil
	}
}

// NamespacedValidator adds a validator namespaced under `ns`. This option fails
// if the DHT is not using a `record.NamespacedValidator` as its validator (it
// uses one by default but this can be overridden with the `Validator` option).
// Adding a namespaced validator without changing the `Validator` will result in
// adding a new validator in addition to the default public key and IPNS validators.
// The "pk" and "ipns" namespaces cannot be overridden here unless a new `Validator`
// has been set first.
//
// Example: Given a validator registered as `NamespacedValidator("ipns",
// myValidator)`, all records with keys starting with `/ipns/` will be validated
// with `myValidator`.
func NamespacedValidator(ns string, v record.Validator) Option {
	return func(c *dhtcfg.Config) error {
		nsval, ok := c.Validator.(record.NamespacedValidator)
		if !ok {
			return fmt.Errorf("can only add namespaced validators to a NamespacedValidator")
		}
		nsval[ns] = v
		return nil
	}
}

// ProtocolPrefix sets an application specific prefix to be attached to all DHT protocols. For example,
// /myapp/kad/1.0.0 instead of /ipfs/kad/1.0.0. Prefix should be of the form /myapp.
//
// Defaults to amino.ProtocolPrefix
func ProtocolPrefix(prefix protocol.ID) Option {
	return func(c *dhtcfg.Config) error {
		c.ProtocolPrefix = prefix
		return nil
	}
}

// ProtocolExtension adds an application specific protocol to the DHT protocol. For example,
// /ipfs/lan/kad/1.0.0 instead of /ipfs/kad/1.0.0. extension should be of the form /lan.
func ProtocolExtension(ext protocol.ID) Option {
	return func(c *dhtcfg.Config) error {
		c.ProtocolPrefix += ext
		return nil
	}
}

// V1ProtocolOverride overrides the protocolID used for /kad/1.0.0 with another. This is an
// advanced feature, and should only be used to handle legacy networks that have not been
// using protocolIDs of the form /app/kad/1.0.0.
//
// This option will override and ignore the ProtocolPrefix and ProtocolExtension options
func V1ProtocolOverride(proto protocol.ID) Option {
	return func(c *dhtcfg.Config) error {
		c.V1ProtocolOverride = proto
		return nil
	}
}

// BucketSize configures the bucket size (k in the Kademlia paper) of the routing table.
//
// The default value is amino.DefaultBucketSize
func BucketSize(bucketSize int) Option {
	return func(c *dhtcfg.Config) error {
		c.BucketSize = bucketSize
		return nil
	}
}

// Concurrency configures the number of concurrent requests (alpha in the Kademlia paper) for a given query path.
//
// The default value is amino.DefaultConcurrency
func Concurrency(alpha int) Option {
	return func(c *dhtcfg.Config) error {
		c.Concurrency = alpha
		return nil
	}
}

// Resiliency configures the number of peers closest to a target that must have responded in order for a given query
// path to complete.
//
// The default value is amino.DefaultResiliency
func Resiliency(beta int) Option {
	return func(c *dhtcfg.Config) error {
		c.Resiliency = beta
		return nil
	}
}

// LookupInterval configures maximal number of go routines that can be used to
// perform a lookup check operation, before adding a new node to the routing table.
func LookupCheckConcurrency(n int) Option {
	return func(c *dhtcfg.Config) error {
		c.LookupCheckConcurrency = n
		return nil
	}
}

// MaxRecordAge specifies the maximum time that any node will hold onto a record ("PutValue record")
// from the time its received. This does not apply to any other forms of validity that
// the record may contain.
// For example, a record may contain an ipns entry with an EOL saying its valid
// until the year 2020 (a great time in the future). For that record to stick around
// it must be rebroadcasted more frequently than once every 'MaxRecordAge'
func MaxRecordAge(maxAge time.Duration) Option {
	return func(c *dhtcfg.Config) error {
		c.MaxRecordAge = maxAge
		return nil
	}
}

// DisableAutoRefresh completely disables 'auto-refresh' on the DHT routing
// table. This means that we will neither refresh the routing table periodically
// nor when the routing table size goes below the minimum threshold.
func DisableAutoRefresh() Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.AutoRefresh = false
		return nil
	}
}

// DisableProviders disables storing and retrieving provider records.
//
// Defaults to enabled.
//
// WARNING: do not change this unless you're using a forked DHT (i.e., a private
// network and/or distinct DHT protocols with the `Protocols` option).
func DisableProviders() Option {
	return func(c *dhtcfg.Config) error {
		c.EnableProviders = false
		return nil
	}
}

// DisableValues disables storing and retrieving value records (including
// public keys).
//
// Defaults to enabled.
//
// WARNING: do not change this unless you're using a forked DHT (i.e., a private
// network and/or distinct DHT protocols with the `Protocols` option).
func DisableValues() Option {
	return func(c *dhtcfg.Config) error {
		c.EnableValues = false
		return nil
	}
}

// QueryFilter sets a function that approves which peers may be dialed in a query
func QueryFilter(filter QueryFilterFunc) Option {
	return func(c *dhtcfg.Config) error {
		c.QueryPeerFilter = filter
		return nil
	}
}

// RoutingTableFilter sets a function that approves which peers may be added to the routing table. The host should
// already have at least one connection to the peer under consideration.
func RoutingTableFilter(filter RouteTableFilterFunc) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.PeerFilter = filter
		return nil
	}
}

// BootstrapPeers configures the bootstrapping nodes that we will connect to to seed
// and refresh our Routing Table if it becomes empty.
func BootstrapPeers(bootstrappers ...peer.AddrInfo) Option {
	return func(c *dhtcfg.Config) error {
		c.BootstrapPeers = func() []peer.AddrInfo {
			return bootstrappers
		}
		return nil
	}
}

// BootstrapPeersFunc configures the function that returns the bootstrapping nodes that we will
// connect to to seed and refresh our Routing Table if it becomes empty.
func BootstrapPeersFunc(getBootstrapPeers func() []peer.AddrInfo) Option {
	return func(c *dhtcfg.Config) error {
		c.BootstrapPeers = getBootstrapPeers
		return nil
	}
}

// RoutingTablePeerDiversityFilter configures the implementation of the `PeerIPGroupFilter` that will be used
// to construct the diversity filter for the Routing Table.
// Please see the docs for `peerdiversity.PeerIPGroupFilter` AND `peerdiversity.Filter` for more details.
func RoutingTablePeerDiversityFilter(pg peerdiversity.PeerIPGroupFilter) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.DiversityFilter = pg
		return nil
	}
}

// disableFixLowPeersRoutine disables the "fixLowPeers" routine in the DHT.
// This is ONLY for tests.
func disableFixLowPeersRoutine(t *testing.T) Option {
	return func(c *dhtcfg.Config) error {
		c.DisableFixLowPeers = true
		return nil
	}
}

// forceAddressUpdateProcessing forces the DHT to handle changes to the hosts addresses.
// This occurs even when AutoRefresh has been disabled.
// This is ONLY for tests.
func forceAddressUpdateProcessing(t *testing.T) Option {
	return func(c *dhtcfg.Config) error {
		c.TestAddressUpdateProcessing = true
		return nil
	}
}

// EnableOptimisticProvide enables an optimization that skips the last hops of the provide process.
// This works by using the network size estimator (which uses the keyspace density of queries)
// to optimistically send ADD_PROVIDER requests when we most likely have found the last hop.
// It will also run some ADD_PROVIDER requests asynchronously in the background after returning,
// this allows to optimistically return earlier if some threshold number of RPCs have succeeded.
// The number of background/in-flight queries can be configured with the OptimisticProvideJobsPoolSize
// option.
//
// EXPERIMENTAL: This is an experimental option and might be removed in the future. Use at your own risk.
func EnableOptimisticProvide() Option {
	return func(c *dhtcfg.Config) error {
		c.EnableOptimisticProvide = true
		return nil
	}
}

// OptimisticProvideJobsPoolSize allows to configure the asynchronicity limit for in-flight ADD_PROVIDER RPCs.
// It makes sense to set it to a multiple of optProvReturnRatio * BucketSize. Check the description of
// EnableOptimisticProvide for more details.
//
// EXPERIMENTAL: This is an experimental option and might be removed in the future. Use at your own risk.
func OptimisticProvideJobsPoolSize(size int) Option {
	return func(c *dhtcfg.Config) error {
		c.OptimisticProvideJobsPoolSize = size
		return nil
	}
}

// AddressFilter allows to configure the address filtering function.
// This function is run before addresses are added to the peerstore.
// It is most useful to avoid adding localhost / local addresses.
func AddressFilter(f func([]ma.Multiaddr) []ma.Multiaddr) Option {
	return func(c *dhtcfg.Config) error {
		c.AddressFilter = f
		return nil
	}
}

// WithCustomMessageSender configures the pb.MessageSender of the IpfsDHT to use the
// custom implementation of the pb.MessageSender
func WithCustomMessageSender(messageSenderBuilder func(h host.Host, protos []protocol.ID) pb.MessageSenderWithDisconnect) Option {
	return func(c *dhtcfg.Config) error {
		c.MsgSenderBuilder = messageSenderBuilder
		return nil
	}
}
