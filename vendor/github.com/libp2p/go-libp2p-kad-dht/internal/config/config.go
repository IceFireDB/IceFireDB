package config

import (
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipns"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/internal/net"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

// DefaultPrefix is the application specific prefix attached to all DHT protocols by default.
const DefaultPrefix protocol.ID = amino.ProtocolPrefix

// ModeOpt describes what mode the dht should operate in
type ModeOpt int

// QueryFilterFunc is a filter applied when considering peers to dial when querying
type QueryFilterFunc func(dht interface{}, ai peer.AddrInfo) bool

// RouteTableFilterFunc is a filter applied when considering connections to keep in
// the local route table.
type RouteTableFilterFunc func(dht interface{}, p peer.ID) bool

// Config is a structure containing all the options that can be used when constructing a DHT.
type Config struct {
	Datastore              ds.Batching
	Validator              record.Validator
	ValidatorChanged       bool // if true implies that the validator has been changed and that Defaults should not be used
	Mode                   ModeOpt
	ProtocolPrefix         protocol.ID
	V1ProtocolOverride     protocol.ID
	BucketSize             int
	Concurrency            int
	Resiliency             int
	MaxRecordAge           time.Duration
	EnableProviders        bool
	EnableValues           bool
	ProviderStore          providers.ProviderStore
	QueryPeerFilter        QueryFilterFunc
	LookupCheckConcurrency int
	MsgSenderBuilder       func(h host.Host, protos []protocol.ID) pb.MessageSenderWithDisconnect

	RoutingTable struct {
		RefreshQueryTimeout time.Duration
		RefreshInterval     time.Duration
		AutoRefresh         bool
		LatencyTolerance    time.Duration
		CheckInterval       time.Duration
		PeerFilter          RouteTableFilterFunc
		DiversityFilter     peerdiversity.PeerIPGroupFilter
	}

	BootstrapPeers func() []peer.AddrInfo
	AddressFilter  func([]ma.Multiaddr) []ma.Multiaddr

	// test specific Config options
	DisableFixLowPeers          bool
	TestAddressUpdateProcessing bool

	EnableOptimisticProvide       bool
	OptimisticProvideJobsPoolSize int
}

func EmptyQueryFilter(_ interface{}, ai peer.AddrInfo) bool { return true }
func EmptyRTFilter(_ interface{}, p peer.ID) bool           { return true }

// Apply applies the given options to this Option
func (c *Config) Apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("dht option %d failed: %s", i, err)
		}
	}
	return nil
}

// ApplyFallbacks sets default values that could not be applied during config creation since they are dependent
// on other configuration parameters (e.g. optA is by default 2x optB) and/or on the Host
func (c *Config) ApplyFallbacks(h host.Host) error {
	if !c.ValidatorChanged {
		nsval, ok := c.Validator.(record.NamespacedValidator)
		if ok {
			if _, pkFound := nsval["pk"]; !pkFound {
				nsval["pk"] = record.PublicKeyValidator{}
			}
			if _, ipnsFound := nsval["ipns"]; !ipnsFound {
				nsval["ipns"] = ipns.Validator{KeyBook: h.Peerstore()}
			}
		} else {
			return fmt.Errorf("the default Validator was changed without being marked as changed")
		}
	}
	return nil
}

// Option DHT option type.
type Option func(*Config) error

// Defaults are the default DHT options. This option will be automatically
// prepended to any options you pass to the DHT constructor.
var Defaults = func(o *Config) error {
	o.Validator = record.NamespacedValidator{}
	o.Datastore = dssync.MutexWrap(ds.NewMapDatastore())
	o.ProtocolPrefix = DefaultPrefix
	o.EnableProviders = true
	o.EnableValues = true
	o.QueryPeerFilter = EmptyQueryFilter
	o.MsgSenderBuilder = net.NewMessageSenderImpl

	o.RoutingTable.LatencyTolerance = 10 * time.Second
	o.RoutingTable.RefreshQueryTimeout = 10 * time.Second
	o.RoutingTable.RefreshInterval = 10 * time.Minute
	o.RoutingTable.AutoRefresh = true
	o.RoutingTable.PeerFilter = EmptyRTFilter

	o.MaxRecordAge = providers.ProvideValidity

	o.BucketSize = amino.DefaultBucketSize
	o.Concurrency = amino.DefaultConcurrency
	o.Resiliency = amino.DefaultResiliency
	o.LookupCheckConcurrency = 256

	// MAGIC: It makes sense to set it to a multiple of OptProvReturnRatio * BucketSize. We chose a multiple of 4.
	o.OptimisticProvideJobsPoolSize = 60

	return nil
}

func (c *Config) Validate() error {
	// Configuration is validated and enforced only if prefix matches Amino DHT
	if c.ProtocolPrefix != DefaultPrefix {
		return nil
	}
	if c.BucketSize != amino.DefaultBucketSize {
		return fmt.Errorf("protocol prefix %s must use bucket size %d", DefaultPrefix, amino.DefaultBucketSize)
	}
	if !c.EnableProviders {
		return fmt.Errorf("protocol prefix %s must have providers enabled", DefaultPrefix)
	}
	if !c.EnableValues {
		return fmt.Errorf("protocol prefix %s must have values enabled", DefaultPrefix)
	}

	nsval, isNSVal := c.Validator.(record.NamespacedValidator)
	if !isNSVal {
		return fmt.Errorf("protocol prefix %s must use a namespaced Validator", DefaultPrefix)
	}

	if len(nsval) != 2 {
		return fmt.Errorf("protocol prefix %s must have exactly two namespaced validators - /pk and /ipns", DefaultPrefix)
	}

	if pkVal, pkValFound := nsval["pk"]; !pkValFound {
		return fmt.Errorf("protocol prefix %s must support the /pk namespaced Validator", DefaultPrefix)
	} else if _, ok := pkVal.(record.PublicKeyValidator); !ok {
		return fmt.Errorf("protocol prefix %s must use the record.PublicKeyValidator for the /pk namespace", DefaultPrefix)
	}

	if ipnsVal, ipnsValFound := nsval["ipns"]; !ipnsValFound {
		return fmt.Errorf("protocol prefix %s must support the /ipns namespaced Validator", DefaultPrefix)
	} else if _, ok := ipnsVal.(ipns.Validator); !ok {
		return fmt.Errorf("protocol prefix %s must use ipns.Validator for the /ipns namespace", DefaultPrefix)
	}
	return nil
}
