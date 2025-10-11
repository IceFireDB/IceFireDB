package provider

import (
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/amino"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider/keystore"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

const (
	// DefaultMaxReprovideDelay is the default maximum delay allowed when
	// reproviding a region. The interval between 2 reprovides of the same region
	// is at most ReprovideInterval+MaxReprovideDelay. This variable is necessary
	// since regions can grow and shrink depending on the network churn.
	DefaultMaxReprovideDelay = 1 * time.Hour

	// DefaultOfflineDelay is the default delay after which a disconnected node
	// is considered as Offline.
	DefaultOfflineDelay = 2 * time.Hour
	// DefaultConnectivityCheckOnlineInterval is the default minimum interval for
	// checking whether the node is still online. Such a check is performed when
	// a network operation fails, and the ConnectivityCheckOnlineInterval limits
	// how often such a check is performed.
	DefaultConnectivityCheckOnlineInterval = 1 * time.Minute
)

type config struct {
	replicationFactor int
	reprovideInterval time.Duration
	maxReprovideDelay time.Duration

	offlineDelay                    time.Duration
	connectivityCheckOnlineInterval time.Duration

	peerid peer.ID
	router KadClosestPeersRouter

	keystore keystore.Keystore

	msgSender      pb.MessageSender
	selfAddrs      func() []ma.Multiaddr
	addLocalRecord func(mh.Multihash) error

	maxWorkers               int
	dedicatedPeriodicWorkers int
	dedicatedBurstWorkers    int
	maxProvideConnsPerWorker int
}

type Option func(opt *config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		replicationFactor:               amino.DefaultBucketSize,
		reprovideInterval:               amino.DefaultReprovideInterval,
		maxReprovideDelay:               DefaultMaxReprovideDelay,
		offlineDelay:                    DefaultOfflineDelay,
		connectivityCheckOnlineInterval: DefaultConnectivityCheckOnlineInterval,

		maxWorkers:               4,
		dedicatedPeriodicWorkers: 2,
		dedicatedBurstWorkers:    1,
		maxProvideConnsPerWorker: 20,

		addLocalRecord: func(mh mh.Multihash) error { return nil },
	}

	// Apply options
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("reprovider dht option %d error: %s", i, err)
		}
	}

	// Validate config
	if len(cfg.peerid) == 0 {
		return config{}, errors.New("reprovider config: peer id is required")
	}
	if cfg.router == nil {
		return config{}, errors.New("reprovider config: router is required")
	}
	if cfg.msgSender == nil {
		return config{}, errors.New("reprovider config: message sender is required")
	}
	if cfg.selfAddrs == nil {
		return config{}, errors.New("reprovider config: self addrs func is required")
	}
	if cfg.dedicatedPeriodicWorkers+cfg.dedicatedBurstWorkers > cfg.maxWorkers {
		return config{}, errors.New("reprovider config: total dedicated workers exceed max workers")
	}
	return cfg, nil
}

// WithReplicationFactor sets the replication factor for provider records. It
// means that during provide and reprovide operations, each provider records is
// allocated to the ReplicationFactor closest peers in the DHT swarm.
func WithReplicationFactor(n int) Option {
	return func(cfg *config) error {
		if n <= 0 {
			return errors.New("reprovider config: replication factor must be a positive integer")
		}
		cfg.replicationFactor = n
		return nil
	}
}

// WithReprovideInterval sets the interval at which regions are reprovided.
func WithReprovideInterval(d time.Duration) Option {
	return func(cfg *config) error {
		if d <= 0 {
			return errors.New("reprovider config: reprovide interval must be greater than 0")
		}
		cfg.reprovideInterval = d
		return nil
	}
}

// WithMaxReprovideDelay sets the maximum delay allowed when reproviding a
// region. The interval between 2 reprovides of the same region is at most
// ReprovideInterval+MaxReprovideDelay.
//
// This parameter is necessary since regions can grow and shrink depending on
// the network churn.
func WithMaxReprovideDelay(d time.Duration) Option {
	return func(cfg *config) error {
		if d <= 0 {
			return errors.New("reprovider config: max reprovide delay must be greater than 0")
		}
		cfg.maxReprovideDelay = d
		return nil
	}
}

// WithOfflineDelay sets the delay after which a disconnected node is
// considered as offline. When a node cannot connect to peers, it is set to
// `Disconnected`, and after `OfflineDelay` it still cannot connect to peers,
// its state changes to `Offline`.
func WithOfflineDelay(d time.Duration) Option {
	return func(cfg *config) error {
		if d < 0 {
			return errors.New("reprovider config: offline delay must be non-negative")
		}
		cfg.offlineDelay = d
		return nil
	}
}

// WithConnectivityCheckOnlineInterval sets the minimal interval for checking
// whether the node is still online. Such a check is performed when a network
// operation fails, and the ConnectivityCheckOnlineInterval limits how often
// such a check is performed.
func WithConnectivityCheckOnlineInterval(d time.Duration) Option {
	return func(cfg *config) error {
		cfg.connectivityCheckOnlineInterval = d
		return nil
	}
}

// WithPeerID sets the peer ID of the node running the provider.
func WithPeerID(p peer.ID) Option {
	return func(cfg *config) error {
		cfg.peerid = p
		return nil
	}
}

// WithRouter sets the router used to find closest peers in the DHT.
func WithRouter(r KadClosestPeersRouter) Option {
	return func(cfg *config) error {
		cfg.router = r
		return nil
	}
}

// WithMessageSender sets the message sender used to send messages out to the
// DHT swarm.
func WithMessageSender(m pb.MessageSender) Option {
	return func(cfg *config) error {
		cfg.msgSender = m
		return nil
	}
}

// WithSelfAddrs sets the function that returns the self addresses of the node.
// These addresses are written in the provider records advertised by the node.
func WithSelfAddrs(f func() []ma.Multiaddr) Option {
	return func(cfg *config) error {
		cfg.selfAddrs = f
		return nil
	}
}

// WithAddLocalRecord sets the function that adds a provider record to the
// local provider record store.
func WithAddLocalRecord(f func(mh.Multihash) error) Option {
	return func(cfg *config) error {
		if f == nil {
			return errors.New("reprovider config: add local record function cannot be nil")
		}
		cfg.addLocalRecord = f
		return nil
	}
}

// WithMaxWorkers sets the maximum number of workers that can be used for
// provide and reprovide jobs. The job of a worker is to explore a region of
// the keyspace and (re)provide the keys matching the region to the closest
// peers.
//
// You can configure a number of workers dedicated to periodic jobs, and a
// number of workers dedicated to burst jobs. MaxWorkers should be greater or
// equal to DedicatedPeriodicWorkers+DedicatedBurstWorkers. The additional
// workers that aren't dedicated to specific jobs can be used for either job
// type where needed.
func WithMaxWorkers(n int) Option {
	return func(cfg *config) error {
		if n < 0 {
			return errors.New("reprovider config: max workers must be non-negative")
		}
		cfg.maxWorkers = n
		return nil
	}
}

// WithDedicatedPeriodicWorkers sets the number of workers dedicated to
// periodic region reprovides.
func WithDedicatedPeriodicWorkers(n int) Option {
	return func(cfg *config) error {
		if n < 0 {
			return errors.New("reprovider config: dedicated periodic workers must be non-negative")
		}
		cfg.dedicatedPeriodicWorkers = n
		return nil
	}
}

// WithDedicatedBurstWorkers sets the number of workers dedicated to burst
// operations. Burst operations consist in work that isn't scheduled
// beforehands, such as initial provides and catching up with reproviding after
// the node went offline for a while.
func WithDedicatedBurstWorkers(n int) Option {
	return func(cfg *config) error {
		if n < 0 {
			return errors.New("reprovider config: dedicated burst workers must be non-negative")
		}
		cfg.dedicatedBurstWorkers = n
		return nil
	}
}

// WithMaxProvideConnsPerWorker sets the maximum number of connections to
// distinct peers that can be opened by a single worker during a provide
// operation.
func WithMaxProvideConnsPerWorker(n int) Option {
	return func(cfg *config) error {
		if n <= 0 {
			return errors.New("reprovider config: max provide conns per worker must be greater than 0")
		}
		cfg.maxProvideConnsPerWorker = n
		return nil
	}
}

// WithKeystore defines the Keystore used to keep track of the keys that need
// to be reprovided.
func WithKeystore(ks keystore.Keystore) Option {
	return func(cfg *config) error {
		if ks == nil {
			return errors.New("reprovider config: multihash store cannot be nil")
		}
		cfg.keystore = ks
		return nil
	}
}
