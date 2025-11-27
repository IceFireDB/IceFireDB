// Package dual provides a SweepingProvider for dual DHT setups (LAN and WAN).
package dual

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	"github.com/libp2p/go-libp2p-kad-dht/provider"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal"
	"github.com/libp2p/go-libp2p-kad-dht/provider/keystore"
	mh "github.com/multiformats/go-multihash"
)

var _ internal.Provider = (*SweepingProvider)(nil)

// SweepingProvider manages provides and reprovides for both DHT swarms (LAN
// and WAN) in the dual DHT setup.
type SweepingProvider struct {
	dht      *dual.DHT
	LAN      *provider.SweepingProvider
	WAN      *provider.SweepingProvider
	keystore keystore.Keystore

	cleanupFuncs []func() error
}

// New creates a new SweepingProvider that manages provides and reprovides for
// both DHT swarms (LAN and WAN) in a dual DHT setup.
func New(d *dual.DHT, opts ...Option) (*SweepingProvider, error) {
	if d == nil || d.LAN == nil || d.WAN == nil {
		return nil, errors.New("cannot create sweeping provider for nil dual DHT")
	}

	cfg, err := getOpts(opts, d)
	if err != nil {
		return nil, err
	}
	var cleanupFuncs []func() error
	if cfg.keystore == nil {
		ds := datastore.NewMapDatastore()
		cfg.keystore, err = keystore.NewKeystore(ds)
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("couldn't create a keystore: %w", err)
		}
		cleanupFuncs = []func() error{ds.Close, cfg.keystore.Close}
	}

	sweepingProviders := make([]*provider.SweepingProvider, 2)
	for i, dht := range []*dht.IpfsDHT{d.LAN, d.WAN} {
		if dht == nil {
			continue
		}
		dhtOpts := []provider.Option{
			provider.WithHost(dht.Host()),
			provider.WithReplicationFactor(dht.BucketSize()),
			provider.WithSelfAddrs(dht.FilteredAddrs),
			provider.WithRouter(dht),
			provider.WithAddLocalRecord(func(h mh.Multihash) error {
				return dht.Provide(dht.Context(), cid.NewCidV1(cid.Raw, h), false)
			}),
			provider.WithResumeCycle(cfg.resumeCycle[i]),
			provider.WithMessageSender(cfg.msgSenders[i]),
			provider.WithReprovideInterval(cfg.reprovideInterval[i]),
			provider.WithMaxReprovideDelay(cfg.maxReprovideDelay[i]),
			provider.WithOfflineDelay(cfg.offlineDelay[i]),
			provider.WithConnectivityCheckOnlineInterval(cfg.connectivityCheckOnlineInterval[i]),
			provider.WithMaxWorkers(cfg.maxWorkers[i]),
			provider.WithDedicatedPeriodicWorkers(cfg.dedicatedPeriodicWorkers[i]),
			provider.WithDedicatedBurstWorkers(cfg.dedicatedBurstWorkers[i]),
			provider.WithMaxProvideConnsPerWorker(cfg.maxProvideConnsPerWorker[i]),
			provider.WithLoggerName(cfg.loggerNames[i]),
			provider.WithDhtType(descriptors[i]),
		}
		if cfg.keystore != nil {
			dhtOpts = append(dhtOpts, provider.WithKeystore(cfg.keystore))
		}
		if cfg.datastores[i] != nil {
			dhtOpts = append(dhtOpts, provider.WithDatastore(cfg.datastores[i]))
		}
		sweepingProviders[i], err = provider.New(dhtOpts...)
		if err != nil {
			return nil, err
		}
	}

	return &SweepingProvider{
		dht:          d,
		LAN:          sweepingProviders[0],
		WAN:          sweepingProviders[1],
		keystore:     cfg.keystore,
		cleanupFuncs: cleanupFuncs,
	}, nil
}

// runOnBoth runs the provided function on both the LAN and WAN providers in
// parallel and waits for both to complete.
func (s *SweepingProvider) runOnBoth(f func(*provider.SweepingProvider) error) error {
	errCh := make(chan error, 1)
	go func() {
		err := f(s.LAN)
		if err != nil {
			err = fmt.Errorf("LAN provider: %w", err)
		}
		errCh <- err
	}()
	err := f(s.WAN)
	if err != nil {
		err = fmt.Errorf("WAN provider: %w", err)
	}
	lanErr := <-errCh
	return errors.Join(lanErr, err)
}

// Close stops both DHT providers and releases associated resources.
func (s *SweepingProvider) Close() error {
	err := s.runOnBoth(func(p *provider.SweepingProvider) error {
		return p.Close()
	})

	if s.cleanupFuncs != nil {
		// Cleanup keystore and datastore if we created them
		var errs []error
		for i := len(s.cleanupFuncs) - 1; i >= 0; i-- { // LIFO: last-added is cleaned up first
			if f := s.cleanupFuncs[i]; f != nil {
				if err := f(); err != nil {
					errs = append(errs, err)
				}
			}
		}
		if len(errs) > 0 {
			err = errors.Join(append(errs, err)...)
		}
	}
	return err
}

// ProvideOnce sends provider records for the specified keys to both DHT swarms
// only once. It does not automatically reprovide those keys afterward.
//
// Add the supplied multihashes to the provide queues, and return right after.
// The provide operation happens asynchronously.
//
// Returns an error if the keys couldn't be added to the provide queue. This
// can happen if the provider is closed or if the node is currently Offline
// (either never bootstrapped, or disconnected since more than `OfflineDelay`).
// The schedule and provide queue depend on the network size, hence recent
// network connectivity is essential.
func (s *SweepingProvider) ProvideOnce(keys ...mh.Multihash) error {
	return s.runOnBoth(func(p *provider.SweepingProvider) error {
		return p.ProvideOnce(keys...)
	})
}

// StartProviding ensures keys are periodically advertised to both DHT swarms.
//
// If the `keys` aren't currently being reprovided, they are added to the
// queue to be provided to the DHT swarm as soon as possible, and scheduled
// to be reprovided periodically. If `force` is set to true, all keys are
// provided to the DHT swarm, regardless of whether they were already being
// reprovided in the past. `keys` keep being reprovided until `StopProviding`
// is called.
//
// This operation is asynchronous, it returns as soon as the `keys` are added
// to the provide queue, and provides happens asynchronously.
//
// Returns an error if the keys couldn't be added to the provide queue. This
// can happen if the provider is closed or if the node is currently Offline
// (either never bootstrapped, or disconnected since more than `OfflineDelay`).
// The schedule and provide queue depend on the network size, hence recent
// network connectivity is essential.
func (s *SweepingProvider) StartProviding(force bool, keys ...mh.Multihash) error {
	ctx := context.Background()
	newKeys, err := s.keystore.Put(ctx, keys...)
	if err != nil {
		return fmt.Errorf("failed to store multihashes: %w", err)
	}

	s.runOnBoth(func(p *provider.SweepingProvider) error {
		return p.AddToSchedule(newKeys...)
	})

	if !force {
		keys = newKeys
	}

	return s.ProvideOnce(keys...)
}

// StopProviding stops reproviding the given keys to both DHT swarms. The node
// stops being referred as a provider when the provider records in the DHT
// swarms expire.
//
// Remove the `keys` from the schedule and return immediately. Valid records
// can remain in the DHT swarms up to the provider record TTL after calling
// `StopProviding`.
func (s *SweepingProvider) StopProviding(keys ...mh.Multihash) error {
	err := s.keystore.Delete(context.Background(), keys...)
	if err != nil {
		return fmt.Errorf("failed to stop providing keys: %w", err)
	}
	return nil
}

// Clear clears the all the keys from the provide queues of both DHTs and
// returns the number of keys that were cleared (sum of both queues).
//
// The keys are not deleted from the keystore, so they will continue to be
// reprovided as scheduled.
func (s *SweepingProvider) Clear() int {
	return s.LAN.Clear() + s.WAN.Clear()
}

// RefreshSchedule scans the Keystore for any keys that are not currently
// scheduled for reproviding. If such keys are found, it schedules their
// associated keyspace region to be reprovided for both DHT providers.
//
// This function doesn't remove prefixes that have no keys from the schedule.
// This is done automatically during the reprovide operation if a region has no
// keys.
//
// Returns an error if the provider is closed or if the node is currently
// Offline (either never bootstrapped, or disconnected since more than
// `OfflineDelay`). The schedule depends on the network size, hence recent
// network connectivity is essential.
func (s *SweepingProvider) RefreshSchedule() error {
	return s.runOnBoth(func(p *provider.SweepingProvider) error {
		return p.RefreshSchedule()
	})
}
