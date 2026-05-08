package keystore

import (
	"fmt"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/provider/internal"
)

type config struct {
	prefixBits int
	batchSize  int
	loggerName string
}

// Option for configuring a Keystore.
type Option func(*config) error

const (
	DefaultBatchSize  = 1 << 14
	DefaultPrefixBits = 16
	DefaultLoggerName = internal.DefaultLoggerName
)

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		prefixBits: DefaultPrefixBits,
		batchSize:  DefaultBatchSize,
		loggerName: DefaultLoggerName,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d error: %w", i, err)
		}
	}
	return cfg, nil
}

// WithPrefixBits sets how many bits from binary keys become individual path
// components in datastore keys. Higher values create deeper hierarchies but
// enable more granular prefix queries.
//
// Must be a multiple of 8 between 0 and 256 (inclusive) to align with byte
// boundaries.
func WithPrefixBits(prefixBits int) Option {
	return func(cfg *config) error {
		if prefixBits < 0 || prefixBits > 256 || prefixBits%8 != 0 {
			return fmt.Errorf("invalid prefix bits %d, must be a non-negative multiple of 8 less or equal to 256", prefixBits)
		}
		cfg.prefixBits = prefixBits
		return nil
	}
}

// WithBatchSize defines the maximal number of keys per batch when reading or
// writing to the datastore. It is typically used in Empty() and ResetCids().
func WithBatchSize(size int) Option {
	return func(cfg *config) error {
		if size <= 0 {
			return fmt.Errorf("invalid batch size %d", size)
		}
		cfg.batchSize = size
		return nil
	}
}

// WithLoggerName sets the logger name for the keystore.
//
// Note: We want to use the same logger as the `SweepingProvider` in order to
// keep the number of loggers to monitor low and consistent. `SweepingProvider`
// needs to accept custom logger names, because multiple instances can exist
// concurrently, and we want to distinguish the log outputs. Hence, we may need
// to pass the logger name from outside.
func WithLoggerName(name string) Option {
	return func(cfg *config) error {
		if len(name) == 0 {
			return fmt.Errorf("logger name cannot be empty")
		}
		cfg.loggerName = name
		return nil
	}
}

// resettableKeystoreConfig holds resettable-specific fields and the base
// keystore config.
type resettableKeystoreConfig struct {
	config // base keystore config (prefixBits, batchSize, loggerName)

	createDs  func(string) (ds.Batching, error) // factory to create per-namespace datastores
	destroyDs func(string) error                // destroys a per-namespace datastore on disk
}

// ResettableKeystoreOption configures a ResettableKeystore.
type ResettableKeystoreOption func(*resettableKeystoreConfig) error

// KeystoreOption wraps base keystore Options for use with NewResettableKeystore.
func KeystoreOption(opts ...Option) ResettableKeystoreOption {
	return func(c *resettableKeystoreConfig) error {
		for _, opt := range opts {
			if err := opt(&c.config); err != nil {
				return err
			}
		}
		return nil
	}
}

// WithDatastoreFactory configures the ResettableKeystore to use independent
// datastores per namespace. create produces a new datastore for a given
// namespace suffix ("0" or "1"), and destroy removes it entirely from disk
// (e.g. os.RemoveAll). The constructor calls create for the active data
// namespace only. The alternate datastore is created on demand when ResetCids
// is called and destroyed after the reset completes. This enables full disk
// reclamation because the old datastore is deleted from disk rather than
// emptied key-by-key.
//
// The meta datastore (for the active-namespace marker) is the positional
// datastore argument passed to NewResettableKeystore.
func WithDatastoreFactory(create func(string) (ds.Batching, error), destroy func(string) error) ResettableKeystoreOption {
	return func(c *resettableKeystoreConfig) error {
		if create == nil {
			return fmt.Errorf("create function cannot be nil")
		}
		if destroy == nil {
			return fmt.Errorf("destroy function cannot be nil")
		}
		c.createDs = create
		c.destroyDs = destroy
		return nil
	}
}

// getResettableOpts applies ResettableKeystoreOptions and returns the
// resulting configuration.
func getResettableOpts(opts []ResettableKeystoreOption) (resettableKeystoreConfig, error) {
	cfg := resettableKeystoreConfig{
		config: config{
			prefixBits: DefaultPrefixBits,
			batchSize:  DefaultBatchSize,
			loggerName: DefaultLoggerName,
		},
	}
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return resettableKeystoreConfig{}, fmt.Errorf("resettable option %d error: %w", i, err)
		}
	}
	return cfg, nil
}
