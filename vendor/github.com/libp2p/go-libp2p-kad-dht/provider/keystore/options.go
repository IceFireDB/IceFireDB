package keystore

import (
	"fmt"

	"github.com/libp2p/go-libp2p-kad-dht/provider/internal"
)

type config struct {
	path       string
	prefixBits int
	batchSize  int
	loggerName string
}

// Option for configuring a Keystore.
type Option func(*config) error

const (
	DefaultPath       = "keystore"
	DefaultBatchSize  = 1 << 14
	DefaultPrefixBits = 16
	DefaultLoggerName = internal.DefaultLoggerName
)

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		path:       DefaultPath,
		prefixBits: DefaultPrefixBits,
		batchSize:  DefaultBatchSize,
		loggerName: DefaultLoggerName,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithDatastorePath sets the datastore prefix under which multihashes are
// stored.
func WithDatastorePath(path string) Option {
	return func(cfg *config) error {
		if path == "" {
			return fmt.Errorf("datastore name cannot be empty")
		}
		cfg.path = path
		return nil
	}
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
