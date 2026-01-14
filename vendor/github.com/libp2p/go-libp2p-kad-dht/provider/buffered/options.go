// Package buffered provides a buffered provider implementation that queues operations
// and processes them in batches for improved performance.
package buffered

import (
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/provider"
)

const (
	// DefaultDsName is the default datastore namespace for the buffered provider.
	DefaultDsName = "bprov" // for buffered provider
	// DefaultBatchSize is the default number of operations to process in a single batch.
	DefaultBatchSize = 1 << 10
	// DefaultIdleWriteTime is the default duration to wait before flushing pending operations.
	DefaultIdleWriteTime = time.Minute
	// DefaultLoggerName is the default logger name for the buffered provider. It
	// is recommended to use the same name as the underlying provider.
	DefaultLoggerName = provider.DefaultLoggerName
)

// config contains all options for the buffered provider.
type config struct {
	dsName        string
	loggerName    string
	batchSize     int
	idleWriteTime time.Duration
}

// Option is a function that configures the buffered provider.
type Option func(*config)

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) config {
	cfg := config{
		dsName:        DefaultDsName,
		loggerName:    DefaultLoggerName,
		batchSize:     DefaultBatchSize,
		idleWriteTime: DefaultIdleWriteTime,
	}

	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// WithDsName sets the datastore namespace for the buffered provider.
// If name is empty, the option is ignored.
func WithDsName(name string) Option {
	return func(c *config) {
		if len(name) > 0 {
			c.dsName = name
		}
	}
}

// WithLoggerName sets the go-log logger name for the buffered provider.
func WithLoggerName(name string) Option {
	return func(c *config) {
		if len(name) > 0 {
			c.loggerName = name
		}
	}
}

// WithBatchSize sets the number of operations to process in a single batch.
// If n is zero or negative, the option is ignored.
func WithBatchSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithIdleWriteTime sets the duration to wait before flushing pending operations.
func WithIdleWriteTime(d time.Duration) Option {
	return func(c *config) {
		c.idleWriteTime = d
	}
}
