package pebbleds

import (
	"github.com/cockroachdb/pebble/v2"
)

type config struct {
	cacheSize  int64
	db         *pebble.DB
	pebbleOpts *pebble.Options
}

type Option func(*config)

func getOpts(options []Option) config {
	var cfg config
	for _, opt := range options {
		if opt == nil {
			continue
		}
		opt(&cfg)
	}
	return cfg
}

// WithCacheSize configures the size of pebble's shared block cache. A value of
// 0 (the default) uses the default cache size.
func WithCacheSize(size int64) Option {
	return func(c *config) {
		c.cacheSize = size
	}
}

// WithPebbleDB is used to configure the Datastore with a custom DB.
func WithPebbleDB(db *pebble.DB) Option {
	return func(c *config) {
		c.db = db
	}
}

// WithPebbleOpts sets any/all configurable values for pebble. If not set, the
// default configuration values are used. Any unspecified value in opts is
// replaced by the default value.
func WithPebbleOpts(opts *pebble.Options) Option {
	return func(c *config) {
		c.pebbleOpts = opts
	}
}
