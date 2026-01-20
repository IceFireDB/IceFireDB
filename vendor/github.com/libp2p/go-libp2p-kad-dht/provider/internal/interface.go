// Package internal defines the core provider interface for DHT content routing.
package internal

import (
	mh "github.com/multiformats/go-multihash"
)

// DefaultLoggerName is the default logger name for the DHT provider.
const DefaultLoggerName = "dht/provider"

type Provider interface {
	StartProviding(force bool, keys ...mh.Multihash) error
	StopProviding(keys ...mh.Multihash) error
	ProvideOnce(keys ...mh.Multihash) error
	Clear() int
	RefreshSchedule() error
	Close() error
}
