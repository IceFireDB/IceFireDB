package internal

import (
	mh "github.com/multiformats/go-multihash"
)

type Provider interface {
	StartProviding(force bool, keys ...mh.Multihash) error
	StopProviding(keys ...mh.Multihash) error
	ProvideOnce(keys ...mh.Multihash) error
	Clear() int
	RefreshSchedule() error
	Close() error
}
