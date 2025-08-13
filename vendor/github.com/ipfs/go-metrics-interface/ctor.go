package metrics

import (
	"context"
	"errors"
)

var ErrImplemented = errors.New("there is implemenation already injected")

var ctorImpl InternalNew = nil

// New returns a Creator. Name is a dot separated path which must be unique.
// Examples:
// ipfs.blockstore.bloomcache.bloom.miss.total
// ipfs.routing.dht.notresuingstream.total
//
// Both arguemnts are mandatory.
func New(name, helptext string) Creator {
	if ctorImpl == nil {
		return &noop{}
	} else {
		return ctorImpl(name, helptext)
	}
}

// NewCtx is like New but obtains the metric scope from the given
// context.
func NewCtx(ctx context.Context, name, helptext string) Creator {
	return New(CtxGetScope(ctx)+"."+name, helptext)
}

type InternalNew func(string, string) Creator

func InjectImpl(newimpl InternalNew) error {
	if ctorImpl != nil {
		return ErrImplemented
	} else {
		ctorImpl = newimpl
		return nil
	}
}

func Active() bool {
	return ctorImpl != nil
}
