// Package provider provides interfaces and tooling for (Re)providers.
//
// This includes methods to provide streams of CIDs (i.e. from pinned
// merkledags, from blockstores, from single dags etc.). These methods can be
// used for other purposes, but are usually fed to the Reprovider to announce
// CIDs.
package provider

import (
	"context"

	"github.com/gammazero/chanqueue"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	mh "github.com/multiformats/go-multihash"
)

// Provider announces blocks to the network
type Provider interface {
	// Provide takes a cid and makes an attempt to announce it to the network
	Provide(context.Context, cid.Cid, bool) error
}

// MultihashProvider is the interface implementing StartProviding.
type MultihashProvider interface {
	// StartProviding announces blocks to the network, batching multiple keys for efficiency.
	// The force parameter, when true, forces re-providing even if the keys were recently provided.
	// When false, the implementation may skip keys that were recently announced based on internal
	// rate limiting or caching strategies.
	StartProviding(force bool, keys ...mh.Multihash) error
}

// Reprovider reannounces blocks to the network
type Reprovider interface {
	// Reprovide starts a new reprovide if one isn't running already.
	Reprovide(context.Context) error
}

// System defines the interface for interacting with the value
// provider system
type System interface {
	// Clear removes all entries from the provide queue. Returns the number of
	// CIDs removed from the queue.
	Clear() int
	Close() error
	Stat() (ReproviderStats, error)
	SetKeyProvider(kp KeyChanFunc)
	Provider
	Reprovider
}

// KeyChanFunc is function streaming CIDs to pass to content routing
type KeyChanFunc func(context.Context) (<-chan cid.Cid, error)

// NewBufferedProvider returns a KeyChanFunc supplying keys from a given
// KeyChanFunction, but buffering keys in memory if we can read them faster
// they are consumed.  This allows the underlying KeyChanFunc to finish
// listing pins as soon as possible releasing any resources, locks, at the
// expense of memory usage.
func NewBufferedProvider(pinsF KeyChanFunc) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		pins, err := pinsF(ctx)
		if err != nil {
			return nil, err
		}

		queue := chanqueue.New(chanqueue.WithInputRdOnly[cid.Cid](pins))
		return queue.Out(), nil
	}
}

func NewPrioritizedProvider(streams ...KeyChanFunc) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		outCh := make(chan cid.Cid)

		go func() {
			defer close(outCh)
			visited := cidutil.NewSet()

			handleStream := func(stream KeyChanFunc, markVisited bool) error {
				ch, err := stream(ctx)
				if err != nil {
					return err
				}

				for {
					select {
					case <-ctx.Done():
						return nil
					case c, ok := <-ch:
						if !ok {
							return nil
						}

						if visited.Has(c) {
							continue
						}

						select {
						case <-ctx.Done():
							return nil
						case outCh <- c:
							if markVisited {
								_ = visited.Visit(c)
							}
						}
					}
				}
			}

			last := len(streams) - 1
			for i, stream := range streams {
				if err := handleStream(stream, i < last); err != nil {
					log.Warnf("error in prioritized strategy while handling CID stream %d: %w", i, err)
					return
				}
			}
		}()

		return outCh, nil
	}
}
