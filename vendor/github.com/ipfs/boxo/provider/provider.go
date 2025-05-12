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
	blocks "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/fetcher"
	fetcherhelpers "github.com/ipfs/boxo/fetcher/helpers"
	pin "github.com/ipfs/boxo/pinning/pinner"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var logR = logging.Logger("reprovider.simple")

// Provider announces blocks to the network
type Provider interface {
	// Provide takes a cid and makes an attempt to announce it to the network
	Provide(context.Context, cid.Cid, bool) error
}

// Reprovider reannounces blocks to the network
type Reprovider interface {
	// Reprovide starts a new reprovide if one isn't running already.
	Reprovide(context.Context) error
}

// System defines the interface for interacting with the value
// provider system
type System interface {
	Close() error
	Stat() (ReproviderStats, error)
	Provider
	Reprovider
}

// KeyChanFunc is function streaming CIDs to pass to content routing
type KeyChanFunc func(context.Context) (<-chan cid.Cid, error)

// NewBlockstoreProvider returns key provider using bstore.AllKeysChan
func NewBlockstoreProvider(bstore blocks.Blockstore) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		return bstore.AllKeysChan(ctx)
	}
}

// NewPinnedProvider returns a KeyChanFunc supplying pinned keys. The Provider
// will block when writing to the channel and there are no readers.
func NewPinnedProvider(onlyRoots bool, pinning pin.Pinner, fetchConfig fetcher.Factory) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		set, err := pinSet(ctx, pinning, fetchConfig, onlyRoots)
		if err != nil {
			return nil, err
		}

		outCh := make(chan cid.Cid)
		go func() {
			defer close(outCh)
			for c := range set.New {
				select {
				case <-ctx.Done():
					return
				case outCh <- c:
				}
			}
		}()

		return outCh, nil
	}
}

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

func pinSet(ctx context.Context, pinning pin.Pinner, fetchConfig fetcher.Factory, onlyRoots bool) (*cidutil.StreamingSet, error) {
	set := cidutil.NewStreamingSet()
	recursivePins := cidutil.NewSet()

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer close(set.New)

		// 1. Recursive keys
		for sc := range pinning.RecursiveKeys(ctx, false) {
			if sc.Err != nil {
				logR.Errorf("reprovide recursive pins: %s", sc.Err)
				return
			}
			if !onlyRoots {
				// Save some bytes.
				_ = recursivePins.Visit(sc.Pin.Key)
			}
			_ = set.Visitor(ctx)(sc.Pin.Key)
		}

		// 2. Direct pins
		for sc := range pinning.DirectKeys(ctx, false) {
			if sc.Err != nil {
				logR.Errorf("reprovide direct pins: %s", sc.Err)
				return
			}
			_ = set.Visitor(ctx)(sc.Pin.Key)
		}

		if onlyRoots {
			return
		}

		// 3. Go through recursive pins to fetch remaining blocks if we want more
		// than just roots.
		session := fetchConfig.NewSession(ctx)
		err := recursivePins.ForEach(func(c cid.Cid) error {
			return fetcherhelpers.BlockAll(ctx, session, cidlink.Link{Cid: c}, func(res fetcher.FetchResult) error {
				clink, ok := res.LastBlockLink.(cidlink.Link)
				if ok {
					_ = set.Visitor(ctx)(clink.Cid)
				}
				return nil
			})
		})
		if err != nil {
			logR.Errorf("reprovide indirect pins: %s", err)
			return
		}
	}()

	return set, nil
}

func NewPrioritizedProvider(priorityCids KeyChanFunc, otherCids KeyChanFunc) KeyChanFunc {
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

			err := handleStream(priorityCids, true)
			if err != nil {
				log.Warnf("error in prioritized strategy while handling priority CIDs: %w", err)
				return
			}

			err = handleStream(otherCids, false)
			if err != nil {
				log.Warnf("error in prioritized strategy while handling other CIDs: %w", err)
			}
		}()

		return outCh, nil
	}
}
