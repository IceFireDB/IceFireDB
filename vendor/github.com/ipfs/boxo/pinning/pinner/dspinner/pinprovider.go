package dspinner

import (
	"context"

	"github.com/ipfs/boxo/fetcher"
	fetcherhelpers "github.com/ipfs/boxo/fetcher/helpers"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// NewPinnedProvider returns a KeyChanFunc supplying pinned keys. The Provider
// will block when writing to the channel and there are no readers.
func NewPinnedProvider(onlyRoots bool, pinning ipfspinner.Pinner, fetchConfig fetcher.Factory) provider.KeyChanFunc {
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

func pinSet(ctx context.Context, pinning ipfspinner.Pinner, fetchConfig fetcher.Factory, onlyRoots bool) (*cidutil.StreamingSet, error) {
	set := cidutil.NewStreamingSet()
	recursivePins := cidutil.NewSet()

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer close(set.New)

		// 1. Recursive keys
		for sc := range pinning.RecursiveKeys(ctx, false) {
			if sc.Err != nil {
				log.Errorf("reprovide recursive pins: %s", sc.Err)
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
				log.Errorf("reprovide direct pins: %s", sc.Err)
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
			log.Errorf("reprovide indirect pins: %s", err)
			return
		}
	}()

	return set, nil
}
