package contentrouter

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/internal"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var logger = logging.Logger("routing/http/contentrouter")

const ttl = 24 * time.Hour

type Client interface {
	FindProviders(ctx context.Context, key cid.Cid) (iter.ResultIter[types.Record], error)
	ProvideBitswap(ctx context.Context, keys []cid.Cid, ttl time.Duration) (time.Duration, error)
	FindPeers(ctx context.Context, pid peer.ID) (peers iter.ResultIter[*types.PeerRecord], err error)
	GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error)
	PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error
}

type contentRouter struct {
	client                Client
	maxProvideConcurrency int
	maxProvideBatchSize   int
}

var (
	_ routing.ContentRouting           = (*contentRouter)(nil)
	_ routing.PeerRouting              = (*contentRouter)(nil)
	_ routing.ValueStore               = (*contentRouter)(nil)
	_ routinghelpers.ProvideManyRouter = (*contentRouter)(nil)
	_ routinghelpers.ReadyAbleRouter   = (*contentRouter)(nil)
)

type option func(c *contentRouter)

func WithMaxProvideConcurrency(max int) option {
	return func(c *contentRouter) {
		c.maxProvideConcurrency = max
	}
}

func WithMaxProvideBatchSize(max int) option {
	return func(c *contentRouter) {
		c.maxProvideBatchSize = max
	}
}

func NewContentRoutingClient(c Client, opts ...option) *contentRouter {
	cr := &contentRouter{
		client:                c,
		maxProvideConcurrency: 5,
		maxProvideBatchSize:   100,
	}
	for _, opt := range opts {
		opt(cr)
	}
	return cr
}

func (c *contentRouter) Provide(ctx context.Context, key cid.Cid, announce bool) error {
	// If 'true' is passed, it also announces it, otherwise it is just kept in the local
	// accounting of which objects are being provided.
	if !announce {
		return nil
	}

	_, err := c.client.ProvideBitswap(ctx, []cid.Cid{key}, ttl)
	return err
}

// ProvideMany provides a set of keys to the remote delegate.
// Large sets of keys are chunked into multiple requests and sent concurrently, according to the concurrency configuration.
// TODO: switch to use [client.Provide] when ready.
func (c *contentRouter) ProvideMany(ctx context.Context, mhKeys []multihash.Multihash) error {
	keys := make([]cid.Cid, 0, len(mhKeys))
	for _, m := range mhKeys {
		keys = append(keys, cid.NewCidV1(cid.Raw, m))
	}

	if len(keys) <= c.maxProvideBatchSize {
		_, err := c.client.ProvideBitswap(ctx, keys, ttl)
		return err
	}

	return internal.DoBatch(
		ctx,
		c.maxProvideBatchSize,
		c.maxProvideConcurrency,
		keys,
		func(ctx context.Context, batch []cid.Cid) error {
			_, err := c.client.ProvideBitswap(ctx, batch, ttl)
			return err
		},
	)
}

// Ready is part of the existing [routing.ReadyAbleRouter] interface.
func (c *contentRouter) Ready() bool {
	return true
}

// readProviderResponses reads peer records (and bitswap records for legacy
// compatibility) from the iterator into the given channel.
func readProviderResponses(ctx context.Context, iter iter.ResultIter[types.Record], ch chan<- peer.AddrInfo) {
	defer close(ch)
	defer iter.Close()
	for iter.Next() {
		res := iter.Val()
		if res.Err != nil {
			logger.Warnf("error iterating provider responses: %s", res.Err)
			continue
		}
		v := res.Val
		switch v.GetSchema() {
		case types.SchemaPeer:
			result, ok := v.(*types.PeerRecord)
			if !ok {
				logger.Errorw(
					"problem casting find providers result",
					"Schema", v.GetSchema(),
					"Type", reflect.TypeOf(v).String(),
				)
				continue
			}

			var addrs []multiaddr.Multiaddr
			for _, a := range result.Addrs {
				addrs = append(addrs, a.Multiaddr)
			}

			select {
			case <-ctx.Done():
				return
			case ch <- peer.AddrInfo{
				ID:    *result.ID,
				Addrs: addrs,
			}:
			}

		//nolint:staticcheck
		//lint:ignore SA1019 // ignore staticcheck
		case types.SchemaBitswap:
			//lint:ignore SA1019 // ignore staticcheck
			result, ok := v.(*types.BitswapRecord)
			if !ok {
				logger.Errorw(
					"problem casting find providers result",
					"Schema", v.GetSchema(),
					"Type", reflect.TypeOf(v).String(),
				)
				continue
			}

			var addrs []multiaddr.Multiaddr
			for _, a := range result.Addrs {
				addrs = append(addrs, a.Multiaddr)
			}

			select {
			case <-ctx.Done():
				return
			case ch <- peer.AddrInfo{
				ID:    *result.ID,
				Addrs: addrs,
			}:
			}
		}
	}
}

func (c *contentRouter) FindProvidersAsync(ctx context.Context, key cid.Cid, numResults int) <-chan peer.AddrInfo {
	resultsIter, err := c.client.FindProviders(ctx, key)
	if err != nil {
		logger.Warnw("error finding providers", "CID", key, "Error", err)
		ch := make(chan peer.AddrInfo)
		close(ch)
		return ch
	}
	ch := make(chan peer.AddrInfo)
	go readProviderResponses(ctx, resultsIter, ch)
	return ch
}

func (c *contentRouter) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	iter, err := c.client.FindPeers(ctx, pid)
	if err != nil {
		return peer.AddrInfo{}, err
	}
	defer iter.Close()

	for iter.Next() {
		res := iter.Val()
		if res.Err != nil {
			logger.Warnf("error iterating peer responses: %s", res.Err)
			continue
		}

		if *res.Val.ID != pid {
			logger.Warnf("searched for peerID %s, got response for %s:", pid, *res.Val.ID)
			continue
		}

		var addrs []multiaddr.Multiaddr
		for _, a := range res.Val.Addrs {
			addrs = append(addrs, a.Multiaddr)
		}

		// If there are no addresses there's nothing of value to return
		if len(addrs) == 0 {
			continue
		}

		return peer.AddrInfo{
			ID:    pid,
			Addrs: addrs,
		}, nil
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}

func (c *contentRouter) PutValue(ctx context.Context, key string, data []byte, opts ...routing.Option) error {
	if !strings.HasPrefix(key, "/ipns/") {
		return routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return err
	}

	record, err := ipns.UnmarshalRecord(data)
	if err != nil {
		return err
	}

	return c.client.PutIPNS(ctx, name, record)
}

func (c *contentRouter) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	if !strings.HasPrefix(key, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return nil, err
	}

	record, err := c.client.GetIPNS(ctx, name)
	if err != nil {
		return nil, err
	}

	return ipns.MarshalRecord(record)
}

func (c *contentRouter) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	if !strings.HasPrefix(key, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte)

	go func() {
		record, err := c.client.GetIPNS(ctx, name)
		if err != nil {
			close(ch)
			return
		}

		raw, err := ipns.MarshalRecord(record)
		if err != nil {
			close(ch)
			return
		}

		ch <- raw
		close(ch)
	}()

	return ch, nil
}
