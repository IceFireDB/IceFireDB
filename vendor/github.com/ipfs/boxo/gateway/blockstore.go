package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/util"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap/zapcore"
)

type cacheBlockStore struct {
	cache               *lru.TwoQueueCache[string, []byte]
	rehash              atomic.Bool
	cacheHitsMetric     prometheus.Counter
	cacheRequestsMetric prometheus.Counter
}

var _ blockstore.Blockstore = (*cacheBlockStore)(nil)

// NewCacheBlockStore creates a new [blockstore.Blockstore] that caches blocks
// in memory using a two queue cache. It can be useful, for example, when paired
// with a proxy blockstore (see [NewRemoteBlockstore]).
//
// If the given [prometheus.Registerer] is nil, a [prometheus.DefaultRegisterer] will be used.
func NewCacheBlockStore(size int, reg prometheus.Registerer) (blockstore.Blockstore, error) {
	c, err := lru.New2Q[string, []byte](size)
	if err != nil {
		return nil, err
	}

	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	cacheHitsMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "http",
		Name:      "blockstore_cache_hit",
		Help:      "The number of global block cache hits.",
	})

	cacheRequestsMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "http",
		Name:      "blockstore_cache_requests",
		Help:      "The number of global block cache requests.",
	})

	registerMetric(reg, cacheHitsMetric)
	registerMetric(reg, cacheRequestsMetric)

	return &cacheBlockStore{
		cache:               c,
		cacheHitsMetric:     cacheHitsMetric,
		cacheRequestsMetric: cacheRequestsMetric,
	}, nil
}

func (l *cacheBlockStore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	l.cache.Remove(string(c.Hash()))
	return nil
}

func (l *cacheBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return l.cache.Contains(string(c.Hash())), nil
}

func (l *cacheBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	l.cacheRequestsMetric.Add(1)

	blkData, found := l.cache.Get(string(c.Hash()))
	if !found {
		if log.Level().Enabled(zapcore.DebugLevel) {
			log.Debugw("block not found in cache", "cid", c.String())
		}
		return nil, format.ErrNotFound{Cid: c}
	}

	// It's a HIT!
	l.cacheHitsMetric.Add(1)
	if log.Level().Enabled(zapcore.DebugLevel) {
		log.Debugw("block found in cache", "cid", c.String())
	}

	if l.rehash.Load() {
		rbcid, err := c.Prefix().Sum(blkData)
		if err != nil {
			return nil, err
		}

		if !rbcid.Equals(c) {
			return nil, blockstore.ErrHashMismatch
		}
	}

	return blocks.NewBlockWithCid(blkData, c)
}

func (l *cacheBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	blkData, found := l.cache.Get(string(c.Hash()))
	if !found {
		return -1, format.ErrNotFound{Cid: c}
	}

	return len(blkData), nil
}

func (l *cacheBlockStore) Put(ctx context.Context, blk blocks.Block) error {
	l.cache.Add(string(blk.Cid().Hash()), blk.RawData())
	return nil
}

func (l *cacheBlockStore) PutMany(ctx context.Context, blks []blocks.Block) error {
	for _, b := range blks {
		if err := l.Put(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

func (l *cacheBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("not implemented")
}

func (l *cacheBlockStore) HashOnRead(enabled bool) {
	l.rehash.Store(enabled)
}

type remoteBlockstore struct {
	httpClient *http.Client
	gatewayURL []string
	rand       *rand.Rand
	validate   bool
}

// NewRemoteBlockstore creates a new [blockstore.Blockstore] that is backed by one
// or more gateways that support [RAW block] requests. See the [Trustless Gateway]
// specification for more details. You can optionally pass your own [http.Client].
//
// [Trustless Gateway]: https://specs.ipfs.tech/http-gateways/trustless-gateway/
// [RAW block]: https://www.iana.org/assignments/media-types/application/vnd.ipld.raw
func NewRemoteBlockstore(gatewayURL []string, httpClient *http.Client) (blockstore.Blockstore, error) {
	if len(gatewayURL) == 0 {
		return nil, errors.New("missing remote block backend URL")
	}

	if httpClient == nil {
		httpClient = newRemoteHTTPClient()
	}

	return &remoteBlockstore{
		gatewayURL: gatewayURL,
		httpClient: httpClient,
		rand:       rand.New(rand.NewSource(time.Now().Unix())),
		// Enables block validation by default. Important since we are
		// proxying block requests to untrusted gateways.
		validate: true,
	}, nil
}

func (ps *remoteBlockstore) fetch(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	urlStr := fmt.Sprintf("%s/ipfs/%s?format=raw", ps.getRandomGatewayURL(), c)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	log.Debugw("raw fetch", "url", req.URL)
	req.Header.Set("Accept", "application/vnd.ipld.raw")
	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http error from remote block backend: %s", resp.Status)
	}

	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if ps.validate {
		nc, err := c.Prefix().Sum(rb)
		if err != nil {
			return nil, blocks.ErrWrongHash
		}
		if !nc.Equals(c) {
			return nil, blocks.ErrWrongHash
		}
	}

	return blocks.NewBlockWithCid(rb, c)
}

func (ps *remoteBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	blk, err := ps.fetch(ctx, c)
	if err != nil {
		return false, err
	}
	return blk != nil, nil
}

func (ps *remoteBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, err := ps.fetch(ctx, c)
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func (ps *remoteBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	blk, err := ps.fetch(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(blk.RawData()), nil
}

func (ps *remoteBlockstore) HashOnRead(enabled bool) {
	ps.validate = enabled
}

func (c *remoteBlockstore) Put(context.Context, blocks.Block) error {
	return util.ErrNotImplemented
}

func (c *remoteBlockstore) PutMany(context.Context, []blocks.Block) error {
	return util.ErrNotImplemented
}

func (c *remoteBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, util.ErrNotImplemented
}

func (c *remoteBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	return util.ErrNotImplemented
}

func (ps *remoteBlockstore) getRandomGatewayURL() string {
	return ps.gatewayURL[ps.rand.Intn(len(ps.gatewayURL))]
}
