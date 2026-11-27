// Package offline implements Routing with a client which
// is only able to perform offline operations.
package offline

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// ErrOffline is returned when trying to perform operations that
// require connectivity.
var ErrOffline = errors.New("routing system in offline mode")

// Option configures a router returned by NewOfflineRouter.
type Option func(*offlineRouting)

// WithMaxRecordAge caps how long a stored value record is served, measured from
// the time the record was stored: once a record is older than age it is treated
// as absent and dropped on the next read. This is independent of the record's
// own validity, an IPNS record is dropped once it passes its EOL regardless of
// this setting.
//
// The default is no cap: records are served for as long as they stay valid.
// This fits the offline router's role as a local store of the node's own
// records, which have no republisher to refresh their stored timestamps. Set a
// cap to mirror the go-libp2p-kad-dht value store (amino.DefaultMaxRecordAge)
// or to bound a datastore that nothing else garbage-collects. A value <= 0
// keeps the no-cap default.
func WithMaxRecordAge(age time.Duration) Option {
	return func(r *offlineRouting) {
		r.maxRecordAge = age
	}
}

// NewOfflineRouter returns a Routing implementation which only performs
// offline operations. It allows to Put and Get signed dht
// records to and from the local datastore.
//
// Records are stored in the same datastore layout as the
// go-libp2p-kad-dht value store (go-libp2p-kad-dht v0.42.0 and later),
// so a node that shares dstore between this router and a DHT instance
// can resolve records offline that were published online and vice
// versa. Records stored by earlier versions of this package (root-level
// base32 keys, the pre-v0.42.0 kad-dht layout) are not read anymore.
//
// A stored record is served for as long as it stays valid (for IPNS, until its
// EOL); see WithMaxRecordAge to also cap retention by store age.
func NewOfflineRouter(dstore ds.Datastore, validator record.Validator, opts ...Option) routing.Routing {
	r := &offlineRouting{
		validator: validator,
	}
	for _, opt := range opts {
		opt(r)
	}
	r.valstore = records.NewValueStore(dstore, validator, r.maxRecordAge)
	return r
}

// offlineRouting implements the Routing interface,
// but only provides the capability to Put and Get signed dht
// records to and from the local datastore.
type offlineRouting struct {
	valstore     *records.ValueStore
	validator    record.Validator
	maxRecordAge time.Duration
}

func (c *offlineRouting) PutValue(ctx context.Context, key string, val []byte, _ ...routing.Option) error {
	rec := record.MakePutRecord(key, val)
	// Validate and store the record.
	err := c.valstore.Put(ctx, key, rec)
	if errors.Is(err, records.ErrOldRecord) {
		// be idempotent to be nice.
		if stored, gerr := c.valstore.Get(ctx, key); gerr == nil && stored != nil && bytes.Equal(stored.GetValue(), val) {
			return nil
		}
	}
	return err
}

func (c *offlineRouting) GetValue(ctx context.Context, key string, _ ...routing.Option) ([]byte, error) {
	rec, err := c.valstore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, routing.ErrNotFound
	}

	val := rec.GetValue()
	// The value store only age-checks records on read; run the validator
	// so callers never see records that are expired by their own rules
	// (e.g. IPNS EOL).
	if err := c.validator.Validate(key, val); err != nil {
		return nil, err
	}
	return val, nil
}

func (c *offlineRouting) SearchValue(ctx context.Context, key string, _ ...routing.Option) (<-chan []byte, error) {
	out := make(chan []byte, 1)
	go func() {
		defer close(out)
		v, err := c.GetValue(ctx, key)
		if err == nil {
			out <- v
		}
	}()
	return out, nil
}

func (c *offlineRouting) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	return peer.AddrInfo{}, ErrOffline
}

func (c *offlineRouting) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	close(out)
	return out
}

func (c *offlineRouting) Provide(_ context.Context, k cid.Cid, _ bool) error {
	return ErrOffline
}

func (c *offlineRouting) Ping(ctx context.Context, p peer.ID) (time.Duration, error) {
	return 0, ErrOffline
}

func (c *offlineRouting) Bootstrap(context.Context) error {
	return nil
}

// ensure offlineRouting matches the Routing interface
var _ routing.Routing = &offlineRouting{}
