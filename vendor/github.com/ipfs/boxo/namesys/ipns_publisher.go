package namesys

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	ds "github.com/ipfs/go-datastore"
	dsquery "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/whyrusleeping/base32"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// IPNSPublisher implements [Publisher] for IPNS Records.
type IPNSPublisher struct {
	routing routing.ValueStore
	ds      ds.Datastore

	// Used to ensure we assign IPNS records sequential sequence numbers.
	mu sync.Mutex
}

var _ Publisher = &IPNSPublisher{}

// NewIPNSResolver constructs a new [IPNSResolver] from a [routing.ValueStore] and
// a [ds.Datastore].
func NewIPNSPublisher(route routing.ValueStore, ds ds.Datastore) *IPNSPublisher {
	if ds == nil {
		panic("nil datastore")
	}

	return &IPNSPublisher{routing: route, ds: ds}
}

func (p *IPNSPublisher) Publish(ctx context.Context, priv crypto.PrivKey, value path.Path, options ...PublishOption) error {
	log.Debugf("Publish %s", value)

	ctx, span := startSpan(ctx, "IPNSPublisher.Publish", trace.WithAttributes(attribute.String("Value", value.String())))
	defer span.End()

	record, err := p.updateRecord(ctx, priv, value, options...)
	if err != nil {
		return err
	}

	return PublishIPNSRecord(ctx, p.routing, priv.GetPublic(), record)
}

// IpnsDsKey returns a datastore key given an IPNS identifier (peer
// ID). Defines the storage key for IPNS records in the local datastore.
func IpnsDsKey(name ipns.Name) ds.Key {
	return ds.NewKey("/ipns/" + base32.RawStdEncoding.EncodeToString([]byte(name.Peer())))
}

// ListPublished returns the latest IPNS records published by this node and
// their expiration times.
//
// This method will not search the routing system for records published by other
// nodes.
func (p *IPNSPublisher) ListPublished(ctx context.Context) (map[ipns.Name]*ipns.Record, error) {
	query, err := p.ds.Query(ctx, dsquery.Query{
		Prefix: ipns.NamespacePrefix,
	})
	if err != nil {
		return nil, err
	}
	defer query.Close()

	records := make(map[ipns.Name]*ipns.Record)
	for {
		select {
		case result, ok := <-query.Next():
			if !ok {
				return records, nil
			}
			if result.Error != nil {
				return nil, result.Error
			}
			rec, err := ipns.UnmarshalRecord(result.Value)
			if err != nil {
				// Might as well return what we can.
				log.Error("found an invalid IPNS entry:", err)
				continue
			}
			if !strings.HasPrefix(result.Key, ipns.NamespacePrefix) {
				log.Errorf("datastore query for keys with prefix %s returned a key: %s", ipns.NamespacePrefix, result.Key)
				continue
			}
			k := result.Key[len(ipns.NamespacePrefix):]
			pid, err := base32.RawStdEncoding.DecodeString(k)
			if err != nil {
				log.Errorf("ipns ds key invalid: %s", result.Key)
				continue
			}
			records[ipns.NameFromPeer(peer.ID(pid))] = rec
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// GetPublished returns the record this node has published corresponding to the
// given peer ID.
//
// If `checkRouting` is true and we have no existing record, this method will
// check the routing system for any existing records.
func (p *IPNSPublisher) GetPublished(ctx context.Context, name ipns.Name, checkRouting bool) (*ipns.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	value, err := p.ds.Get(ctx, IpnsDsKey(name))
	switch err {
	case nil:
	case ds.ErrNotFound:
		if !checkRouting {
			return nil, nil
		}
		routingKey := name.RoutingKey()
		value, err = p.routing.GetValue(ctx, string(routingKey))
		if err != nil {
			// Not found or other network issue. Can't really do
			// anything about this case.
			if err != routing.ErrNotFound {
				log.Debugf("error when determining the last published IPNS record for %s: %s", name, err)
			}

			return nil, nil
		}
	default:
		return nil, err
	}

	return ipns.UnmarshalRecord(value)
}

func (p *IPNSPublisher) updateRecord(ctx context.Context, k crypto.PrivKey, value path.Path, options ...PublishOption) (*ipns.Record, error) {
	id, err := peer.IDFromPrivateKey(k)
	if err != nil {
		return nil, err
	}
	name := ipns.NameFromPeer(id)

	p.mu.Lock()
	defer p.mu.Unlock()

	// get previous records sequence number
	rec, err := p.GetPublished(ctx, name, true)
	if err != nil {
		return nil, err
	}

	seq := uint64(0)
	if rec != nil {
		seq, err = rec.Sequence()
		if err != nil {
			return nil, err
		}

		p, err := rec.Value()
		if err != nil {
			return nil, err
		}
		if value.String() != p.String() {
			// Don't bother incrementing the sequence number unless the
			// value changes.
			seq++
		}
	}

	opts := ProcessPublishOptions(options)

	// Create record
	r, err := ipns.NewRecord(k, value, seq, opts.EOL, opts.TTL, opts.IPNSOptions...)
	if err != nil {
		return nil, err
	}

	data, err := ipns.MarshalRecord(r)
	if err != nil {
		return nil, err
	}

	// Put the new record.
	dsKey := IpnsDsKey(name)
	if err := p.ds.Put(ctx, dsKey, data); err != nil {
		return nil, err
	}
	if err := p.ds.Sync(ctx, dsKey); err != nil {
		return nil, err
	}

	return r, nil
}

// PublishIPNSRecord publishes the given [ipns.Record] for the provided [crypto.PubKey] in
// the provided [routing.ValueStore]. The public key is also made available to the routing
// system if it cannot be derived from the corresponding [peer.ID].
func PublishIPNSRecord(ctx context.Context, r routing.ValueStore, pubKey crypto.PubKey, rec *ipns.Record) error {
	ctx, span := startSpan(ctx, "PublishIPNSRecord")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 2) // At most two errors (IPNS, and public key)

	pid, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return err
	}

	go func() {
		errs <- PutIPNSRecord(ctx, r, ipns.NameFromPeer(pid), rec)
	}()

	// Publish the public key if the public key cannot be extracted from the peer ID.
	// This is most likely not necessary since IPNS Records include, by default, the public
	// key in those cases. However, this ensures it's still possible to easily retrieve
	// the public key if, for some reason, it is not embedded.
	if _, err := pid.ExtractPublicKey(); errors.Is(err, peer.ErrNoPublicKey) {
		go func() {
			errs <- PutPublicKey(ctx, r, pid, pubKey)
		}()

		if err := waitOnErrChan(ctx, errs); err != nil {
			return err
		}
	}

	return waitOnErrChan(ctx, errs)
}

func waitOnErrChan(ctx context.Context, errs chan error) error {
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PutPublicKey puts the given [crypto.PubKey] for the given [peer.ID] in the [routing.ValueStore].
func PutPublicKey(ctx context.Context, r routing.ValueStore, pid peer.ID, pubKey crypto.PubKey) error {
	routingKey := PkRoutingKey(pid)
	ctx, span := startSpan(ctx, "PutPublicKey", trace.WithAttributes(attribute.String("Key", routingKey)))
	defer span.End()

	bytes, err := crypto.MarshalPublicKey(pubKey)
	if err != nil {
		return err
	}

	log.Debugf("Storing public key at: %x", routingKey)
	return r.PutValue(ctx, routingKey, bytes)
}

// PkRoutingKey returns the public key routing key for the given [peer.ID].
func PkRoutingKey(id peer.ID) string {
	return "/pk/" + string(id)
}

// PutIPNSRecord puts the given [ipns.Record] for the given [ipns.Name] in the [routing.ValueStore].
func PutIPNSRecord(ctx context.Context, r routing.ValueStore, name ipns.Name, rec *ipns.Record) error {
	routingKey := string(name.RoutingKey())
	ctx, span := startSpan(ctx, "PutIPNSRecord", trace.WithAttributes(attribute.String("IPNSKey", routingKey)))
	defer span.End()

	bytes, err := ipns.MarshalRecord(rec)
	if err != nil {
		return err
	}

	log.Debugf("Storing ipns record at: %x", routingKey)
	return r.PutValue(ctx, routingKey, bytes)
}
