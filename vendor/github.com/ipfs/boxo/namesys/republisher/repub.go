// Package republisher provides a utility to automatically re-publish IPNS
// records related to the keys in a Keystore.
package republisher

import (
	"context"
	"errors"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/keystore"
	"github.com/ipfs/boxo/namesys"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	errNoEntry = errors.New("no previous entry")
	log        = logging.Logger("ipns/repub")
)

const (
	// DefaultRebroadcastInterval is the default interval at which we rebroadcast IPNS records
	DefaultRebroadcastInterval = time.Hour * 4

	// InitialRebroadcastDelay is the delay before first broadcasting IPNS records on start
	InitialRebroadcastDelay = time.Minute * 1

	// FailureRetryInterval is the interval at which we retry IPNS records broadcasts (when they fail)
	FailureRetryInterval = time.Minute * 5

	// DefaultRecordLifetime is the default lifetime for IPNS records
	DefaultRecordLifetime = ipns.DefaultRecordLifetime
)

// Republisher facilitates the regular publishing of all the IPNS records
// associated to keys in a [keystore.Keystore].
type Republisher struct {
	ns   namesys.Publisher
	ds   ds.Datastore
	self ic.PrivKey
	ks   keystore.Keystore

	Interval time.Duration

	// how long records that are republished should be valid for
	RecordLifetime time.Duration
}

// NewRepublisher creates a new [Republisher] from the given options.
func NewRepublisher(ns namesys.Publisher, ds ds.Datastore, self ic.PrivKey, ks keystore.Keystore) *Republisher {
	return &Republisher{
		ns:             ns,
		ds:             ds,
		self:           self,
		ks:             ks,
		Interval:       DefaultRebroadcastInterval,
		RecordLifetime: DefaultRecordLifetime,
	}
}

// Run starts the republisher facility. It can be stopped by calling the returned function..
func (rp *Republisher) Run() func() {
	ctx, cancel := context.WithCancel(context.Background())
	go rp.run(ctx)
	return func() {
		log.Debug("stopping republisher")
		cancel()
	}
}

func (rp *Republisher) run(ctx context.Context) {
	timer := time.NewTimer(InitialRebroadcastDelay)
	defer timer.Stop()
	if rp.Interval < InitialRebroadcastDelay {
		timer.Reset(rp.Interval)
	}

	for {
		select {
		case <-timer.C:
			timer.Reset(rp.Interval)
			err := rp.republishEntries(ctx)
			if err != nil {
				log.Info("republisher failed to republish: ", err)
				if FailureRetryInterval < rp.Interval {
					timer.Reset(FailureRetryInterval)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (rp *Republisher) republishEntries(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx, span := startSpan(ctx, "Republisher.RepublishEntries")
	defer span.End()

	// TODO: Use rp.ipns.ListPublished(). We can't currently *do* that
	// because:
	// 1. There's no way to get keys from the keystore by ID.
	// 2. We don't actually have access to the IPNS publisher.
	err := rp.republishEntry(ctx, rp.self)
	if err != nil {
		return err
	}

	if rp.ks != nil {
		keyNames, err := rp.ks.List()
		if err != nil {
			return err
		}
		for _, name := range keyNames {
			priv, err := rp.ks.Get(name)
			if err != nil {
				return err
			}
			err = rp.republishEntry(ctx, priv)
			if err != nil {
				return err
			}

		}
	}

	return nil
}

func (rp *Republisher) republishEntry(ctx context.Context, priv ic.PrivKey) error {
	ctx, span := startSpan(ctx, "Republisher.RepublishEntry")
	defer span.End()
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		span.RecordError(err)
		return err
	}

	log.Debugf("republishing ipns entry for %s", id)

	// Look for it locally only
	rec, err := rp.getLastIPNSRecord(ctx, ipns.NameFromPeer(id))
	if err != nil {
		if err == errNoEntry {
			span.SetAttributes(attribute.Bool("NoEntry", true))
			return nil
		}
		span.RecordError(err)
		return err
	}

	p, err := rec.Value()
	if err != nil {
		span.RecordError(err)
		return err
	}

	prevEol, err := rec.Validity()
	if err != nil {
		span.RecordError(err)
		return err
	}

	// update record with same sequence number
	eol := time.Now().Add(rp.RecordLifetime)
	if prevEol.After(eol) {
		eol = prevEol
	}
	err = rp.ns.Publish(ctx, priv, p, namesys.PublishWithEOL(eol))
	span.RecordError(err)
	return err
}

func (rp *Republisher) getLastIPNSRecord(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	// Look for it locally only
	val, err := rp.ds.Get(ctx, namesys.IpnsDsKey(name))
	switch err {
	case nil:
	case ds.ErrNotFound:
		return nil, errNoEntry
	default:
		return nil, err
	}

	return ipns.UnmarshalRecord(val)
}

var tracer = otel.Tracer("boxo/namesys/republisher")

func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, "Namesys."+name, opts...)
}
