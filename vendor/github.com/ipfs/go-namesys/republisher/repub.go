// Package republisher provides a utility to automatically re-publish IPNS
// records related to the keys in a Keystore.
package republisher

import (
	"context"
	"errors"
	"time"

	keystore "github.com/ipfs/go-ipfs-keystore"
	namesys "github.com/ipfs/go-namesys"
	path "github.com/ipfs/go-path"
	"go.opentelemetry.io/otel/attribute"

	proto "github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	ipns "github.com/ipfs/go-ipns"
	pb "github.com/ipfs/go-ipns/pb"
	logging "github.com/ipfs/go-log"
	goprocess "github.com/jbenet/goprocess"
	gpctx "github.com/jbenet/goprocess/context"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var errNoEntry = errors.New("no previous entry")

var log = logging.Logger("ipns-repub")

// DefaultRebroadcastInterval is the default interval at which we rebroadcast IPNS records
var DefaultRebroadcastInterval = time.Hour * 4

// InitialRebroadcastDelay is the delay before first broadcasting IPNS records on start
var InitialRebroadcastDelay = time.Minute * 1

// FailureRetryInterval is the interval at which we retry IPNS records broadcasts (when they fail)
var FailureRetryInterval = time.Minute * 5

// DefaultRecordLifetime is the default lifetime for IPNS records
const DefaultRecordLifetime = time.Hour * 24

// Republisher facilitates the regular publishing of all the IPNS records
// associated to keys in a Keystore.
type Republisher struct {
	ns   namesys.Publisher
	ds   ds.Datastore
	self ic.PrivKey
	ks   keystore.Keystore

	Interval time.Duration

	// how long records that are republished should be valid for
	RecordLifetime time.Duration
}

// NewRepublisher creates a new Republisher
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

// Run starts the republisher facility. It can be stopped by stopping the
// provided proc.
func (rp *Republisher) Run(proc goprocess.Process) {
	timer := time.NewTimer(InitialRebroadcastDelay)
	defer timer.Stop()
	if rp.Interval < InitialRebroadcastDelay {
		timer.Reset(rp.Interval)
	}

	for {
		select {
		case <-timer.C:
			timer.Reset(rp.Interval)
			err := rp.republishEntries(proc)
			if err != nil {
				log.Info("republisher failed to republish: ", err)
				if FailureRetryInterval < rp.Interval {
					timer.Reset(FailureRetryInterval)
				}
			}
		case <-proc.Closing():
			return
		}
	}
}

func (rp *Republisher) republishEntries(p goprocess.Process) error {
	ctx, cancel := context.WithCancel(gpctx.OnClosingContext(p))
	defer cancel()
	ctx, span := namesys.StartSpan(ctx, "Republisher.RepublishEntries")
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
	ctx, span := namesys.StartSpan(ctx, "Republisher.RepublishEntry")
	defer span.End()
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		span.RecordError(err)
		return err
	}

	log.Debugf("republishing ipns entry for %s", id)

	// Look for it locally only
	e, err := rp.getLastIPNSEntry(ctx, id)
	if err != nil {
		if err == errNoEntry {
			span.SetAttributes(attribute.Bool("NoEntry", true))
			return nil
		}
		span.RecordError(err)
		return err
	}

	p := path.Path(e.GetValue())
	prevEol, err := ipns.GetEOL(e)
	if err != nil {
		span.RecordError(err)
		return err
	}

	// update record with same sequence number
	eol := time.Now().Add(rp.RecordLifetime)
	if prevEol.After(eol) {
		eol = prevEol
	}
	err = rp.ns.PublishWithEOL(ctx, priv, p, eol)
	span.RecordError(err)
	return err
}

func (rp *Republisher) getLastIPNSEntry(ctx context.Context, id peer.ID) (*pb.IpnsEntry, error) {
	// Look for it locally only
	val, err := rp.ds.Get(ctx, namesys.IpnsDsKey(id))
	switch err {
	case nil:
	case ds.ErrNotFound:
		return nil, errNoEntry
	default:
		return nil, err
	}

	e := new(pb.IpnsEntry)
	if err := proto.Unmarshal(val, e); err != nil {
		return nil, err
	}
	return e, nil
}
