// Package namesys defines Resolver and Publisher interfaces for IPNS paths,
// that is, IPFS paths in the form of /ipns/<name_to_be_resolved>. A "resolved"
// IPNS path becomes an /ipfs/<cid> path.
//
// Traditionally, these paths would be in the form of /ipns/peer_id, which
// references an IPNS record in a distributed ValueStore (usually the IPFS
// DHT).
//
// Additionally, the /ipns/ namespace can also be used with domain names that
// use DNSLink (/ipns/<dnslink_name>, https://docs.ipfs.io/concepts/dnslink/)
//
// The package provides implementations for all three resolvers.
package namesys

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/miekg/dns"
	madns "github.com/multiformats/go-multiaddr-dns"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
)

// namesys is a multi-protocol [NameSystem] that implements generic IPFS naming.
// It uses several [Resolver]s:
//
//  1. IPFS routing naming: SFS-like PKI names.
//  2. dns domains: resolves using links in DNS TXT records
//
// It can only publish to: 1. IPFS routing naming.
type namesys struct {
	ds ds.Datastore

	dnsResolver, ipnsResolver resolver
	ipnsPublisher             Publisher

	staticMap   map[string]*cacheEntry
	cache       *lru.Cache[string, cacheEntry]
	maxCacheTTL *time.Duration
}

var _ NameSystem = &namesys{}

type Option func(*namesys) error

// WithCache is an option that instructs the name system to use a (LRU) cache of the given size.
func WithCache(size int) Option {
	return func(ns *namesys) error {
		if size <= 0 {
			return fmt.Errorf("invalid cache size %d; must be > 0", size)
		}

		cache, err := lru.New[string, cacheEntry](size)
		if err != nil {
			return err
		}

		ns.cache = cache
		return nil
	}
}

// WithMaxCacheTTL configures the maximum cache TTL. By default, if the cache is
// enabled, the entry TTL will be used for caching. By setting this option, you
// can limit how long that TTL is.
//
// For example, if you configure a maximum cache TTL of 1 minute:
//   - Entry TTL is 5 minutes -> Cache TTL is 1 minute
//   - Entry TTL is 30 seconds -> Cache TTL is 30 seconds
func WithMaxCacheTTL(dur time.Duration) Option {
	return func(n *namesys) error {
		n.maxCacheTTL = &dur
		return nil
	}
}

// WithDNSResolver is an option that supplies a custom DNS resolver to use instead
// of the system default.
func WithDNSResolver(rslv madns.BasicResolver) Option {
	return func(ns *namesys) error {
		ns.dnsResolver = NewDNSResolver(rslv.LookupTXT)
		return nil
	}
}

// WithDatastore is an option that supplies a datastore to use instead of an in-memory map datastore.
// The datastore is used to store published IPNS Records and make them available for querying.
func WithDatastore(ds ds.Datastore) Option {
	return func(ns *namesys) error {
		ns.ds = ds
		return nil
	}
}

// NewNameSystem constructs an IPFS [NameSystem] based on the given [routing.ValueStore].
func NewNameSystem(r routing.ValueStore, opts ...Option) (NameSystem, error) {
	var staticMap map[string]*cacheEntry

	// Prewarm namesys cache with static records for deterministic tests and debugging.
	// Useful for testing things like DNSLink without real DNS lookup.
	// Example:
	// IPFS_NS_MAP="dnslink-test.example.com:/ipfs/bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"
	if list := os.Getenv("IPFS_NS_MAP"); list != "" {
		staticMap = make(map[string]*cacheEntry)
		for _, pair := range strings.Split(list, ",") {
			mapping := strings.SplitN(pair, ":", 2)
			key := mapping[0]
			value, err := path.NewPath(mapping[1])
			if err != nil {
				return nil, err
			}
			staticMap[ipns.NamespacePrefix+key] = &cacheEntry{val: value, ttl: 0}
		}
	}

	ns := &namesys{
		staticMap: staticMap,
	}

	for _, opt := range opts {
		err := opt(ns)
		if err != nil {
			return nil, err
		}
	}

	if ns.ds == nil {
		ns.ds = dssync.MutexWrap(ds.NewMapDatastore())
	}

	if ns.dnsResolver == nil {
		ns.dnsResolver = NewDNSResolver(madns.DefaultResolver.LookupTXT)
	}

	ns.ipnsResolver = NewIPNSResolver(r)
	ns.ipnsPublisher = NewIPNSPublisher(r, ns.ds)

	return ns, nil
}

// Resolve implements Resolver.
func (ns *namesys) Resolve(ctx context.Context, p path.Path, options ...ResolveOption) (Result, error) {
	ctx, span := startSpan(ctx, "namesys.Resolve", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	return resolve(ctx, ns, p, ProcessResolveOptions(options))
}

func (ns *namesys) ResolveAsync(ctx context.Context, p path.Path, options ...ResolveOption) <-chan AsyncResult {
	ctx, span := startSpan(ctx, "namesys.ResolveAsync", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	return resolveAsync(ctx, ns, p, ProcessResolveOptions(options))
}

// resolveOnce implements resolver.
func (ns *namesys) resolveOnceAsync(ctx context.Context, p path.Path, options ResolveOptions) <-chan AsyncResult {
	ctx, span := startSpan(ctx, "namesys.ResolveOnceAsync", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	out := make(chan AsyncResult, 1)
	if !p.Mutable() {
		out <- AsyncResult{Path: p}
		close(out)
		return out
	}

	segments := p.Segments()
	resolvablePath, err := path.NewPathFromSegments(segments[0], segments[1])
	if err != nil {
		out <- AsyncResult{Err: err}
		close(out)
		return out
	}

	if resolvedBase, ttl, lastMod, ok := ns.cacheGet(resolvablePath.String()); ok {
		p, err = joinPaths(resolvedBase, p)
		span.SetAttributes(attribute.Bool("CacheHit", true))
		span.RecordError(err)
		out <- AsyncResult{Path: p, TTL: ttl, LastMod: lastMod, Err: err}
		close(out)
		return out
	} else {
		span.SetAttributes(attribute.Bool("CacheHit", false))
	}

	// Resolver selection:
	// 	1. If it is an IPNS Name, resolve through IPNS.
	// 	2. if it is a domain name, resolve through DNSLink.

	var res resolver
	if _, err := ipns.NameFromString(segments[1]); err == nil {
		res = ns.ipnsResolver
	} else if _, ok := dns.IsDomainName(segments[1]); ok {
		res = ns.dnsResolver
	} else {
		// CIDs in IPNS are expected to have libp2p-key multicodec
		// We ease the transition by returning a more meaningful error with a valid CID
		ipnsCid, cidErr := cid.Decode(segments[1])
		if cidErr == nil && ipnsCid.Version() == 1 && ipnsCid.Type() != cid.Libp2pKey {
			fixedCid := cid.NewCidV1(cid.Libp2pKey, ipnsCid.Hash()).String()
			codecErr := fmt.Errorf("peer ID represented as CIDv1 require libp2p-key multicodec: retry with /ipns/%s", fixedCid)
			log.Debugf("RoutingResolver: could not convert public key hash %q to peer ID: %s\n", segments[1], codecErr)
			out <- AsyncResult{Err: codecErr}
		} else {
			out <- AsyncResult{Err: fmt.Errorf("cannot resolve: %q", resolvablePath.String())}
		}

		close(out)
		return out
	}

	resCh := res.resolveOnceAsync(ctx, resolvablePath, options)
	var best AsyncResult
	go func() {
		defer close(out)
		for {
			select {
			case res, ok := <-resCh:
				if !ok {
					if best != (AsyncResult{}) {
						ns.cacheSet(resolvablePath.String(), best.Path, best.TTL, best.LastMod)
					}
					return
				}

				if res.Err == nil {
					best = res
				}

				p, err := joinPaths(res.Path, p)
				if err != nil {
					// res.Err may already be defined, so just combine them
					res.Err = multierr.Combine(err, res.Err)
				}

				emitOnceResult(ctx, out, AsyncResult{Path: p, TTL: res.TTL, LastMod: res.LastMod, Err: res.Err})
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func emitOnceResult(ctx context.Context, outCh chan<- AsyncResult, r AsyncResult) {
	select {
	case outCh <- r:
	case <-ctx.Done():
	}
}

// Publish implements Publisher
func (ns *namesys) Publish(ctx context.Context, name ci.PrivKey, value path.Path, options ...PublishOption) error {
	ctx, span := startSpan(ctx, "namesys.Publish")
	defer span.End()

	// This is a bit hacky. We do this because the EOL is based on the current
	// time, but also needed in the end of the function. Therefore, we parse
	// the options immediately and add an option PublishWithEOL with the EOL
	// calculated in this moment.
	publishOpts := ProcessPublishOptions(options)
	options = append(options, PublishWithEOL(publishOpts.EOL))

	pid, err := peer.IDFromPrivateKey(name)
	if err != nil {
		span.RecordError(err)
		return err
	}

	ipnsName := ipns.NameFromPeer(pid)
	cacheKey := ipnsName.String()

	span.SetAttributes(attribute.String("ID", pid.String()))
	if err := ns.ipnsPublisher.Publish(ctx, name, value, options...); err != nil {
		// Invalidate the cache. Publishing may _partially_ succeed but
		// still return an error.
		ns.cacheInvalidate(cacheKey)
		span.RecordError(err)
		return err
	}

	ttl := DefaultResolverCacheTTL
	if publishOpts.TTL >= 0 {
		ttl = publishOpts.TTL
	}
	if ttEOL := time.Until(publishOpts.EOL); ttEOL < ttl {
		ttl = ttEOL
	}
	ns.cacheSet(cacheKey, value, ttl, time.Now())
	return nil
}

// Resolve is an utility function that takes a [NameSystem] and a [path.Path], and
// returns the result of [NameSystem.Resolve] for the given path. If the given namesys
// is nil, [ErrNoNamesys] is returned.
func Resolve(ctx context.Context, ns NameSystem, p path.Path) (Result, error) {
	ctx, span := startSpan(ctx, "Resolve", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	if ns == nil {
		return Result{}, ErrNoNamesys
	}

	return ns.Resolve(ctx, p)
}
