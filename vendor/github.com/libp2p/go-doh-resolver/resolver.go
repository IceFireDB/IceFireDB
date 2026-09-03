package doh

import (
	"context"
	"errors"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"

	madns "github.com/multiformats/go-multiaddr-dns"
)

type Resolver struct {
	mx  sync.Mutex
	url string

	// RR cache
	ipCache     map[string]ipAddrEntry
	txtCache    map[string]txtEntry
	maxCacheTTL time.Duration
}

type ipAddrEntry struct {
	ips    []net.IPAddr
	expire time.Time
}

type txtEntry struct {
	txt    []string
	expire time.Time
}

type Option func(*Resolver) error

// Specifies the maximum time entries are valid in the cache
// A maxCacheTTL of zero or less is equivalent to `WithCacheDisabled`
func WithMaxCacheTTL(maxCacheTTL time.Duration) Option {
	return func(tr *Resolver) error {
		tr.maxCacheTTL = max(0, maxCacheTTL)
		return nil
	}
}

func WithCacheDisabled() Option {
	return func(tr *Resolver) error {
		tr.maxCacheTTL = 0
		return nil
	}
}

func NewResolver(url string, opts ...Option) (*Resolver, error) {
	if strings.HasPrefix(url, "http:") &&
		!strings.HasPrefix(url, "http://localhost") &&
		!strings.HasPrefix(url, "http://127.0.0.1") &&
		!strings.HasPrefix(url, "http://[::1]") {
		return nil, errors.New("insecure URL: non-local DoH resolvers must use HTTPS")
	}

	if !strings.HasPrefix(url, "http:") && !strings.HasPrefix(url, "https:") {
		url = "https://" + url
	}

	r := &Resolver{
		url:         url,
		ipCache:     make(map[string]ipAddrEntry),
		txtCache:    make(map[string]txtEntry),
		maxCacheTTL: time.Duration(math.MaxUint32) * time.Second,
	}

	for _, o := range opts {
		if err := o(r); err != nil {
			return nil, err
		}
	}

	return r, nil
}

var _ madns.BasicResolver = (*Resolver)(nil)

// Consumers detect TXT TTL support through this optional interface at runtime,
// so a signature drift would silently disable TTL reporting downstream; the
// assertion makes it a compile error instead.
var _ madns.TXTWithTTLResolver = (*Resolver)(nil)

func (r *Resolver) LookupIPAddr(ctx context.Context, domain string) (result []net.IPAddr, err error) {
	result, ok := r.getCachedIPAddr(domain)
	if ok {
		return result, nil
	}

	type response struct {
		ips []net.IPAddr
		ttl uint32
		err error
	}

	resch := make(chan response, 2)
	go func() {
		ip4, ttl, err := doRequestA(ctx, r.url, domain)
		resch <- response{ip4, ttl, err}
	}()

	go func() {
		ip6, ttl, err := doRequestAAAA(ctx, r.url, domain)
		resch <- response{ip6, ttl, err}
	}()

	var ttl uint32
	first := true
	for range 2 {
		r := <-resch
		if r.err != nil {
			return nil, r.err
		}

		result = append(result, r.ips...)
		// The combined TTL is the lowest across the A and AAAA answers that
		// carried records; an empty answer has no RRset TTL to contribute, so
		// its placeholder 0 must not zero out the other family's TTL. A
		// genuine 0 from an existing RRset (do not cache) still wins.
		if len(r.ips) > 0 && (first || r.ttl < ttl) {
			ttl = r.ttl
			first = false
		}
	}

	cacheTTL := minTTL(time.Duration(ttl)*time.Second, r.maxCacheTTL)
	r.cacheIPAddr(domain, result, cacheTTL)
	return result, nil
}

func (r *Resolver) LookupTXT(ctx context.Context, domain string) ([]string, error) {
	result, _, err := r.LookupTXTWithTTL(ctx, domain)
	return result, err
}

// LookupTXTWithTTL is like [Resolver.LookupTXT] but also returns how long the
// TXT records may be cached. The TTL is the smallest Ttl across the answer's
// TXT resource records, capped by the resolver's max cache TTL ([WithMaxCacheTTL]).
// On a cache hit it is the remaining lifetime of the cached entry, so the value
// shrinks as the entry ages. A TTL of 0 means the records may not be cached:
// because the cache is disabled ([WithCacheDisabled]), the records themselves
// carry a TTL of 0, or the upstream resolver did not provide one.
func (r *Resolver) LookupTXTWithTTL(ctx context.Context, domain string) ([]string, time.Duration, error) {
	if result, ttl, ok := r.getCachedTXTWithTTL(domain); ok {
		return result, ttl, nil
	}

	result, ttl, err := doRequestTXT(ctx, r.url, domain)
	if err != nil {
		return nil, 0, err
	}

	cacheTTL := minTTL(time.Duration(ttl)*time.Second, r.maxCacheTTL)
	r.cacheTXT(domain, result, cacheTTL)
	return result, cacheTTL, nil
}

func (r *Resolver) getCachedIPAddr(domain string) ([]net.IPAddr, bool) {
	r.mx.Lock()
	defer r.mx.Unlock()

	fqdn := dns.Fqdn(domain)
	entry, ok := r.ipCache[fqdn]
	if !ok {
		return nil, false
	}

	if time.Now().After(entry.expire) {
		delete(r.ipCache, fqdn)
		return nil, false
	}

	return entry.ips, true
}

func (r *Resolver) cacheIPAddr(domain string, ips []net.IPAddr, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	r.mx.Lock()
	defer r.mx.Unlock()

	fqdn := dns.Fqdn(domain)
	r.ipCache[fqdn] = ipAddrEntry{ips, time.Now().Add(ttl)}
}

func (r *Resolver) getCachedTXT(domain string) ([]string, bool) {
	txt, _, ok := r.getCachedTXTWithTTL(domain)
	return txt, ok
}

func (r *Resolver) getCachedTXTWithTTL(domain string) ([]string, time.Duration, bool) {
	r.mx.Lock()
	defer r.mx.Unlock()

	fqdn := dns.Fqdn(domain)
	entry, ok := r.txtCache[fqdn]
	if !ok {
		return nil, 0, false
	}

	// Read the clock once: a second read after an expiry check could land
	// past the deadline and report a negative TTL.
	remaining := time.Until(entry.expire)
	if remaining <= 0 {
		delete(r.txtCache, fqdn)
		return nil, 0, false
	}

	return entry.txt, remaining, true
}

func (r *Resolver) cacheTXT(domain string, txt []string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	r.mx.Lock()
	defer r.mx.Unlock()

	fqdn := dns.Fqdn(domain)
	r.txtCache[fqdn] = txtEntry{txt, time.Now().Add(ttl)}
}

func minTTL(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
