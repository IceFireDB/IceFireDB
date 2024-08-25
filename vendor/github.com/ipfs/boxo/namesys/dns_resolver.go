package namesys

import (
	"context"
	"errors"
	"fmt"
	"net"
	gopath "path"
	"strings"
	"time"

	path "github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	dns "github.com/miekg/dns"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// LookupTXTFunc is a function that lookups TXT record values.
type LookupTXTFunc func(ctx context.Context, name string) (txt []string, err error)

// DNSResolver implements [Resolver] on DNS domains.
type DNSResolver struct {
	lookupTXT LookupTXTFunc
}

var _ Resolver = &DNSResolver{}

// NewDNSResolver constructs a name resolver using DNS TXT records.
func NewDNSResolver(lookup LookupTXTFunc) *DNSResolver {
	return &DNSResolver{lookupTXT: lookup}
}

func (r *DNSResolver) Resolve(ctx context.Context, p path.Path, options ...ResolveOption) (Result, error) {
	ctx, span := startSpan(ctx, "DNSResolver.Resolve", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	return resolve(ctx, r, p, ProcessResolveOptions(options))
}

func (r *DNSResolver) ResolveAsync(ctx context.Context, p path.Path, options ...ResolveOption) <-chan AsyncResult {
	ctx, span := startSpan(ctx, "DNSResolver.ResolveAsync", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	return resolveAsync(ctx, r, p, ProcessResolveOptions(options))
}

func (r *DNSResolver) resolveOnceAsync(ctx context.Context, p path.Path, options ResolveOptions) <-chan AsyncResult {
	ctx, span := startSpan(ctx, "DNSResolver.ResolveOnceAsync", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	out := make(chan AsyncResult, 1)
	if p.Namespace() != path.IPNSNamespace {
		out <- AsyncResult{Err: fmt.Errorf("unsupported namespace: %q", p.Namespace())}
		close(out)
		return out
	}

	fqdn := p.Segments()[1]
	if _, ok := dns.IsDomainName(fqdn); !ok {
		out <- AsyncResult{Err: fmt.Errorf("not a valid domain name: %q", fqdn)}
		close(out)
		return out
	}

	log.Debugf("DNSResolver resolving %q", fqdn)

	if !strings.HasSuffix(fqdn, ".") {
		fqdn += "."
	}

	resChan := make(chan AsyncResult, 1)
	go workDomain(ctx, r, "_dnslink."+fqdn, resChan)

	go func() {
		defer close(out)
		ctx, span := startSpan(ctx, "DNSResolver.ResolveOnceAsync.Worker")
		defer span.End()

		select {
		case subRes, ok := <-resChan:
			if !ok {
				break
			}
			if subRes.Err == nil {
				p, err := joinPaths(subRes.Path, p)
				emitOnceResult(ctx, out, AsyncResult{Path: p, LastMod: time.Now(), Err: err})
				// Return without waiting for rootRes, since this result
				// (for "_dnslink."+fqdn) takes precedence
			} else {
				err := fmt.Errorf("DNSLink lookup for %q failed: %w", gopath.Base(fqdn), subRes.Err)
				emitOnceResult(ctx, out, AsyncResult{Err: err})
			}
			return
		case <-ctx.Done():
			return
		}
	}()

	return out
}

func workDomain(ctx context.Context, r *DNSResolver, name string, res chan AsyncResult) {
	ctx, span := startSpan(ctx, "DNSResolver.WorkDomain", trace.WithAttributes(attribute.String("Name", name)))
	defer span.End()

	defer close(res)

	txt, err := r.lookupTXT(ctx, name)
	if err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) {
			// If no TXT records found, return same error as when no text
			// records contain dnslink. Otherwise, return the actual error.
			if dnsErr.IsNotFound {
				err = ErrMissingDNSLinkRecord
			}
		}
		// Could not look up any text records for name
		res <- AsyncResult{Err: err}
		return
	}

	// Convert all the found TXT records into paths. Ignore invalid ones.
	var paths []path.Path
	for _, t := range txt {
		p, err := parseEntry(t)
		if err == nil {
			paths = append(paths, p)
		}
	}

	// Filter only the IPFS and IPNS paths.
	paths = lo.Filter(paths, func(item path.Path, index int) bool {
		return item.Namespace() == path.IPFSNamespace ||
			item.Namespace() == path.IPNSNamespace
	})

	switch len(paths) {
	case 0:
		// There were no TXT records with a dnslink
		res <- AsyncResult{Err: ErrMissingDNSLinkRecord}
	case 1:
		// Found 1 valid! Return it.
		res <- AsyncResult{Path: paths[0]}
	default:
		// Found more than 1 IPFS/IPNS path.
		res <- AsyncResult{Err: ErrMultipleDNSLinkRecords}
	}
}

func parseEntry(txt string) (path.Path, error) {
	p, err := path.NewPath(txt) // bare IPFS multihashes
	if err == nil {
		return p, nil
	}

	// Support legacy DNSLink entries composed by the CID only.
	if cid, err := cid.Decode(txt); err == nil {
		return path.FromCid(cid), nil
	}

	return tryParseDNSLink(txt)
}

func tryParseDNSLink(txt string) (path.Path, error) {
	parts := strings.SplitN(txt, "=", 2)
	if len(parts) == 2 && parts[0] == "dnslink" {
		p, err := path.NewPath(parts[1])
		if err == nil {
			return p, nil
		}

		// Support legacy DNSLink entries composed by "dnslink={CID}".
		if cid, err := cid.Decode(parts[1]); err == nil {
			return path.FromCid(cid), nil
		}
	}

	return nil, errors.New("not a valid dnslink entry")
}
