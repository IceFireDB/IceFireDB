package namesys

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	logging "github.com/ipfs/go-log/v2"
	ci "github.com/libp2p/go-libp2p/core/crypto"
)

var log = logging.Logger("namesys")

var (
	// ErrResolveFailed signals an error when attempting to resolve.
	ErrResolveFailed = errors.New("could not resolve name")

	// ErrResolveRecursion signals a recursion-depth limit.
	ErrResolveRecursion = errors.New("could not resolve name (recursion limit exceeded)")

	// ErrNoNamesys is an explicit error for when no [NameSystem] is provided.
	ErrNoNamesys = errors.New("no namesys has been provided")

	// ErrMultipleDNSLinkRecords signals that the domain had multiple valid DNSLink TXT entries.
	ErrMultipleDNSLinkRecords = fmt.Errorf("%w: DNSLink lookup returned more than one IPFS content path; ask domain owner to remove duplicate TXT records", ErrResolveFailed)

	// ErrMissingDNSLinkRecord signals that the domain has no DNSLink TXT entries.
	ErrMissingDNSLinkRecord = fmt.Errorf("%w: DNSLink lookup could not find a TXT record (https://docs.ipfs.tech/concepts/dnslink/)", ErrResolveFailed)
)

const (
	// DefaultDepthLimit is the default depth limit used by [Resolver].
	DefaultDepthLimit = 32

	// UnlimitedDepth allows infinite recursion in [Resolver]. You probably don't want
	// to use this, but it's here if you absolutely trust resolution to eventually
	// complete and can't put an upper limit on how many steps it will take.
	UnlimitedDepth = 0

	// DefaultResolverRecordCount is the number of IPNS Record copies to
	// retrieve from the routing system like Amino DHT (the best record is
	// selected from this set).
	DefaultResolverDhtRecordCount = 16

	// DefaultResolverDhtTimeout is the amount of time to wait for records to be fetched
	// and verified.
	DefaultResolverDhtTimeout = time.Minute

	// DefaultResolverCacheTTL defines default TTL of a record placed in [NameSystem] cache.
	DefaultResolverCacheTTL = time.Minute
)

// NameSystem represents a cohesive name publishing and resolving system.
//
// Publishing a name is the process of establishing a mapping, a key-value
// pair, according to naming rules and databases.
//
// Resolving a name is the process of looking up the value associated with the
// key (name).
type NameSystem interface {
	Resolver
	Publisher
}

// Result is the return type for [Resolver.Resolve].
type Result struct {
	Path    path.Path
	TTL     time.Duration
	LastMod time.Time
}

// AsyncResult is the return type for [Resolver.ResolveAsync].
type AsyncResult struct {
	Path    path.Path
	TTL     time.Duration
	LastMod time.Time
	Err     error
}

// Resolver is an object capable of resolving names.
type Resolver interface {
	// Resolve performs a recursive lookup, returning the dereferenced path and the TTL.
	// If the TTL is unknown, then a TTL of 0 is returned. For example, if example.com
	// has a DNS TXT record pointing to:
	//
	//   /ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy
	//
	// and there is a DHT IPNS entry for
	//
	//   QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy
	//   -> /ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj
	//
	// then
	//
	//   Resolve(ctx, "/ipns/ipfs.io")
	//
	// will resolve both names, returning
	//
	//   /ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj
	//
	// There is a default depth-limit to avoid infinite recursion. Most users will be fine with
	// this default limit, but if you need to adjust the limit you can specify it as an option.
	Resolve(context.Context, path.Path, ...ResolveOption) (Result, error)

	// ResolveAsync performs recursive name lookup, like Resolve, but it returns entries as
	// they are discovered in the DHT. Each returned result is guaranteed to be "better"
	// (which usually means newer) than the previous one.
	ResolveAsync(context.Context, path.Path, ...ResolveOption) <-chan AsyncResult
}

// ResolveOptions specifies options for resolving an IPNS Path.
type ResolveOptions struct {
	// Depth is the recursion depth limit.
	Depth uint

	// DhtRecordCount is the number of IPNS Records to retrieve from the routing system
	// (the best record is selected from this set).
	DhtRecordCount uint

	// DhtTimeout is the amount of time to wait for records to be fetched and
	// verified. A zero value indicates that there is no explicit timeout
	// (although there may be an implicit timeout due to dial timeouts within
	// the specific routing system like DHT).
	DhtTimeout time.Duration
}

// DefaultResolveOptions returns the default options for resolving an IPNS Path.
func DefaultResolveOptions() ResolveOptions {
	return ResolveOptions{
		Depth:          DefaultDepthLimit,
		DhtRecordCount: DefaultResolverDhtRecordCount,
		DhtTimeout:     DefaultResolverDhtTimeout,
	}
}

// ResolveOption is used to set a resolve option.
type ResolveOption func(*ResolveOptions)

// ResolveWithDepth sets [ResolveOptions.Depth].
func ResolveWithDepth(depth uint) ResolveOption {
	return func(o *ResolveOptions) {
		o.Depth = depth
	}
}

// ResolveWithDhtRecordCount sets [ResolveOptions.DhtRecordCount].
func ResolveWithDhtRecordCount(count uint) ResolveOption {
	return func(o *ResolveOptions) {
		o.DhtRecordCount = count
	}
}

// ResolveWithDhtTimeout sets [ResolveOptions.ResolveWithDhtTimeout].
func ResolveWithDhtTimeout(timeout time.Duration) ResolveOption {
	return func(o *ResolveOptions) {
		o.DhtTimeout = timeout
	}
}

// ProcessResolveOptions converts an array of [ResolveOption] into a [ResolveOptions] object.
func ProcessResolveOptions(opts []ResolveOption) ResolveOptions {
	resolveOptions := DefaultResolveOptions()
	for _, option := range opts {
		option(&resolveOptions)
	}
	return resolveOptions
}

// Publisher is an object capable of publishing particular names.
type Publisher interface {
	// Publish publishes the given value under the name represented by the given private key.
	Publish(ctx context.Context, sk ci.PrivKey, value path.Path, options ...PublishOption) error
}

// PublishOptions specifies options for publishing an IPNS Record.
type PublishOptions struct {
	// EOL defines for how long the published value is valid.
	EOL time.Time

	// TTL defines for how long the published value is cached locally before checking for updates.
	TTL time.Duration

	// IPNSOptions are options passed by [IPNSPublisher] to [ipns.NewRecord] when
	// creating a new record to publish. With this options, you can further customize
	// the way IPNS Records are created.
	IPNSOptions []ipns.Option
}

// DefaultPublishOptions returns the default options for publishing an IPNS Record.
func DefaultPublishOptions() PublishOptions {
	return PublishOptions{
		EOL: time.Now().Add(ipns.DefaultRecordLifetime),
		TTL: ipns.DefaultRecordTTL,
	}
}

// PublishOption is used to set an option for [PublishOptions].
type PublishOption func(*PublishOptions)

// PublishWithEOL sets [PublishOptions.EOL].
func PublishWithEOL(eol time.Time) PublishOption {
	return func(o *PublishOptions) {
		o.EOL = eol
	}
}

// PublishWithEOL sets [PublishOptions.TTL].
func PublishWithTTL(ttl time.Duration) PublishOption {
	return func(o *PublishOptions) {
		o.TTL = ttl
	}
}

// PublishWithIPNSOption adds an [ipns.Option] to [PublishOptions.IPNSOptions].
// These options are used by [IPNSPublisher], which passes them onto the IPNS
// record creation at [ipns.NewRecord]
func PublishWithIPNSOption(option ipns.Option) PublishOption {
	return func(o *PublishOptions) {
		o.IPNSOptions = append(o.IPNSOptions, option)
	}
}

// ProcessPublishOptions converts an array of [PublishOption] into a [PublishOptions] object.
func ProcessPublishOptions(opts []PublishOption) PublishOptions {
	publishOptions := DefaultPublishOptions()
	for _, option := range opts {
		option(&publishOptions)
	}
	return publishOptions
}
