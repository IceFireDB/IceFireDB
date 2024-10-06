package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/gateway/assets"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
)

// Config is the configuration used when creating a new gateway handler.
type Config struct {
	// DeserializedResponses configures this gateway to support returning data
	// in deserialized format. By default, the gateway will only support
	// trustless, verifiable [application/vnd.ipld.raw] and
	// [application/vnd.ipld.car] responses, operating as a [Trustless Gateway].
	//
	// This global flag can be overridden per FQDN in PublicGateways map.
	//
	// [application/vnd.ipld.raw]: https://www.iana.org/assignments/media-types/application/vnd.ipld.raw
	// [application/vnd.ipld.car]: https://www.iana.org/assignments/media-types/application/vnd.ipld.car
	// [Trustless Gateway]: https://specs.ipfs.tech/http-gateways/trustless-gateway/
	DeserializedResponses bool

	// NoDNSLink configures the gateway to _not_ perform DNS TXT record lookups in
	// response to requests with values in `Host` HTTP header. This flag can be
	// overridden per FQDN in PublicGateways. To be used with WithHostname.
	NoDNSLink bool

	// DisableHTMLErrors disables pretty HTML pages when an error occurs. Instead, a `text/plain`
	// page will be sent with the raw error message. This can be useful if this gateway
	// is being proxied by other service, which wants to use the error message.
	DisableHTMLErrors bool

	// PublicGateways configures the behavior of known public gateways. Each key is
	// a fully qualified domain name (FQDN). To be used with WithHostname.
	PublicGateways map[string]*PublicGateway

	// Menu adds items to the gateway menu that are shown in pages, such as
	// directory listings, DAG previews and errors. These will be displayed to the
	// right of "About IPFS" and "Install IPFS".
	Menu []assets.MenuItem
}

// PublicGateway is the specification of an IPFS Public Gateway.
type PublicGateway struct {
	// Paths is explicit list of path prefixes that should be handled by
	// this gateway. Example: `["/ipfs", "/ipns"]`
	// Useful if you only want to support immutable `/ipfs`.
	Paths []string

	// UseSubdomains indicates whether or not this is a [Subdomain Gateway].
	//
	// If this flag is set, any `/ipns/$id` and/or `/ipfs/$id` paths in Paths
	// will be permanently redirected to `http(s)://$id.[ipns|ipfs].$gateway/`.
	//
	// We do not support using both paths and subdomains for a single domain
	// for security reasons ([Origin isolation]).
	//
	// [Subdomain Gateway]: https://specs.ipfs.tech/http-gateways/subdomain-gateway/
	// [Origin isolation]: https://en.wikipedia.org/wiki/Same-origin_policy
	UseSubdomains bool

	// NoDNSLink configures this gateway to _not_ resolve DNSLink for the
	// specific FQDN provided in `Host` HTTP header. Useful when you want to
	// explicitly allow or refuse hosting a single hostname. To refuse all
	// DNSLinks in `Host` processing, set NoDNSLink in Config instead. This setting
	// overrides the global setting.
	NoDNSLink bool

	// InlineDNSLink configures this gateway to always inline DNSLink names
	// (FQDN) into a single DNS label in order to interop with wildcard TLS certs
	// and Origin per CID isolation provided by rules like https://publicsuffix.org
	//
	// This should be set to true if you use HTTPS.
	InlineDNSLink bool

	// DeserializedResponses configures this gateway to support returning data
	// in deserialized format. This setting overrides the global setting.
	DeserializedResponses bool
}

type CarParams struct {
	Range      *DagByteRange
	Scope      DagScope
	Order      DagOrder
	Duplicates DuplicateBlocksPolicy
}

// DagByteRange describes a range request within a UnixFS file. "From" and
// "To" mostly follow the [HTTP Byte Range] Request semantics:
//
//   - From >= 0 and To = nil: Get the file (From, Length)
//   - From >= 0 and To >= 0: Get the range (From, To)
//   - From >= 0 and To <0: Get the range (From, Length - To)
//   - From < 0 and To = nil: Get the file (Length - From, Length)
//   - From < 0 and To >= 0: Get the range (Length - From, To)
//   - From < 0 and To <0: Get the range (Length - From, Length - To)
//
// [HTTP Byte Range]: https://httpwg.org/specs/rfc9110.html#rfc.section.14.1.2
type DagByteRange struct {
	From int64
	To   *int64
}

func NewDagByteRange(rangeStr string) (DagByteRange, error) {
	rangeElems := strings.Split(rangeStr, ":")
	if len(rangeElems) != 2 {
		return DagByteRange{}, errors.New("range must have two numbers separated with ':'")
	}
	from, err := strconv.ParseInt(rangeElems[0], 10, 64)
	if err != nil {
		return DagByteRange{}, err
	}

	if rangeElems[1] == "*" {
		return DagByteRange{
			From: from,
			To:   nil,
		}, nil
	}

	to, err := strconv.ParseInt(rangeElems[1], 10, 64)
	if err != nil {
		return DagByteRange{}, err
	}

	if from >= 0 && to >= 0 && from > to {
		return DagByteRange{}, errors.New("cannot have an entity-bytes range where 'from' is after 'to'")
	}

	if from < 0 && to < 0 && from > to {
		return DagByteRange{}, errors.New("cannot have an entity-bytes range where 'from' is after 'to'")
	}

	return DagByteRange{
		From: from,
		To:   &to,
	}, nil
}

// DagScope describes the scope of the requested DAG, as per the [Trustless Gateway]
// specification.
//
// [Trustless Gateway]: https://specs.ipfs.tech/http-gateways/trustless-gateway/
type DagScope string

const (
	DagScopeAll    DagScope = "all"
	DagScopeEntity DagScope = "entity"
	DagScopeBlock  DagScope = "block"
)

type DagOrder string

const (
	DagOrderUnspecified DagOrder = ""
	DagOrderUnknown     DagOrder = "unk"
	DagOrderDFS         DagOrder = "dfs"
)

// DuplicateBlocksPolicy represents the content type parameter 'dups' (IPIP-412)
type DuplicateBlocksPolicy uint8

const (
	DuplicateBlocksUnspecified DuplicateBlocksPolicy = iota // 0 - implicit default
	DuplicateBlocksIncluded                                 // 1 - explicitly include duplicates
	DuplicateBlocksExcluded                                 // 2 - explicitly NOT include duplicates
)

// NewDuplicateBlocksPolicy returns DuplicateBlocksPolicy based on the content type parameter 'dups' (IPIP-412)
func NewDuplicateBlocksPolicy(dupsValue string) (DuplicateBlocksPolicy, error) {
	switch dupsValue {
	case "y":
		return DuplicateBlocksIncluded, nil
	case "n":
		return DuplicateBlocksExcluded, nil
	case "":
		return DuplicateBlocksUnspecified, nil
	}
	return 0, fmt.Errorf("unsupported application/vnd.ipld.car content type dups parameter: %q", dupsValue)
}

func (d DuplicateBlocksPolicy) Bool() bool {
	// duplicates should be returned only when explicitly requested,
	// so any other state than DuplicateBlocksIncluded should return false
	return d == DuplicateBlocksIncluded
}

func (d DuplicateBlocksPolicy) String() string {
	switch d {
	case DuplicateBlocksIncluded:
		return "y"
	case DuplicateBlocksExcluded:
		return "n"
	}
	return ""
}

type ContentPathMetadata struct {
	PathSegmentRoots     []cid.Cid
	LastSegment          path.ImmutablePath
	LastSegmentRemainder []string
	ContentType          string    // Only used for UnixFS requests
	ModTime              time.Time // Optional, non-zero values may be present in UnixFS 1.5 DAGs
}

// ByteRange describes a range request within a UnixFS file. "From" and "To" mostly
// follow [HTTP Byte Range] Request semantics:
//
//   - From >= 0 and To = nil: Get the file (From, Length)
//   - From >= 0 and To >= 0: Get the range (From, To)
//   - From >= 0 and To <0: Get the range (From, Length - To)
//
// [HTTP Byte Range]: https://httpwg.org/specs/rfc9110.html#rfc.section.14.1.2
type ByteRange struct {
	From uint64
	To   *int64
}

type GetResponse struct {
	bytes             io.ReadCloser
	bytesSize         int64
	symlink           *files.Symlink
	directoryMetadata *directoryMetadata
}

func (r *GetResponse) Close() error {
	if r.bytes != nil {
		return r.bytes.Close()
	}
	if r.symlink != nil {
		return r.symlink.Close()
	}
	if r.directoryMetadata != nil {
		if r.directoryMetadata.closeFn == nil {
			return nil
		}
		return r.directoryMetadata.closeFn()
	}
	// Should be unreachable
	return nil
}

var _ io.Closer = (*GetResponse)(nil)

type directoryMetadata struct {
	dagSize uint64
	entries <-chan unixfs.LinkResult
	closeFn func() error
}

func NewGetResponseFromReader(file io.ReadCloser, fullFileSize int64) *GetResponse {
	return &GetResponse{bytes: file, bytesSize: fullFileSize}
}

func NewGetResponseFromSymlink(symlink *files.Symlink, size int64) *GetResponse {
	return &GetResponse{symlink: symlink, bytesSize: size}
}

func NewGetResponseFromDirectoryListing(dagSize uint64, entries <-chan unixfs.LinkResult, closeFn func() error) *GetResponse {
	return &GetResponse{directoryMetadata: &directoryMetadata{dagSize: dagSize, entries: entries, closeFn: closeFn}}
}

type HeadResponse struct {
	bytesSize     int64
	startingBytes io.ReadCloser
	isFile        bool
	isSymLink     bool
	isDir         bool
}

func (r *HeadResponse) Close() error {
	if r.startingBytes != nil {
		return r.startingBytes.Close()
	}
	return nil
}

func NewHeadResponseForFile(startingBytes io.ReadCloser, size int64) *HeadResponse {
	return &HeadResponse{startingBytes: startingBytes, isFile: true, bytesSize: size}
}

func NewHeadResponseForSymlink(symlinkSize int64) *HeadResponse {
	return &HeadResponse{isSymLink: true, bytesSize: symlinkSize}
}

func NewHeadResponseForDirectory(dagSize int64) *HeadResponse {
	return &HeadResponse{isDir: true, bytesSize: dagSize}
}

// IPFSBackend is the required set of functionality used to implement the IPFS
// [HTTP Gateway] specification.
//
// The error returned by the implementer influences the status code that the user
// receives. By default, the gateway is able to handle a few error types, such as
// [context.DeadlineExceeded], [cid.ErrInvalidCid] and various IPLD-pathing related
// errors.
//
// To signal custom error types to the gateway, such that not everything is an
// 500 Internal Server Error, implementers can return errors wrapped in either
// [ErrorRetryAfter] or [ErrorStatusCode].
//
// [HTTP Gateway]: https://specs.ipfs.tech/http-gateways/
type IPFSBackend interface {
	// Get returns a [GetResponse] with UnixFS file, directory or a block in IPLD
	// format, e.g. (DAG-)CBOR/JSON.
	//
	// Returned Directories are preferably a minimum info required for enumeration: Name, Size, and Cid.
	//
	// Optional ranges follow [HTTP Byte Ranges] notation and can be used for
	// pre-fetching specific sections of a file or a block.
	//
	// Range notes:
	//   - Generating response to a range request may require additional data
	//     beyond the passed ranges (e.g. a single byte range from the middle of a
	//     file will still need magic bytes from the very beginning for content
	//     type sniffing).
	//   - A range request for a directory currently holds no semantic meaning.
	//   - For non-UnixFS (and non-raw data) such as terminal IPLD dag-cbor/json, etc. blocks the returned response
	//     bytes should be the complete block and returned as an [io.ReadSeekCloser] starting at the beginning of the
	//     block rather than as an [io.ReadCloser] that starts at the beginning of the range request.
	//
	// [HTTP Byte Ranges]: https://httpwg.org/specs/rfc9110.html#rfc.section.14.1.2
	Get(context.Context, path.ImmutablePath, ...ByteRange) (ContentPathMetadata, *GetResponse, error)

	// GetAll returns a UnixFS file or directory depending on what the path is that has been requested. Directories should
	// include all content recursively.
	GetAll(context.Context, path.ImmutablePath) (ContentPathMetadata, files.Node, error)

	// GetBlock returns a single block of data
	GetBlock(context.Context, path.ImmutablePath) (ContentPathMetadata, files.File, error)

	// Head returns a [HeadResponse] depending on what the path is that has been requested.
	// For UnixFS files (and raw blocks) should return the size of the file and either set the ContentType in
	// ContentPathMetadata or send back a reader from the beginning of the file with enough data (e.g. 3kiB) such that
	// the content type can be determined by sniffing.
	//
	// For UnixFS directories and symlinks only setting the size and type are necessary.
	//
	// For all other data types (e.g. (DAG-)CBOR/JSON blocks) returning the size information as a file while setting
	// the content-type is sufficient.
	Head(context.Context, path.ImmutablePath) (ContentPathMetadata, *HeadResponse, error)

	// ResolvePath resolves the path using UnixFS resolver. If the path does not
	// exist due to a missing link, it should return an error of type:
	// NewErrorResponse(fmt.Errorf("no link named %q under %s", name, cid), http.StatusNotFound)
	ResolvePath(context.Context, path.ImmutablePath) (ContentPathMetadata, error)

	// GetCAR returns a CAR file for the given immutable path. It returns an error
	// if there was an issue before the CAR streaming begins.
	GetCAR(context.Context, path.ImmutablePath, CarParams) (ContentPathMetadata, io.ReadCloser, error)

	// IsCached returns whether or not the path exists locally.
	IsCached(context.Context, path.Path) bool

	// GetIPNSRecord retrieves the best IPNS record for a given CID (libp2p-key)
	// from the routing system.
	GetIPNSRecord(context.Context, cid.Cid) ([]byte, error)

	// ResolveMutable takes a mutable path and resolves it into an immutable one. This means recursively resolving any
	// DNSLink or IPNS records. It should also return a TTL. If the TTL is unknown, 0 should be returned.
	//
	// For example, given a mapping from `/ipns/dnslink.tld -> /ipns/ipns-id/mydirectory` and `/ipns/ipns-id` to
	// `/ipfs/some-cid`, the result of passing `/ipns/dnslink.tld/myfile` would be `/ipfs/some-cid/mydirectory/myfile`.
	ResolveMutable(context.Context, path.Path) (path.ImmutablePath, time.Duration, time.Time, error)

	// GetDNSLinkRecord returns the DNSLink TXT record for the provided FQDN.
	// Unlike ResolvePath, it does not perform recursive resolution. It only
	// checks for the existence of a DNSLink TXT record with path starting with
	// /ipfs/ or /ipns/ and returns the path as-is.
	GetDNSLinkRecord(context.Context, string) (path.Path, error)
}

// WithContextHint allows an [IPFSBackend] to inject custom [context.Context] configurations.
// This should be considered optional, consumers might only make a best effort attempt at calling WrapContextForRequest on requests.
type WithContextHint interface {
	// WrapContextForRequest allows the backend to add request scopped modifications to the context, like debug values or value caches.
	// There are no promises on actual usage in consumers.
	WrapContextForRequest(context.Context) context.Context
}

// RequestContextKey is a type representing a [context.Context] value key.
type RequestContextKey string

const (
	// GatewayHostnameKey is the key for the hostname at which the gateway is
	// operating. It may be a DNSLink, Subdomain or Regular gateway.
	GatewayHostnameKey RequestContextKey = "gw-hostname"

	// DNSLinkHostnameKey is the key for the hostname of a [DNSLink Gateway].
	//
	// [DNSLink Gateway]: https://specs.ipfs.tech/http-gateways/dnslink-gateway/
	DNSLinkHostnameKey RequestContextKey = "dnslink-hostname"

	// SubdomainHostnameKey is the key for the hostname of a [Subdomain Gateway].
	//
	// [Subdomain Gateway]: https://specs.ipfs.tech/http-gateways/subdomain-gateway/
	SubdomainHostnameKey RequestContextKey = "subdomain-hostname"

	// OriginalPathKey is the key for the original [http.Request] [url.URL.Path],
	// as a string. This is the original path of the request, before [NewHostnameHandler].
	OriginalPathKey RequestContextKey = "original-path-key"

	// ContentPathKey is the key for the content [path.Path] of the current request.
	// This already accounts with changes made with [NewHostnameHandler].
	ContentPathKey RequestContextKey = "content-path"
)
