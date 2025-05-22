package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/textproto"
	"net/url"
	gopath "path"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	"github.com/ipfs/boxo/gateway/assets"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
	mc "github.com/multiformats/go-multicodec"
	prometheus "github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

var log = logging.Logger("boxo/gateway")

const (
	ipfsPathPrefix        = "/ipfs/"
	ipnsPathPrefix        = ipns.NamespacePrefix
	immutableCacheControl = "public, max-age=29030400, immutable"
)

var (
	onlyASCII = regexp.MustCompile("[[:^ascii:]]")
	noModtime = time.Unix(0, 0) // disables Last-Modified header if passed as modtime
)

// handler is a HTTP handler that serves IPFS objects (accessible by default at /ipfs/<path>)
// (it serves requests like GET /ipfs/QmVRzPKPzNtSrEzBFm2UZfxmPAgnaLke4DMcerbsGGSaFe/link)
type handler struct {
	config  *Config
	backend IPFSBackend

	// response type metrics
	requestTypeMetric            *prometheus.CounterVec
	getMetric                    *prometheus.HistogramVec
	unixfsFileGetMetric          *prometheus.HistogramVec
	unixfsDirIndexGetMetric      *prometheus.HistogramVec
	unixfsGenDirListingGetMetric *prometheus.HistogramVec
	carStreamGetMetric           *prometheus.HistogramVec
	carStreamFailMetric          *prometheus.HistogramVec
	rawBlockGetMetric            *prometheus.HistogramVec
	tarStreamGetMetric           *prometheus.HistogramVec
	tarStreamFailMetric          *prometheus.HistogramVec
	jsoncborDocumentGetMetric    *prometheus.HistogramVec
	ipnsRecordGetMetric          *prometheus.HistogramVec
}

// NewHandler returns an [http.Handler] that provides the functionality
// of an [IPFS HTTP Gateway] based on a [Config] and [IPFSBackend].
//
// [IPFS HTTP Gateway]: https://specs.ipfs.tech/http-gateways/
func NewHandler(c Config, backend IPFSBackend) http.Handler {
	return newHandlerWithMetrics(&c, backend)
}

// serveContent replies to the request using the content in the provided Reader
// and returns the status code written and any error encountered during a write.
// It wraps httpServeContent (a close clone of http.ServeContent) which takes care of If-None-Match+Etag,
// Content-Length and range requests.
//
// Notes:
// 1. For HEAD requests the io.Reader may be nil/undefined
// 2. When the io.Reader is needed it must start at the beginning of the first Range Request component if it exists
// 3. Only a single HTTP Range Request is supported, if more than one are requested only the first will be honored
// 4. The Content-Type header must already be set
func serveContent(w http.ResponseWriter, req *http.Request, modtime time.Time, size int64, content io.Reader) (int, bool, error) {
	ew := &errRecordingResponseWriter{ResponseWriter: w}
	httpServeContent(ew, req, modtime, size, content)

	// When we calculate some metrics we want a flag that lets us to ignore
	// errors and 304 Not Modified, and only care when requested data
	// was sent in full.
	dataSent := ew.code/100 == 2 && ew.err == nil

	return ew.code, dataSent, ew.err
}

// errRecordingResponseWriter wraps a ResponseWriter to record the status code and any write error.
type errRecordingResponseWriter struct {
	http.ResponseWriter
	code int
	err  error
}

func (w *errRecordingResponseWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *errRecordingResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if err != nil && w.err == nil {
		w.err = err
	}
	return n, err
}

// ReadFrom exposes errRecordingResponseWriter's underlying ResponseWriter to io.Copy
// to allow optimized methods to be taken advantage of.
func (w *errRecordingResponseWriter) ReadFrom(r io.Reader) (n int64, err error) {
	n, err = io.Copy(w.ResponseWriter, r)
	if err != nil && w.err == nil {
		w.err = err
	}
	return n, err
}

func (i *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer panicHandler(w)

	// the hour is a hard fallback, we don't expect it to happen, but just in case
	ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
	defer cancel()

	if withCtxWrap, ok := i.backend.(WithContextHint); ok {
		ctx = withCtxWrap.WrapContextForRequest(ctx)
	}

	r = r.WithContext(ctx)

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		i.getOrHeadHandler(w, r)
		return
	case http.MethodOptions:
		i.optionsHandler(w, r)
		return
	}

	addAllowHeader(w)

	errmsg := "Method " + r.Method + " not allowed: read only access"
	http.Error(w, errmsg, http.StatusMethodNotAllowed)
}

func (i *handler) optionsHandler(w http.ResponseWriter, r *http.Request) {
	addAllowHeader(w)
	// OPTIONS is a noop request that is used by the browsers to check if server accepts
	// cross-site XMLHttpRequest, which is indicated by the presence of CORS headers:
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS#Preflighted_requests
}

// addAllowHeader sets Allow header with supported HTTP methods
func addAllowHeader(w http.ResponseWriter) {
	w.Header().Add("Allow", http.MethodGet)
	w.Header().Add("Allow", http.MethodHead)
	w.Header().Add("Allow", http.MethodOptions)
}

type requestData struct {
	// Defined for all requests.
	begin          time.Time
	logger         *zap.SugaredLogger
	contentPath    path.Path
	responseFormat string
	responseParams map[string]string

	// Defined for non IPNS Record requests.
	immutablePath path.ImmutablePath
	ttl           time.Duration
	lastMod       time.Time

	// Defined if resolution has already happened.
	pathMetadata *ContentPathMetadata
}

// mostlyResolvedPath is an opportunistic optimization that returns the mostly
// resolved version of ImmutablePath available. It does not guarantee it is fully
// resolved, nor that it is the original.
func (rq *requestData) mostlyResolvedPath() path.ImmutablePath {
	if rq.pathMetadata != nil {
		imPath, err := path.NewImmutablePath(rq.pathMetadata.LastSegment)
		if err != nil {
			// This will never happen. This error has previously been checked in
			// [handleIfNoneMatch] and the request will have returned 500.
			panic(err)
		}
		return imPath
	}
	return rq.immutablePath
}

func (i *handler) getOrHeadHandler(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()

	logger := log.With("from", r.RequestURI)
	logger.Debug("http request received")

	if handleProtocolHandlerRedirect(w, r, i.config) ||
		i.handleServiceWorkerRegistration(w, r) ||
		handleIpnsB58mhToCidRedirection(w, r) ||
		i.handleSuperfluousNamespace(w, r) {
		return
	}

	var success bool
	contentPath, err := path.NewPath(r.URL.Path)
	if err != nil {
		i.webError(w, r, err, http.StatusBadRequest)
		return
	}

	ctx := context.WithValue(r.Context(), ContentPathKey, contentPath)
	r = r.WithContext(ctx)

	defer func() {
		if success {
			i.getMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
		}
	}()

	if i.handleOnlyIfCached(w, r, contentPath) {
		return
	}

	// Detect when explicit Accept header or ?format parameter are present
	responseFormat, formatParams, err := customResponseFormat(r)
	if err != nil {
		i.webError(w, r, fmt.Errorf("error while processing the Accept header: %w", err), http.StatusBadRequest)
		return
	}
	trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("ResponseFormat", responseFormat))
	i.requestTypeMetric.WithLabelValues(contentPath.Namespace(), responseFormat).Inc()

	w.Header().Set("X-Ipfs-Path", contentPath.String())

	// Fail fast if unsupported request type was sent to a Trustless Gateway.
	if !i.isDeserializedResponsePossible(r) && !i.isTrustlessRequest(contentPath, responseFormat) {
		err := errors.New("only trustless requests are accepted on this gateway: https://specs.ipfs.tech/http-gateways/trustless-gateway/")
		i.webError(w, r, err, http.StatusNotAcceptable)
		return
	}

	rq := &requestData{
		begin:          begin,
		logger:         logger,
		contentPath:    contentPath,
		responseFormat: responseFormat,
		responseParams: formatParams,
	}

	addContentLocation(r, w, rq)

	// IPNS Record response format can be handled now, since (1) it needs the
	// non-resolved mutable path, and (2) has custom If-None-Match header handling
	// due to custom ETag.
	if responseFormat == ipnsRecordResponseFormat {
		logger.Debugw("serving ipns record", "path", contentPath)
		success = i.serveIpnsRecord(r.Context(), w, r, rq)
		return
	}

	if contentPath.Mutable() {
		rq.immutablePath, rq.ttl, rq.lastMod, err = i.backend.ResolveMutable(r.Context(), contentPath)
		if err != nil {
			err = fmt.Errorf("failed to resolve %s: %w", debugStr(contentPath.String()), err)
			i.webError(w, r, err, http.StatusInternalServerError)
			return
		}
	} else {
		rq.immutablePath, err = path.NewImmutablePath(contentPath)
		if err != nil {
			err = fmt.Errorf("path was expected to be immutable, but was not %s: %w", debugStr(contentPath.String()), err)
			i.webError(w, r, err, http.StatusInternalServerError)
			return
		}
	}

	// CAR response format can be handled now, since (1) it explicitly needs the
	// full immutable path to include in the CAR, and (2) has custom If-None-Match
	// header handling due to custom ETag.
	if responseFormat == carResponseFormat {
		logger.Debugw("serving car stream", "path", contentPath)
		success = i.serveCAR(r.Context(), w, r, rq)
		return
	}

	// Detect when If-None-Match HTTP header allows returning HTTP 304 Not Modified.
	if i.handleIfNoneMatch(w, r, rq) {
		return
	}

	// Detect when If-Modified-Since HTTP header + UnixFS 1.5 allow returning HTTP 304 Not Modified.
	if i.handleIfModifiedSince(w, r, rq) {
		return
	}

	// Support custom response formats passed via ?format or Accept HTTP header
	switch responseFormat {
	case "", jsonResponseFormat, cborResponseFormat:
		success = i.serveDefaults(r.Context(), w, r, rq)
	case rawResponseFormat:
		logger.Debugw("serving raw block", "path", contentPath)
		success = i.serveRawBlock(r.Context(), w, r, rq)
	case tarResponseFormat:
		logger.Debugw("serving tar file", "path", contentPath)
		success = i.serveTAR(r.Context(), w, r, rq)
	case dagJsonResponseFormat, dagCborResponseFormat:
		logger.Debugw("serving codec", "path", contentPath)
		success = i.serveCodec(r.Context(), w, r, rq)
	default: // catch-all for unsuported application/vnd.*
		err := fmt.Errorf("unsupported format %q", responseFormat)
		i.webError(w, r, err, http.StatusBadRequest)
	}
}

// isDeserializedResponsePossible returns true if deserialized responses
// are allowed on the specified hostname, or globally. Host-specific rules
// override global config.
func (i *handler) isDeserializedResponsePossible(r *http.Request) bool {
	// Get the value from HTTP Host header
	host := r.Host

	// If this request went through WithHostname, use the key in the context.
	if h, ok := r.Context().Value(GatewayHostnameKey).(string); ok {
		host = h
	}

	// If a reverse-proxy passed explicit hostname override
	// in the X-Forwarded-Host header, it takes precedence above everything else.
	if xHost := r.Header.Get("X-Forwarded-Host"); xHost != "" {
		host = xHost
	}

	// If the gateway is defined, return whatever is set.
	if gw, ok := i.config.PublicGateways[host]; ok {
		return gw.DeserializedResponses
	}

	// Otherwise, the default.
	return i.config.DeserializedResponses
}

// isTrustlessRequest returns true if the responseFormat and contentPath allow
// client to trustlessly verify response. Relevant response formats are defined
// in the [Trustless Gateway] spec.
//
// [Trustless Gateway]: https://specs.ipfs.tech/http-gateways/trustless-gateway/
func (i *handler) isTrustlessRequest(contentPath path.Path, responseFormat string) bool {
	// Only allow "/{#1}/{#2}"-like paths.
	trimmedPath := strings.Trim(contentPath.String(), "/")
	pathComponents := strings.Split(trimmedPath, "/")
	if responseFormat != carResponseFormat && len(pathComponents) != 2 {
		return false
	}

	if contentPath.Namespace() == path.IPNSNamespace {
		// TODO: only ipns records allowed until https://github.com/ipfs/specs/issues/369 is resolved
		if responseFormat != ipnsRecordResponseFormat {
			return false
		}

		// Only valid, cryptographically verifiable IPNS record names (no DNSLink on trustless gateways)
		if _, err := ipns.NameFromString(pathComponents[1]); err != nil {
			return false
		}

		return true
	}

	// Only valid CIDs.
	if _, err := cid.Decode(pathComponents[1]); err != nil {
		return false
	}

	switch responseFormat {
	case rawResponseFormat, carResponseFormat:
		return true
	default:
		return false
	}
}

func panicHandler(w http.ResponseWriter) {
	if r := recover(); r != nil {
		log.Error("A panic occurred in the gateway handler!")
		log.Error(r)
		debug.PrintStack()
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func addCacheControlHeaders(w http.ResponseWriter, r *http.Request, contentPath path.Path, ttl time.Duration, lastMod time.Time, cid cid.Cid, responseFormat string) (modtime time.Time) {
	// Best effort attempt to set an Etag based on the CID and response format.
	// Setting an ETag is handled separately for CARs and IPNS records.
	if etag := getEtag(r, cid, responseFormat); etag != "" {
		w.Header().Set("Etag", etag)
	}

	// Set Cache-Control and Last-Modified based on contentPath properties
	if contentPath.Mutable() {
		if ttl > 0 {
			// When we know the TTL, set the Cache-Control header and disable Last-Modified.
			w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(ttl.Seconds())))
		}

		if lastMod.IsZero() {
			// If no lastMod, set Last-Modified to the current time to leverage caching heuristics
			// built into modern browsers: https://github.com/ipfs/kubo/pull/8074#pullrequestreview-645196768
			modtime = time.Now()
		} else {
			// set Last-Modified to a meaningful value e.g. one read from dag-pb (UnixFS 1.5, mtime field)
			// or the last time DNSLink / IPNS Record was modified / resoved or cache
			modtime = lastMod
		}

	} else {
		w.Header().Set("Cache-Control", immutableCacheControl)

		if lastMod.IsZero() {
			// (noop) skip Last-Modified on immutable response
			modtime = noModtime
		} else {
			// set Last-Modified to value read from dag-pb (UnixFS 1.5, mtime field)
			modtime = lastMod
		}
	}

	return modtime
}

// addContentDispositionHeader sets the Content-Disposition header if "filename"
// URL query parameter is present. THis allows:
//
//   - Creation of HTML links that trigger "Save As.." dialog instead of being rendered by the browser
//   - Overriding the filename used when saving sub-resource assets on HTML page
//   - providing a default filename for HTTP clients when downloading direct /ipfs/CID without any subpath
func addContentDispositionHeader(w http.ResponseWriter, r *http.Request, contentPath path.Path) string {
	// URL param ?filename=cat.jpg triggers Content-Disposition: [..] filename
	// which impacts default name used in "Save As.." dialog
	name := getFilename(contentPath)
	urlFilename := r.URL.Query().Get("filename")
	if urlFilename != "" {
		disposition := "inline"
		// URL param ?download=true triggers Content-Disposition: [..] attachment
		// which skips rendering and forces "Save As.." dialog in browsers
		if r.URL.Query().Get("download") == "true" {
			disposition = "attachment"
		}
		setContentDispositionHeader(w, urlFilename, disposition)
		name = urlFilename
	}
	return name
}

func getFilename(contentPath path.Path) string {
	s := contentPath.String()
	if (strings.HasPrefix(s, ipfsPathPrefix) || strings.HasPrefix(s, ipnsPathPrefix)) && strings.Count(gopath.Clean(s), "/") <= 2 {
		// Don't want to treat ipfs.io in /ipns/ipfs.io as a filename.
		return ""
	}
	return gopath.Base(s)
}

// setContentDispositionHeader sets the Content-Disposition header to the given
// filename and disposition.
func setContentDispositionHeader(w http.ResponseWriter, filename string, disposition string) {
	utf8Name := url.PathEscape(filename)
	asciiName := url.PathEscape(onlyASCII.ReplaceAllLiteralString(filename, "_"))
	w.Header().Set("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"; filename*=UTF-8''%s", disposition, asciiName, utf8Name))
}

// setIpfsRootsHeader sets the X-Ipfs-Roots header with logical CID array for
// efficient HTTP cache invalidation.
func setIpfsRootsHeader(w http.ResponseWriter, rq *requestData, md *ContentPathMetadata) {
	// Update requestData with the latest ContentPathMetadata if it wasn't set yet.
	if rq.pathMetadata == nil {
		rq.pathMetadata = md
	}

	// These are logical roots where each CID represent one path segment
	// and resolves to either a directory or the root block of a file.
	// The main purpose of this header is allow HTTP caches to do smarter decisions
	// around cache invalidation (eg. keep specific subdirectory/file if it did not change)

	// A good example is Wikipedia, which is HAMT-sharded, but we only care about
	// logical roots that represent each segment of the human-readable content
	// path:

	// Given contentPath = /ipns/en.wikipedia-on-ipfs.org/wiki/Block_of_Wikipedia_in_Turkey
	// rootCidList is a generated by doing `ipfs resolve -r` on each sub path:
	// 	/ipns/en.wikipedia-on-ipfs.org → bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze
	// 	/ipns/en.wikipedia-on-ipfs.org/wiki/ → bafybeihn2f7lhumh4grizksi2fl233cyszqadkn424ptjajfenykpsaiw4
	// 	/ipns/en.wikipedia-on-ipfs.org/wiki/Block_of_Wikipedia_in_Turkey → bafkreibn6euazfvoghepcm4efzqx5l3hieof2frhp254hio5y7n3hv5rma

	// The result is an ordered array of values:
	// 	X-Ipfs-Roots: bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze,bafybeihn2f7lhumh4grizksi2fl233cyszqadkn424ptjajfenykpsaiw4,bafkreibn6euazfvoghepcm4efzqx5l3hieof2frhp254hio5y7n3hv5rma

	// Note that while the top one will change every time any article is changed,
	// the last root (responsible for specific article) may not change at all.

	var pathRoots []string
	for _, c := range rq.pathMetadata.PathSegmentRoots {
		pathRoots = append(pathRoots, c.String())
	}
	pathRoots = append(pathRoots, rq.pathMetadata.LastSegment.RootCid().String())
	rootCidList := strings.Join(pathRoots, ",") // convention from rfc2616#sec4.2

	w.Header().Set("X-Ipfs-Roots", rootCidList)
}

// lastModifiedMatch returns true if we can respond with HTTP 304 Not Modified
// It compares If-Modified-Since with logical modification time read from DAG
// (e.g. UnixFS 1.5 modtime, if present)
func lastModifiedMatch(ifModifiedSinceHeader string, lastModified time.Time) bool {
	if ifModifiedSinceHeader == "" || lastModified.IsZero() {
		return false
	}
	ifModifiedSinceTime, err := time.Parse(time.RFC1123, ifModifiedSinceHeader)
	if err != nil {
		return false
	}
	// ignoring fractional seconds (as HTTP dates don't include fractional seconds)
	return !lastModified.Truncate(time.Second).After(ifModifiedSinceTime)
}

// etagMatch evaluates if we can respond with HTTP 304 Not Modified
// It supports multiple weak and strong etags passed in If-None-Match string
// including the wildcard one.
func etagMatch(ifNoneMatchHeader string, etagsToCheck ...string) bool {
	buf := ifNoneMatchHeader
	for {
		buf = textproto.TrimString(buf)
		if len(buf) == 0 {
			break
		}
		if buf[0] == ',' {
			buf = buf[1:]
			continue
		}
		// If-None-Match: * should match against any etag
		if buf[0] == '*' {
			return true
		}
		etag, remain := scanETag(buf)
		if etag == "" {
			break
		}
		// Check for match both strong and weak etags
		for _, etagToCheck := range etagsToCheck {
			if etagWeakMatch(etag, etagToCheck) {
				return true
			}
		}

		buf = remain
	}
	return false
}

// getEtag generates an ETag value based on an HTTP Request, a CID and a response
// format. This function DOES NOT generate ETags for CARs or IPNS Records.
func getEtag(r *http.Request, cid cid.Cid, responseFormat string) string {
	prefix := `"`
	suffix := `"`

	// For Codecs, ensure that we have the right content-type.
	if responseFormat == "" {
		cidCodec := mc.Code(cid.Prefix().Codec)
		if contentType, ok := codecToContentType[cidCodec]; ok {
			responseFormat = contentType
		}
	}

	switch responseFormat {
	case "":
		// Do nothing.
	case carResponseFormat, ipnsRecordResponseFormat:
		// CARs and IPNS Record ETags are handled differently, in their respective handler.
		return ""
	case tarResponseFormat:
		// Weak Etag W/ for formats that we can't guarantee byte-for-byte identical
		// responses, but still want to benefit from HTTP Caching.
		prefix = "W/" + prefix
		fallthrough
	default:
		// application/vnd.ipld.foo → foo
		// application/x-bar → x-bar
		shortFormat := responseFormat[strings.LastIndexAny(responseFormat, "/.")+1:]
		// Etag: "cid.shortFmt" (gives us nice compression together with Content-Disposition in block (raw) and car responses)
		suffix = `.` + shortFormat + suffix
	}

	return prefix + cid.String() + suffix
}

const (
	rawResponseFormat        = "application/vnd.ipld.raw"
	carResponseFormat        = "application/vnd.ipld.car"
	tarResponseFormat        = "application/x-tar"
	jsonResponseFormat       = "application/json"
	cborResponseFormat       = "application/cbor"
	dagJsonResponseFormat    = "application/vnd.ipld.dag-json"
	dagCborResponseFormat    = "application/vnd.ipld.dag-cbor"
	ipnsRecordResponseFormat = "application/vnd.ipfs.ipns-record"
)

var (
	formatParamToResponseFormat = map[string]string{
		"raw":         rawResponseFormat,
		"car":         carResponseFormat,
		"tar":         tarResponseFormat,
		"json":        jsonResponseFormat,
		"cbor":        cborResponseFormat,
		"dag-json":    dagJsonResponseFormat,
		"dag-cbor":    dagCborResponseFormat,
		"ipns-record": ipnsRecordResponseFormat,
	}

	responseFormatToFormatParam = map[string]string{}
)

func init() {
	for k, v := range formatParamToResponseFormat {
		responseFormatToFormatParam[v] = k
	}
}

// return explicit response format if specified in request as query parameter or via Accept HTTP header
func customResponseFormat(r *http.Request) (mediaType string, params map[string]string, err error) {
	// First, inspect Accept header, as it may not only include content type, but also optional parameters.
	// such as CAR version or additional ones from IPIP-412.
	//
	// Browsers and other user agents will send Accept header with generic types like:
	// Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8
	// We only care about explicit, vendor-specific content-types and respond to the first match (in order).
	// TODO: make this RFC compliant and respect weights (eg. return CAR for Accept:application/vnd.ipld.dag-json;q=0.1,application/vnd.ipld.car;q=0.2)
	for _, header := range r.Header.Values("Accept") {
		for _, value := range strings.Split(header, ",") {
			accept := strings.TrimSpace(value)
			// respond to the very first matching content type
			if strings.HasPrefix(accept, "application/vnd.ipld") ||
				strings.HasPrefix(accept, "application/vnd.ipfs") ||
				strings.HasPrefix(accept, tarResponseFormat) ||
				strings.HasPrefix(accept, jsonResponseFormat) ||
				strings.HasPrefix(accept, cborResponseFormat) {
				mediatype, params, err := mime.ParseMediaType(accept)
				if err != nil {
					return "", nil, err
				}
				return mediatype, params, nil
			}
		}
	}

	// If no Accept header, translate query param to a content type, if present.
	if formatParam := r.URL.Query().Get("format"); formatParam != "" {
		if responseFormat, ok := formatParamToResponseFormat[formatParam]; ok {
			return responseFormat, nil, nil
		}
	}

	// If none of special-cased content types is found, return empty string
	// to indicate default, implicit UnixFS response should be prepared
	return "", nil, nil
}

// Add 'Content-Location' headers for non-default response formats. This allows
// correct caching of such format requests when the format is passed via the
// Accept header, for example.
func addContentLocation(r *http.Request, w http.ResponseWriter, rq *requestData) {
	// Skip Content-Location if no explicit format was requested
	// via Accept HTTP header or ?format URL param
	if rq.responseFormat == "" {
		return
	}

	format := responseFormatToFormatParam[rq.responseFormat]

	// Skip Content-Location if there is no conflict between
	// 'format' in URL and value in 'Accept' header.
	// If both are present and don't match, we continue and generate
	// Content-Location to ensure value from Accept overrides 'format' from URL.
	if urlFormat := r.URL.Query().Get("format"); urlFormat != "" && urlFormat == format {
		return
	}

	path := r.URL.Path
	if p, ok := r.Context().Value(OriginalPathKey).(string); ok {
		path = p
	}

	// Copy all existing query parameters.
	query := url.Values{}
	for k, v := range r.URL.Query() {
		query[k] = v
	}
	query.Set("format", format)

	// Set response params as query elements.
	for k, v := range rq.responseParams {
		query.Set(format+"-"+k, v)
	}

	w.Header().Set("Content-Location", path+"?"+query.Encode())
}

// returns unquoted path with all special characters revealed as \u codes
func debugStr(path string) string {
	q := fmt.Sprintf("%+q", path)
	if len(q) >= 3 {
		q = q[1 : len(q)-1]
	}
	return q
}

func (i *handler) handleIfNoneMatch(w http.ResponseWriter, r *http.Request, rq *requestData) bool {
	// Detect when If-None-Match HTTP header allows returning HTTP 304 Not Modified
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		pathMetadata, err := i.backend.ResolvePath(r.Context(), rq.immutablePath)
		if err != nil {
			var forwardedPath path.ImmutablePath
			var continueProcessing bool
			if isWebRequest(rq.responseFormat) {
				forwardedPath, continueProcessing = i.handleWebRequestErrors(w, r, rq.mostlyResolvedPath(), rq.immutablePath, rq.contentPath, err, rq.logger)
				if continueProcessing {
					pathMetadata, err = i.backend.ResolvePath(r.Context(), forwardedPath)
				}
			}
			if !continueProcessing || err != nil {
				err = fmt.Errorf("failed to resolve %s: %w", debugStr(rq.contentPath.String()), err)
				i.webError(w, r, err, http.StatusInternalServerError)
				return true
			}
		}

		pathCid := pathMetadata.LastSegment.RootCid()

		// Checks against both file, dir listing, and dag index Etags.
		// This is an inexpensive check, and it happens before we do any I/O.
		cidEtag := getEtag(r, pathCid, rq.responseFormat)
		dirEtag := getDirListingEtag(pathCid)
		dagEtag := getDagIndexEtag(pathCid)

		if etagMatch(ifNoneMatch, cidEtag, dirEtag, dagEtag) {
			// Finish early if client already has a matching Etag
			w.WriteHeader(http.StatusNotModified)
			return true
		}

		// Check if the resolvedPath is an immutable path.
		_, err = path.NewImmutablePath(pathMetadata.LastSegment)
		if err != nil {
			i.webError(w, r, err, http.StatusInternalServerError)
			return true
		}

		rq.pathMetadata = &pathMetadata
		return false
	}

	return false
}

func (i *handler) handleIfModifiedSince(w http.ResponseWriter, r *http.Request, rq *requestData) bool {
	// Detect when If-Modified-Since HTTP header allows returning HTTP 304 Not Modified
	ifModifiedSince := r.Header.Get("If-Modified-Since")
	if ifModifiedSince == "" {
		return false
	}

	// Resolve path to be able to read pathMetadata.ModTime
	pathMetadata, err := i.backend.ResolvePath(r.Context(), rq.immutablePath)
	if err != nil {
		var forwardedPath path.ImmutablePath
		var continueProcessing bool
		if isWebRequest(rq.responseFormat) {
			forwardedPath, continueProcessing = i.handleWebRequestErrors(w, r, rq.mostlyResolvedPath(), rq.immutablePath, rq.contentPath, err, rq.logger)
			if continueProcessing {
				pathMetadata, err = i.backend.ResolvePath(r.Context(), forwardedPath)
			}
		}
		if !continueProcessing || err != nil {
			err = fmt.Errorf("failed to resolve %s: %w", debugStr(rq.contentPath.String()), err)
			i.webError(w, r, err, http.StatusInternalServerError)
			return true
		}
	}

	// Currently we only care about optional mtime from UnixFS 1.5 (dag-pb)
	// but other sources of this metadata could be added in the future
	lastModified := pathMetadata.ModTime
	if lastModifiedMatch(ifModifiedSince, lastModified) {
		w.WriteHeader(http.StatusNotModified)
		return true
	}

	// Check if the resolvedPath is an immutable path.
	_, err = path.NewImmutablePath(pathMetadata.LastSegment)
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return true
	}

	rq.pathMetadata = &pathMetadata
	return false
}

// check if request was for one of known explicit formats,
// or should use the default, implicit Web+UnixFS behaviors.
func isWebRequest(responseFormat string) bool {
	// The implicit response format is ""
	return responseFormat == ""
}

// handleRequestErrors is used when request type is other than Web+UnixFS
func (i *handler) handleRequestErrors(w http.ResponseWriter, r *http.Request, contentPath path.Path, err error) bool {
	if err == nil {
		return true
	}
	err = fmt.Errorf("failed to resolve %s: %w", debugStr(contentPath.String()), err)
	i.webError(w, r, err, http.StatusInternalServerError)
	return false
}

// handleWebRequestErrors is used when request type is Web+UnixFS and err could
// be a 404 (Not Found) that should be recovered via _redirects file (IPIP-290)
func (i *handler) handleWebRequestErrors(w http.ResponseWriter, r *http.Request, maybeResolvedImPath, immutableContentPath path.ImmutablePath, contentPath path.Path, err error, logger *zap.SugaredLogger) (path.ImmutablePath, bool) {
	if err == nil {
		return maybeResolvedImPath, true
	}

	if errors.Is(err, ErrServiceUnavailable) {
		err = fmt.Errorf("failed to resolve %s: %w", debugStr(contentPath.String()), err)
		i.webError(w, r, err, http.StatusServiceUnavailable)
		return path.ImmutablePath{}, false
	}

	// If the error is not an IPLD traversal error then we should not be looking for _redirects or legacy 404s
	if !isErrNotFound(err) {
		err = fmt.Errorf("failed to resolve %s: %w", debugStr(contentPath.String()), err)
		i.webError(w, r, err, http.StatusInternalServerError)
		return path.ImmutablePath{}, false
	}

	// If we have origin isolation (subdomain gw, DNSLink website),
	// and response type is UnixFS (default for website hosting)
	// we can leverage the presence of an _redirects file and apply rules defined there.
	// See: https://github.com/ipfs/specs/pull/290
	if hasOriginIsolation(r) {
		newContentPath, ok, hadMatchingRule := i.serveRedirectsIfPresent(w, r, maybeResolvedImPath, immutableContentPath, contentPath, logger)
		if hadMatchingRule {
			logger.Debugw("applied a rule from _redirects file")
			return newContentPath, ok
		}
	}

	err = fmt.Errorf("failed to resolve %s: %w", debugStr(contentPath.String()), err)
	i.webError(w, r, err, http.StatusInternalServerError)
	return path.ImmutablePath{}, false
}

// Detect 'Cache-Control: only-if-cached' in request and return data if it is already in the local datastore.
// https://github.com/ipfs/specs/blob/main/http-gateways/PATH_GATEWAY.md#cache-control-request-header
func (i *handler) handleOnlyIfCached(w http.ResponseWriter, r *http.Request, contentPath path.Path) bool {
	if r.Header.Get("Cache-Control") == "only-if-cached" {
		if !i.backend.IsCached(r.Context(), contentPath) {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusPreconditionFailed)
				return true
			}
			errMsg := fmt.Sprintf("%q not in local datastore", contentPath.String())
			http.Error(w, errMsg, http.StatusPreconditionFailed)
			return true
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return true
		}
	}
	return false
}

// ?uri query param support for requests produced by web browsers
// via navigator.registerProtocolHandler Web API
// https://developer.mozilla.org/en-US/docs/Web/API/Navigator/registerProtocolHandler
// TLDR: redirect /ipfs/?uri=ipfs%3A%2F%2Fcid%3Fquery%3Dval to /ipfs/cid?query=val
func handleProtocolHandlerRedirect(w http.ResponseWriter, r *http.Request, c *Config) bool {
	if uriParam := r.URL.Query().Get("uri"); uriParam != "" {
		u, err := url.Parse(uriParam)
		if err != nil {
			webError(w, r, c, fmt.Errorf("failed to parse uri query parameter: %w", err), http.StatusBadRequest)
			return true
		}
		if u.Scheme != "ipfs" && u.Scheme != "ipns" {
			webError(w, r, c, fmt.Errorf("uri query parameter scheme must be ipfs or ipns: %w", err), http.StatusBadRequest)
			return true
		}

		path := u.EscapedPath()
		if u.RawQuery != "" { // preserve query if present
			path += "?" + url.PathEscape(u.RawQuery)
		}

		redirectURL := gopath.Join("/", u.Scheme, u.Host, path)
		http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
		return true
	}

	return false
}

// Disallow Service Worker registration on namespace roots
// https://github.com/ipfs/kubo/issues/4025
func (i *handler) handleServiceWorkerRegistration(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Service-Worker") == "script" {
		matched, _ := regexp.MatchString(`^/ip[fn]s/[^/]+$`, r.URL.Path)
		if matched {
			err := errors.New("navigator.serviceWorker: registration is not allowed for this scope")
			i.webError(w, r, err, http.StatusBadRequest)
			return true
		}
	}

	return false
}

// handleIpnsB58mhToCidRedirection redirects from /ipns/b58mh to /ipns/cid in
// the most cost-effective way.
func handleIpnsB58mhToCidRedirection(w http.ResponseWriter, r *http.Request) bool {
	if _, dnslink := r.Context().Value(DNSLinkHostnameKey).(string); dnslink {
		// For DNSLink hostnames, do not perform redirection in order to not break
		// website. For example, if `example.net` is backed by `/ipns/base58`, we
		// must NOT redirect to `example.net/ipns/base36-id`.
		return false
	}

	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 3 {
		return false
	}

	if pathParts[1] != "ipns" {
		return false
	}

	id, err := peer.Decode(pathParts[2])
	if err != nil {
		return false
	}

	// Convert the peer ID to a CIDv1.
	cid := peer.ToCid(id)

	// Encode CID in base36 to match the subdomain URLs.
	encodedCID, err := cid.StringOfBase(multibase.Base36)
	if err != nil {
		return false
	}

	// If the CID was already encoded, do not redirect.
	if encodedCID == pathParts[2] {
		return false
	}

	pathParts[2] = encodedCID
	r.URL.Path = strings.Join(pathParts, "/")
	http.Redirect(w, r, r.URL.String(), http.StatusFound)
	return true
}

// Attempt to fix redundant /ipfs/ namespace as long as resulting
// 'intended' path is valid.  This is in case gremlins were tickled
// wrong way and user ended up at /ipfs/ipfs/{cid} or /ipfs/ipns/{id}
// like in bafybeien3m7mdn6imm425vc2s22erzyhbvk5n3ofzgikkhmdkh5cuqbpbq :^))
func (i *handler) handleSuperfluousNamespace(w http.ResponseWriter, r *http.Request) bool {
	// If there's no superflous namespace, there's nothing to do
	if !(strings.HasPrefix(r.URL.Path, "/ipfs/ipfs/") || strings.HasPrefix(r.URL.Path, "/ipfs/ipns/")) {
		return false
	}

	// Attempt to fix the superflous namespace
	intendedPath, err := path.NewPath(strings.TrimPrefix(r.URL.EscapedPath(), "/ipfs"))
	if err != nil {
		i.webError(w, r, fmt.Errorf("invalid ipfs path: %w", err), http.StatusBadRequest)
		return true
	}

	intendedURL := intendedPath.String()
	if r.URL.RawQuery != "" {
		// we render HTML, so ensure query entries are properly escaped
		q, _ := url.ParseQuery(r.URL.RawQuery)
		intendedURL = intendedURL + "?" + q.Encode()
	}

	http.Redirect(w, r, intendedURL, http.StatusMovedPermanently)
	return true
}

// getTemplateGlobalData returns the global data necessary by most templates.
func (i *handler) getTemplateGlobalData(r *http.Request, contentPath path.Path) assets.GlobalData {
	// gatewayURL is used to link to other root CIDs. THis will be blank unless
	// subdomain or DNSLink resolution is being used for this request.
	var gatewayURL string
	if h, ok := r.Context().Value(SubdomainHostnameKey).(string); ok {
		gatewayURL = "//" + h
	} else if h, ok := r.Context().Value(DNSLinkHostnameKey).(string); ok {
		gatewayURL = "//" + h
	} else {
		gatewayURL = ""
	}

	dnsLink := assets.HasDNSLinkOrigin(gatewayURL, contentPath.String())

	return assets.GlobalData{
		Menu:       i.config.Menu,
		GatewayURL: gatewayURL,
		DNSLink:    dnsLink,
	}
}

func (i *handler) webError(w http.ResponseWriter, r *http.Request, err error, defaultCode int) {
	webError(w, r, i.config, err, defaultCode)
}
