package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ipfs/boxo/gateway/assets"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"

	// Ensure basic codecs are registered.
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	mc "github.com/multiformats/go-multicodec"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// codecToContentType maps the supported IPLD codecs to the HTTP Content
// Type they should have.
var codecToContentType = map[mc.Code]string{
	mc.Json:    jsonResponseFormat,
	mc.Cbor:    cborResponseFormat,
	mc.DagJson: dagJsonResponseFormat,
	mc.DagCbor: dagCborResponseFormat,
}

// contentTypeToRaw maps the HTTP Content Type to the respective codec that
// allows raw response without any conversion.
var contentTypeToRaw = map[string][]mc.Code{
	jsonResponseFormat: {mc.Json, mc.DagJson},
	cborResponseFormat: {mc.Cbor, mc.DagCbor},
}

// contentTypeToCodec maps the HTTP Content Type to the respective codec. We
// only add here the codecs that we want to convert-to-from.
var contentTypeToCodec = map[string]mc.Code{
	dagJsonResponseFormat: mc.DagJson,
	dagCborResponseFormat: mc.DagCbor,
}

// contentTypeToExtension maps the HTTP Content Type to the respective file
// extension, used in Content-Disposition header when downloading the file.
var contentTypeToExtension = map[string]string{
	jsonResponseFormat:    ".json",
	dagJsonResponseFormat: ".json",
	cborResponseFormat:    ".cbor",
	dagCborResponseFormat: ".cbor",
}

func (i *handler) serveCodec(ctx context.Context, w http.ResponseWriter, r *http.Request, rq *requestData) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeCodec", trace.WithAttributes(attribute.String("path", rq.immutablePath.String()), attribute.String("requestedContentType", rq.responseFormat)))
	defer span.End()

	pathMetadata, data, err := i.backend.GetBlock(ctx, rq.mostlyResolvedPath())
	if !i.handleRequestErrors(w, r, rq.contentPath, err) {
		return false
	}
	defer data.Close()

	setIpfsRootsHeader(w, rq, &pathMetadata)

	blockSize, err := data.Size()
	if !i.handleRequestErrors(w, r, rq.contentPath, err) {
		return false
	}

	return i.renderCodec(ctx, w, r, rq, blockSize, data)
}

func (i *handler) renderCodec(ctx context.Context, w http.ResponseWriter, r *http.Request, rq *requestData, blockSize int64, blockData io.ReadSeekCloser) bool {
	resolvedPath := rq.pathMetadata.LastSegment
	ctx, span := spanTrace(ctx, "Handler.RenderCodec", trace.WithAttributes(attribute.String("path", resolvedPath.String()), attribute.String("requestedContentType", rq.responseFormat)))
	defer span.End()

	blockCid := resolvedPath.RootCid()
	cidCodec := mc.Code(blockCid.Prefix().Codec)
	responseContentType := rq.responseFormat

	// If the resolved path still has some remainder, return error for now.
	// TODO: handle this when we have IPLD Patch (https://ipld.io/specs/patch/) via HTTP PUT
	// TODO: (depends on https://github.com/ipfs/kubo/issues/4801 and https://github.com/ipfs/kubo/issues/4782)
	if len(rq.pathMetadata.LastSegmentRemainder) != 0 {
		remainderStr := path.SegmentsToString(rq.pathMetadata.LastSegmentRemainder...)
		path := strings.TrimSuffix(resolvedPath.String(), remainderStr)
		err := fmt.Errorf("%q of %q could not be returned: reading IPLD Kinds other than Links (CBOR Tag 42) is not implemented: try reading %q instead", remainderStr, resolvedPath.String(), path)
		i.webError(w, r, err, http.StatusNotImplemented)
		return false
	}

	// If no explicit content type was requested, the response will have one based on the codec from the CID
	if rq.responseFormat == "" {
		cidContentType, ok := codecToContentType[cidCodec]
		if !ok {
			// Should not happen unless function is called with wrong parameters.
			err := fmt.Errorf("content type not found for codec: %v", cidCodec)
			i.webError(w, r, err, http.StatusInternalServerError)
			return false
		}
		responseContentType = cidContentType
	}

	// Set HTTP headers (for caching, etc). Etag will be replaced if handled by serveCodecHTML.
	modtime := addCacheControlHeaders(w, r, rq.contentPath, rq.ttl, rq.lastMod, resolvedPath.RootCid(), responseContentType)
	_ = setCodecContentDisposition(w, r, resolvedPath, responseContentType)
	w.Header().Set("Content-Type", responseContentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// No content type is specified by the user (via Accept, or format=). However,
	// we support this format. Let's handle it.
	if rq.responseFormat == "" {
		isDAG := cidCodec == mc.DagJson || cidCodec == mc.DagCbor
		acceptsHTML := strings.Contains(r.Header.Get("Accept"), "text/html")
		download := r.URL.Query().Get("download") == "true"

		if isDAG && acceptsHTML && !download {
			return i.serveCodecHTML(ctx, w, r, blockCid, blockData, resolvedPath, rq.contentPath)
		} else {
			// This covers CIDs with codec 'json' and 'cbor' as those do not have
			// an explicit requested content type.
			return i.serveCodecRaw(ctx, w, r, blockSize, blockData, rq.contentPath, modtime, rq.begin)
		}
	}

	// If DAG-JSON or DAG-CBOR was requested using corresponding plain content type
	// return raw block as-is, without conversion
	skipCodecs, ok := contentTypeToRaw[rq.responseFormat]
	if ok {
		for _, skipCodec := range skipCodecs {
			if skipCodec == cidCodec {
				return i.serveCodecRaw(ctx, w, r, blockSize, blockData, rq.contentPath, modtime, rq.begin)
			}
		}
	}

	// Otherwise, the user has requested a specific content type (a DAG-* variant).
	// Let's first get the codecs that can be used with this content type.
	toCodec, ok := contentTypeToCodec[rq.responseFormat]
	if !ok {
		err := fmt.Errorf("converting from %q to %q is not supported", cidCodec.String(), rq.responseFormat)
		i.webError(w, r, err, http.StatusBadRequest)
		return false
	}

	// This handles DAG-* conversions and validations.
	return i.serveCodecConverted(ctx, w, r, blockCid, blockData, rq.contentPath, toCodec, modtime, rq.begin)
}

func (i *handler) serveCodecHTML(ctx context.Context, w http.ResponseWriter, r *http.Request, blockCid cid.Cid, blockData io.Reader, resolvedPath path.ImmutablePath, contentPath path.Path) bool {
	// WithHostname may have constructed an IPFS (or IPNS) path using the Host header.
	// In this case, we need the original path for constructing the redirect.
	requestURI, err := url.ParseRequestURI(r.RequestURI)
	if err != nil {
		i.webError(w, r, fmt.Errorf("failed to parse request path: %w", err), http.StatusInternalServerError)
		return false
	}

	// Ensure HTML rendering is in a path that ends with trailing slash.
	if requestURI.Path[len(requestURI.Path)-1] != '/' {
		suffix := "/"
		// preserve query parameters
		if r.URL.RawQuery != "" {
			suffix = suffix + "?" + url.PathEscape(r.URL.RawQuery)
		}
		// Re-escape path instead of reusing RawPath to avod mix of lawer
		// and upper hex that may come from RawPath.
		if strings.ContainsRune(requestURI.RawPath, '%') {
			requestURI.RawPath = ""
		}
		// /ipfs/cid/foo?bar must be redirected to /ipfs/cid/foo/?bar
		redirectURL := requestURI.EscapedPath() + suffix
		http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
		return true
	}

	// A HTML directory index will be presented, be sure to set the correct
	// type instead of relying on autodetection (which may fail).
	w.Header().Set("Content-Type", "text/html")

	// Clear Content-Disposition -- we want HTML to be rendered inline
	w.Header().Del("Content-Disposition")

	// Generated index requires custom Etag (output may change between Kubo versions)
	dagEtag := getDagIndexEtag(resolvedPath.RootCid())
	w.Header().Set("Etag", dagEtag)

	// Remove Cache-Control for now to match UnixFS dir-index-html responses
	// (we don't want browser to cache HTML forever)
	// TODO: if we ever change behavior for UnixFS dir listings, same changes should be applied here
	w.Header().Del("Cache-Control")

	cidCodec := mc.Code(resolvedPath.RootCid().Prefix().Codec)
	err = assets.DagTemplate.Execute(w, assets.DagTemplateData{
		GlobalData: i.getTemplateGlobalData(r, contentPath),
		Path:       contentPath.String(),
		CID:        resolvedPath.RootCid().String(),
		CodecName:  cidCodec.String(),
		CodecHex:   fmt.Sprintf("0x%x", uint64(cidCodec)),
		Node:       parseNode(blockCid, blockData),
	})
	if err != nil {
		_, _ = w.Write([]byte(fmt.Sprintf("error during body generation: %v", err)))
	}

	return err == nil
}

// parseNode does a best effort attempt to parse this request's block such that
// a preview can be displayed in the gateway. If something fails along the way,
// returns nil, therefore not displaying the preview.
func parseNode(blockCid cid.Cid, blockData io.Reader) *assets.ParsedNode {
	codec := blockCid.Prefix().Codec
	decoder, err := multicodec.LookupDecoder(codec)
	if err != nil {
		return nil
	}

	nodeBuilder := basicnode.Prototype.Any.NewBuilder()
	err = decoder(nodeBuilder, blockData)
	if err != nil {
		return nil
	}

	parsedNode, err := assets.ParseNode(nodeBuilder.Build())
	if err != nil {
		return nil
	}

	return parsedNode
}

// serveCodecRaw returns the raw block without any conversion
func (i *handler) serveCodecRaw(ctx context.Context, w http.ResponseWriter, r *http.Request, blockSize int64, blockData io.ReadSeekCloser, contentPath path.Path, modtime, begin time.Time) bool {
	// ServeContent will take care of
	// If-None-Match+Etag, Content-Length and setting range request headers after we've already seeked to the start of
	// the first range
	if !i.seekToStartOfFirstRange(w, r, blockData) {
		return false
	}
	_, dataSent, _ := serveContent(w, r, modtime, blockSize, blockData)

	if dataSent {
		// Update metrics
		i.jsoncborDocumentGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
	}

	return dataSent
}

// serveCodecConverted returns payload converted to codec specified in toCodec
func (i *handler) serveCodecConverted(ctx context.Context, w http.ResponseWriter, r *http.Request, blockCid cid.Cid, blockData io.ReadCloser, contentPath path.Path, toCodec mc.Code, modtime, begin time.Time) bool {
	codec := blockCid.Prefix().Codec
	decoder, err := multicodec.LookupDecoder(codec)
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	node := basicnode.Prototype.Any.NewBuilder()
	err = decoder(node, blockData)
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	encoder, err := multicodec.LookupEncoder(uint64(toCodec))
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	// Ensure IPLD node conforms to the codec specification.
	var buf bytes.Buffer
	err = encoder(node.Build(), &buf)
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	// Sets correct Last-Modified header. This code is borrowed from the standard
	// library (net/http/server.go) as we cannot use serveFile.
	if !(modtime.IsZero() || modtime.Equal(unixEpochTime)) {
		w.Header().Set("Last-Modified", modtime.UTC().Format(http.TimeFormat))
	}

	_, err = w.Write(buf.Bytes())
	if err == nil {
		// Update metrics
		i.jsoncborDocumentGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
		return true
	}

	return false
}

func setCodecContentDisposition(w http.ResponseWriter, r *http.Request, resolvedPath path.ImmutablePath, contentType string) string {
	var dispType, name string

	ext, ok := contentTypeToExtension[contentType]
	if !ok {
		// Should never happen.
		ext = ".bin"
	}

	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = resolvedPath.RootCid().String() + ext
	}

	// JSON should be inlined, but ?download=true should still override
	if r.URL.Query().Get("download") == "true" {
		dispType = "attachment"
	} else {
		switch ext {
		case ".json": // codecs that serialize to JSON can be rendered by browsers
			dispType = "inline"
		default: // everything else is assumed binary / opaque bytes
			dispType = "attachment"
		}
	}

	setContentDispositionHeader(w, name, dispType)
	return name
}

func getDagIndexEtag(dagCid cid.Cid) string {
	return `"DagIndex-` + assets.AssetHash + `_CID-` + dagCid.String() + `"`
}
