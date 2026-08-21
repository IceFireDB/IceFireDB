package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ipfs/boxo/files"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// EmptyIdentityCIDString is the canonical string form of [EmptyIdentityCID].
const EmptyIdentityCIDString = "bafkqaaa"

// EmptyIdentityCID is the empty block addressed with an identity multihash:
// CIDv1, raw codec, zero-length digest (bytes 0x01 0x55 0x00 0x00). Because the
// multihash is an identity hash, the block's content (zero bytes) is encoded in
// the CID itself, so it can be served without any blockstore or network lookup.
//
// The trustless gateway specification designates this CID as the probe for
// checking whether an endpoint is a working trustless gateway, and requires a
// 200 OK with an empty body for a raw-block request:
// https://specs.ipfs.tech/http-gateways/trustless-gateway/#dedicated-probe-paths
//
// boxo's bitswap HTTP client (bitswap/network/httpnet) sends HEAD and GET
// requests for this CID with ?format=raw to probe providers before fetching
// real blocks and to ping connected ones. [handler.serveRawBlock] answers those
// requests directly so the gateway returns 200 with an empty body even when the
// configured [IPFSBackend] cannot reconstruct identity CIDs from the CID alone.
var EmptyIdentityCID = cid.MustParse(EmptyIdentityCIDString)

// serveRawBlock returns bytes behind a raw block
func (i *handler) serveRawBlock(ctx context.Context, w http.ResponseWriter, r *http.Request, rq *requestData) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeRawBlock", trace.WithAttributes(attribute.String("path", rq.immutablePath.String())))
	defer span.End()

	var pathMetadata ContentPathMetadata
	var data files.File
	var err error
	if isEmptyIdentityProbe(rq) {
		// Serve the well-known empty identity block ([EmptyIdentityCID]) without
		// touching the backend. This keeps the trustless-gateway probe and
		// bitswap httpnet pings working even when the backend cannot serve
		// identity CIDs. See [EmptyIdentityCID] for details.
		pathMetadata, data = emptyIdentityBlock(rq)
	} else {
		pathMetadata, data, err = i.backend.GetBlock(ctx, rq.mostlyResolvedPath())
	}
	if !i.handleRequestErrors(w, r, rq.contentPath, err) {
		return false
	}
	defer data.Close()

	sz, err := data.Size()
	if err != nil {
		i.handleRequestErrors(w, r, rq.contentPath, err)
		return false
	}

	// Check size limit before setting response headers so 410 responses
	// stay clean.
	if i.exceedsMaxUnixFSDAGResponseSize(w, r, sz) {
		return false
	}

	setIpfsRootsHeader(w, rq, &pathMetadata)

	blockCid := pathMetadata.LastSegment.RootCid()

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = blockCid.String() + ".bin"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Set remaining headers
	modtime := addCacheControlHeaders(w, r, rq.contentPath, rq.ttl, rq.lastMod, blockCid, rawResponseFormat)
	w.Header().Set("Content-Type", rawResponseFormat)
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	s, ok := data.(io.Seeker)
	if !ok {
		i.webError(w, r, fmt.Errorf("block data does not support seeking"), http.StatusInternalServerError)
		return false
	}
	if !i.seekToStartOfFirstRange(w, r, s, sz) {
		return false
	}

	// ServeContent will take care of
	// If-None-Match+Etag, Content-Length and range requests
	_, dataSent, _ := serveContent(w, r, modtime, sz, data)

	if dataSent {
		// Update metrics
		i.rawBlockGetMetric.WithLabelValues(rq.contentPath.Namespace()).Observe(time.Since(rq.begin).Seconds())
	}

	return dataSent
}

// isEmptyIdentityProbe reports whether rq is a raw-block request for exactly
// [EmptyIdentityCID] with no trailing path. Such requests are the probe and ping
// defined by the trustless-gateway spec and used by bitswap httpnet.
func isEmptyIdentityProbe(rq *requestData) bool {
	imPath := rq.mostlyResolvedPath()
	// The CID comparison is an allocation-free string compare and gates the
	// segment check (which allocates) so non-probe raw requests pay nothing.
	// Two segments means the bare CID with no remainder, e.g. /ipfs/bafkqaaa.
	return imPath.RootCid().Equals(EmptyIdentityCID) && len(imPath.Segments()) == 2
}

// emptyIdentityBlock returns the canonical raw-block response for
// [EmptyIdentityCID]: a zero-byte block. It is synthesized locally so the rest
// of serveRawBlock writes the same 200 response it would for any other raw
// block.
func emptyIdentityBlock(rq *requestData) (ContentPathMetadata, files.File) {
	md := ContentPathMetadata{
		LastSegment: rq.mostlyResolvedPath(),
	}
	return md, files.NewBytesFile(nil)
}
