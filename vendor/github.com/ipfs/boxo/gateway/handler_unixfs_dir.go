package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	gopath "path"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/gateway/assets"
	"github.com/ipfs/boxo/path"
	cid "github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// serveDirectory returns the best representation of UnixFS directory
//
// It will return index.html if present, or generate directory listing otherwise.
func (i *handler) serveDirectory(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath path.ImmutablePath, rq *requestData, isHeadRequest bool, directoryMetadata *directoryMetadata, ranges []ByteRange) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeDirectory", trace.WithAttributes(attribute.String("path", resolvedPath.String())))
	defer span.End()

	// WithHostname might have constructed an IPNS/IPFS path using the Host header.
	// In this case, we need the original path for constructing redirects and links
	// that match the requested URL.
	// For example, http://example.net would become /ipns/example.net, and
	// the redirects and links would end up as http://example.net/ipns/example.net
	requestURI, err := url.ParseRequestURI(r.RequestURI)
	if err != nil {
		i.webError(w, r, fmt.Errorf("failed to parse request path: %w", err), http.StatusInternalServerError)
		return false
	}
	originalURLPath := requestURI.Path

	// Ensure directory paths end with '/'
	if originalURLPath[len(originalURLPath)-1] != '/' {
		// don't redirect to trailing slash if it's go get
		// https://github.com/ipfs/kubo/pull/3963
		goget := r.URL.Query().Get("go-get") == "1"
		if !goget {
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
			rq.logger.Debugw("directory location moved permanently", "status", http.StatusMovedPermanently)

			http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
			return true
		}
	}

	// Check if directory has index.html, if so, serveFile
	idxPath, err := path.Join(rq.contentPath, "index.html")
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	indexPath, err := path.Join(resolvedPath, "index.html")
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	imIndexPath, err := path.NewImmutablePath(indexPath)
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	// TODO: could/should this all be skipped to have HEAD requests just return html content type and save the complexity? If so can we skip the above code as well?
	var idxFileBytes io.ReadCloser
	var idxFileSize int64
	var returnRangeStartsAtZero bool
	if isHeadRequest {
		var idxHeadResp *HeadResponse
		_, idxHeadResp, err = i.backend.Head(ctx, imIndexPath)
		if err == nil {
			defer idxHeadResp.Close()
			if !idxHeadResp.isFile {
				i.webError(w, r, fmt.Errorf("%q could not be read: %w", imIndexPath, files.ErrNotReader), http.StatusUnprocessableEntity)
				return false
			}
			returnRangeStartsAtZero = true
			idxFileBytes = idxHeadResp.startingBytes
			idxFileSize = idxHeadResp.bytesSize
		}
	} else {
		var idxGetResp *GetResponse
		_, idxGetResp, err = i.backend.Get(ctx, imIndexPath, ranges...)
		if err == nil {
			defer idxGetResp.Close()
			if idxGetResp.bytes == nil {
				i.webError(w, r, fmt.Errorf("%q could not be read: %w", imIndexPath, files.ErrNotReader), http.StatusUnprocessableEntity)
				return false
			}
			if len(ranges) > 0 {
				ra := ranges[0]
				returnRangeStartsAtZero = ra.From == 0
			}
			idxFileBytes = idxGetResp.bytes
			idxFileSize = idxGetResp.bytesSize
		}
	}

	if err == nil {
		rq.logger.Debugw("serving index.html file", "path", idxPath)
		originalContentPath := rq.contentPath
		rq.contentPath = idxPath
		// write to request
		success := i.serveFile(ctx, w, r, resolvedPath, rq, idxFileSize, idxFileBytes, false, returnRangeStartsAtZero, "text/html")
		if success {
			i.unixfsDirIndexGetMetric.WithLabelValues(originalContentPath.Namespace()).Observe(time.Since(rq.begin).Seconds())
		}
		return success
	} else if isErrNotFound(err) {
		rq.logger.Debugw("no index.html; noop", "path", idxPath)
	} else {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	// A HTML directory index will be presented, be sure to set the correct
	// type instead of relying on autodetection (which may fail).
	w.Header().Set("Content-Type", "text/html")

	// Generated dir index requires custom Etag (output may change between go-libipfs versions)
	dirEtag := getDirListingEtag(resolvedPath.RootCid())
	w.Header().Set("Etag", dirEtag)

	// Set Cache-Control
	if rq.ttl > 0 {
		// Use known TTL from IPNS Record or DNSLink TXT Record
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d, stale-while-revalidate=2678400", int(rq.ttl.Seconds())))
	} else if !rq.contentPath.Mutable() {
		// Cache for 1 week, serve stale cache for up to a month
		// (style of generated HTML may change, should not be cached forever)
		w.Header().Set("Cache-Control", "public, max-age=604800, stale-while-revalidate=2678400")
	}

	if r.Method == http.MethodHead {
		rq.logger.Debug("return as request's HTTP method is HEAD")
		return true
	}

	var dirListing []assets.DirectoryItem
	for l := range directoryMetadata.entries {
		if l.Err != nil {
			i.webError(w, r, l.Err, http.StatusInternalServerError)
			return false
		}

		name := l.Link.Name
		sz := l.Link.Size
		linkCid := l.Link.Cid

		hash := linkCid.String()
		di := assets.DirectoryItem{
			Size:      humanize.Bytes(sz),
			Name:      name,
			Path:      gopath.Join(originalURLPath, name),
			Hash:      hash,
			ShortHash: assets.ShortHash(hash),
		}
		dirListing = append(dirListing, di)
	}

	// construct the correct back link
	// https://github.com/ipfs/kubo/issues/1365
	backLink := originalURLPath

	// don't go further up than /ipfs/$hash/
	pathSplit := strings.Split(rq.contentPath.String(), "/")
	switch {
	// skip backlink when listing a content root
	case len(pathSplit) == 3: // url: /ipfs/$hash
		backLink = ""

	// skip backlink when listing a content root
	case len(pathSplit) == 4 && pathSplit[3] == "": // url: /ipfs/$hash/
		backLink = ""

	// add the correct link depending on whether the path ends with a slash
	default:
		if strings.HasSuffix(backLink, "/") {
			backLink += ".."
		} else {
			backLink += "/.."
		}
	}

	size := humanize.Bytes(directoryMetadata.dagSize)
	hash := resolvedPath.RootCid().String()
	globalData := i.getTemplateGlobalData(r, rq.contentPath)

	// See comment above where originalUrlPath is declared.
	tplData := assets.DirectoryTemplateData{
		GlobalData:  globalData,
		Listing:     dirListing,
		Size:        size,
		Path:        rq.contentPath.String(),
		Breadcrumbs: assets.Breadcrumbs(rq.contentPath.String(), globalData.DNSLink),
		BackLink:    backLink,
		Hash:        hash,
	}

	rq.logger.Debugw("request processed", "tplDataDNSLink", globalData.DNSLink, "tplDataSize", size, "tplDataBackLink", backLink, "tplDataHash", hash)

	if err := assets.DirectoryTemplate.Execute(w, tplData); err != nil {
		_, _ = w.Write([]byte(fmt.Sprintf("error during body generation: %v", err)))
		return false
	}

	// Update metrics
	i.unixfsGenDirListingGetMetric.WithLabelValues(rq.contentPath.Namespace()).Observe(time.Since(rq.begin).Seconds())
	return true
}

func getDirListingEtag(dirCid cid.Cid) string {
	return `"DirIndex-` + assets.AssetHash + `_CID-` + dirCid.String() + `"`
}
