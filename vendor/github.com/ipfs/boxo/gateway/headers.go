package gateway

import (
	"net/http"
	"slices"
)

// Headers is an HTTP middleware that sets the configured headers in all requests.
type Headers struct {
	headers map[string][]string
}

// NewHeaders creates a new [Headers] middleware that applies the given headers
// to all requests. If you call [Headers.ApplyCors], the default CORS configuration
// will also be applied, if any of the CORS headers is missing.
func NewHeaders(headers map[string][]string) *Headers {
	h := &Headers{
		headers: map[string][]string{},
	}

	for k, v := range headers {
		h.headers[http.CanonicalHeaderKey(k)] = v
	}

	return h
}

// ApplyCors applies safe default HTTP headers for controlling cross-origin
// requests. This function adds several values to the [Access-Control-Allow-Headers]
// and [Access-Control-Expose-Headers] entries to be exposed on GET and OPTIONS
// responses, including [CORS Preflight].
//
// If the Access-Control-Allow-Origin entry is missing, a default value of '*' is
// added, indicating that browsers should allow requesting code from any
// origin to access the resource.
//
// If the Access-Control-Allow-Methods entry is missing a value, 'GET, HEAD,
// OPTIONS' is added, indicating that browsers may use them when issuing cross
// origin requests.
//
// [Access-Control-Allow-Headers]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Headers
// [Access-Control-Expose-Headers]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Expose-Headers
// [CORS Preflight]: https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request
func (h *Headers) ApplyCors() *Headers {
	// Hard-coded headers.
	const ACAHeadersName = "Access-Control-Allow-Headers"
	const ACEHeadersName = "Access-Control-Expose-Headers"
	const ACAOriginName = "Access-Control-Allow-Origin"
	const ACAMethodsName = "Access-Control-Allow-Methods"

	if _, ok := h.headers[ACAOriginName]; !ok {
		// Default to *all*
		h.headers[ACAOriginName] = []string{"*"}
	}
	if _, ok := h.headers[ACAMethodsName]; !ok {
		// Default to GET, HEAD, OPTIONS
		h.headers[ACAMethodsName] = []string{
			http.MethodGet,
			http.MethodHead,
			http.MethodOptions,
		}
	}

	h.headers[ACAHeadersName] = cleanHeaderSet(
		append([]string{
			"Content-Type",
			"User-Agent",
			"Range",
			"X-Requested-With",
		}, h.headers[ACAHeadersName]...))

	h.headers[ACEHeadersName] = cleanHeaderSet(
		append([]string{
			"Content-Length",
			"Content-Range",
			"X-Chunked-Output",
			"X-Stream-Output",
			"X-Ipfs-Path",
			"X-Ipfs-Roots",
		}, h.headers[ACEHeadersName]...))

	return h
}

// Wrap wraps the given [http.Handler] with the headers middleware.
func (h *Headers) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range h.headers {
			w.Header()[k] = v
		}

		next.ServeHTTP(w, r)
	})
}

// cleanHeaderSet is an helper function that cleans a set of headers by
// (1) canonicalizing, (2) de-duplicating and (3) sorting.
func cleanHeaderSet(headers []string) []string {
	// Deduplicate and canonicalize.
	m := make(map[string]struct{}, len(headers))
	for _, h := range headers {
		m[http.CanonicalHeaderKey(h)] = struct{}{}
	}
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}

	slices.Sort(result)
	return result
}
