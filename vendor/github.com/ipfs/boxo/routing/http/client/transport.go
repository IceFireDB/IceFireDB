package client

import (
	"fmt"
	"io"
	"net/http"
	"reflect"
	"runtime/debug"
	"strings"
)

type ResponseBodyLimitedTransport struct {
	http.RoundTripper
	LimitBytes int64
	UserAgent  string
}

func (r *ResponseBodyLimitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if r.UserAgent != "" {
		req.Header.Set("User-Agent", r.UserAgent)
	}
	resp, err := r.RoundTripper.RoundTrip(req)
	if resp != nil && resp.Body != nil {
		resp.Body = &limitReadCloser{
			limit:      r.LimitBytes,
			ReadCloser: resp.Body,
		}
	}
	return resp, err
}

type limitReadCloser struct {
	limit     int64
	bytesRead int64
	io.ReadCloser
}

func (l *limitReadCloser) Read(p []byte) (int, error) {
	n, err := l.ReadCloser.Read(p)
	l.bytesRead += int64(n)
	if l.bytesRead > l.limit {
		return 0, fmt.Errorf("reached read limit of %d bytes after reading %d bytes", l.limit, l.bytesRead)
	}
	return n, err
}

// ImportPath is the canonical import path that allows us to identify
// official client builds vs modified forks, and use that info in User-Agent header.
var ImportPath = importPath()

// importPath returns the path that library consumers would have in go.mod
func importPath() string {
	p := reflect.ValueOf(ResponseBodyLimitedTransport{}).Type().PkgPath()
	// we have monorepo, so stripping the remainder
	return strings.TrimSuffix(p, "/routing/http/client")
}

// moduleVersion returns a useful user agent version string allowing us to
// identify requests coming from official releases of this module vs forks.
func moduleVersion() (ua string) {
	ua = ImportPath
	var module *debug.Module
	if bi, ok := debug.ReadBuildInfo(); ok {
		// If debug.ReadBuildInfo was successful, we can read Version by finding
		// this client in the dependency list of the app that has it in go.mod
		for _, dep := range bi.Deps {
			if dep.Path == ImportPath {
				module = dep
				break
			}
		}
		if module != nil {
			ua += "@" + module.Version
			return
		}
		ua += "@unknown"
	}
	return
}
