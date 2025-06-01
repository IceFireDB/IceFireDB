package gateway

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	gopath "path"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/path"
	redirects "github.com/ipfs/go-ipfs-redirects-file"
	"go.uber.org/zap"
)

// Resolving a UnixFS path involves determining if the provided `path.Path`
// exists and returning the `path.Resolved` corresponding to that path. For
// UnixFS, path resolution is more involved.
//
// When a path under requested CID does not exist, Gateway will check if a
// `_redirects` file exists underneath the root CID of the path, and apply
// rules defined there. See specification introduced in:
// https://github.com/ipfs/specs/pull/290
//
// # Scenario 1:
//
// If a path exists, we always return the `path.Resolved` corresponding to that
// path, regardless of the existence of a `_redirects` file.
//
// # Scenario 2:
//
// If a path does not exist, usually we should return a `nil` resolution path
// and an error indicating that the path doesn't exist. However, a `_redirects`
// file may exist and contain a redirect rule that redirects that path to a
// different path. We need to evaluate the rule and perform the redirect if
// present.
//
// # Scenario 3:
//
// Another possibility is that the path corresponds to a rewrite rule (i.e. a
// rule with a status of 200). In this case, we don't perform a redirect, but
// do need to return a `path.Resolved` and `path.Path` corresponding to the
// rewrite destination path.
//
// Note that for security reasons, redirect rules are only processed when the
// request has origin isolation. See https://github.com/ipfs/specs/pull/290 for
// more information.
func (i *handler) serveRedirectsIfPresent(w http.ResponseWriter, r *http.Request, maybeResolvedImPath, immutableContentPath path.ImmutablePath, contentPath path.Path, logger *zap.SugaredLogger) (newContentPath path.ImmutablePath, continueProcessing bool, hadMatchingRule bool) {
	// contentPath is the full ipfs path to the requested resource,
	// regardless of whether path or subdomain resolution is used.
	rootPath, err := getRootPath(immutableContentPath)
	if err != nil {
		err = fmt.Errorf("trouble processing _redirects path %q: %w", immutableContentPath.String(), err)
		i.webError(w, r, err, http.StatusInternalServerError)
		return path.ImmutablePath{}, false, true
	}

	redirectsPath, err := path.Join(rootPath, "_redirects")
	if err != nil {
		err = fmt.Errorf("trouble processing _redirects path %q: %w", rootPath.String(), err)
		i.webError(w, r, err, http.StatusInternalServerError)
		return path.ImmutablePath{}, false, true
	}

	imRedirectsPath, err := path.NewImmutablePath(redirectsPath)
	if err != nil {
		err = fmt.Errorf("trouble processing _redirects path %q: %w", redirectsPath, err)
		i.webError(w, r, err, http.StatusInternalServerError)
		return path.ImmutablePath{}, false, true
	}

	foundRedirect, redirectRules, err := i.getRedirectRules(r, imRedirectsPath)
	if err != nil {
		err = fmt.Errorf("trouble processing _redirects file at %q: %w", redirectsPath, err)
		i.webError(w, r, err, http.StatusInternalServerError)
		return path.ImmutablePath{}, false, true
	}

	if foundRedirect {
		redirected, newPath, err := i.handleRedirectsFileRules(w, r, immutableContentPath, contentPath, redirectRules, logger)
		if err != nil {
			err = fmt.Errorf("trouble processing _redirects file at %q: %w", redirectsPath, err)
			i.webError(w, r, err, http.StatusInternalServerError)
			return path.ImmutablePath{}, false, true
		}

		if redirected {
			return path.ImmutablePath{}, false, true
		}

		// 200 is treated as a rewrite, so update the path and continue
		if newPath != "" {
			// Reassign contentPath and resolvedPath since the URL was rewritten
			p, err := path.NewPath(newPath)
			if err != nil {
				err = fmt.Errorf("could not use _redirects file to %q: %w", p, err)
				i.webError(w, r, err, http.StatusInternalServerError)
				return path.ImmutablePath{}, false, true
			}
			imPath, err := path.NewImmutablePath(p)
			if err != nil {
				err = fmt.Errorf("could not use _redirects file to %q: %w", p, err)
				i.webError(w, r, err, http.StatusInternalServerError)
				return path.ImmutablePath{}, false, true
			}
			return imPath, true, true
		}
	}

	// No matching rule, paths remain the same, continue regular processing
	return maybeResolvedImPath, true, false
}

func (i *handler) handleRedirectsFileRules(w http.ResponseWriter, r *http.Request, immutableContentPath path.ImmutablePath, cPath path.Path, redirectRules []redirects.Rule, logger *zap.SugaredLogger) (redirected bool, newContentPath string, err error) {
	// Attempt to match a rule to the URL path, and perform the corresponding redirect or rewrite
	pathParts := strings.Split(immutableContentPath.String(), "/")
	if len(pathParts) > 3 {
		// All paths should start with /ipfs/cid/, so get the path after that
		urlPath := "/" + strings.Join(pathParts[3:], "/")
		rootPath := strings.Join(pathParts[:3], "/")
		// Trim off the trailing /
		urlPath = strings.TrimSuffix(urlPath, "/")

		for _, rule := range redirectRules {
			// Error right away if the rule is invalid
			if !rule.MatchAndExpandPlaceholders(urlPath) {
				continue
			}

			// We have a match!

			// Rewrite
			if rule.Status == 200 {
				// Prepend the rootPath
				toPath := rootPath + rule.To
				return false, toPath, nil
			}

			// Or 4xx
			if rule.Status == 404 || rule.Status == 410 || rule.Status == 451 {
				toPath := rootPath + rule.To
				p, err := path.NewPath(toPath)
				if err != nil {
					return true, toPath, err
				}

				imContent4xxPath, err := path.NewImmutablePath(p)
				if err != nil {
					return true, toPath, err
				}

				// While we have the immutable path which is enough to fetch the data we need to track mutability for
				// headers.
				contentPathParts := strings.Split(cPath.String(), "/")
				if len(contentPathParts) <= 3 {
					// Match behavior as with the immutable path
					return false, "", nil
				}
				// All paths should start with /ip(f|n)s/<root>/, so get the path after that
				contentRootPath := strings.Join(contentPathParts[:3], "/")
				content4xxPath, err := path.NewPath(contentRootPath + rule.To)
				if err != nil {
					return true, toPath, err
				}

				err = i.serve4xx(w, r, imContent4xxPath, content4xxPath, rule.Status, logger)
				return true, toPath, err
			}

			// Or redirect
			if rule.Status >= 301 && rule.Status <= 308 {
				redirectURL := rule.To
				// Preserve query parameters from the original request if they exist
				if r.URL.RawQuery != "" {
					u, err := url.Parse(rule.To)
					if err != nil {
						return false, "", err
					}
					// Merge original query parameters into target
					originalQuery, _ := url.ParseQuery(r.URL.RawQuery)
					targetQuery := u.Query()
					for key, values := range originalQuery {
						for _, value := range values {
							targetQuery.Add(key, value)
						}
					}
					// Set the merged query parameters back
					u.RawQuery = targetQuery.Encode()
					redirectURL = u.String()
				}
				http.Redirect(w, r, redirectURL, rule.Status)
				return true, "", nil
			}
		}
	}

	// No redirects matched
	return false, "", nil
}

// getRedirectRules fetches the _redirects file corresponding to a given path
// and returns the rules Returns whether _redirects was found, the rules (if
// they exist) and if there was an error (other than a missing _redirects) If
// there is an error returns (false, nil, err)
func (i *handler) getRedirectRules(r *http.Request, redirectsPath path.ImmutablePath) (bool, []redirects.Rule, error) {
	// Check for _redirects file. Any path resolution failures are ignored and
	// we just assume there's no _redirects file. Note that ignoring these
	// errors also ensures that the use of the empty CID (bafkqaaa) in tests
	// doesn't fail.
	_, redirectsFileGetResp, err := i.backend.Get(r.Context(), redirectsPath)
	if err != nil {
		if isErrNotFound(err) {
			return false, nil, nil
		}
		return false, nil, err
	}
	defer redirectsFileGetResp.Close()

	if redirectsFileGetResp.bytes == nil {
		return false, nil, errors.New(" _redirects is not a file")
	}
	f := redirectsFileGetResp.bytes

	// Parse redirect rules from file
	redirectRules, err := redirects.Parse(f)
	if err != nil {
		return false, nil, fmt.Errorf("could not parse _redirects: %w", err)
	}
	return true, redirectRules, nil
}

// Returns the root CID Path for the given path
func getRootPath(p path.Path) (path.Path, error) {
	parts := strings.Split(p.String(), "/")
	return path.NewPath(gopath.Join("/", p.Namespace(), parts[2]))
}

func (i *handler) serve4xx(w http.ResponseWriter, r *http.Request, content4xxPathImPath path.ImmutablePath, content4xxPath path.Path, status int, logger *zap.SugaredLogger) error {
	pathMetadata, getresp, err := i.backend.Get(r.Context(), content4xxPathImPath)
	if err != nil {
		return err
	}
	defer getresp.Close()

	if getresp.bytes == nil {
		return fmt.Errorf("could not convert node for %d page to file", status)
	}
	content4xxFile := getresp.bytes

	content4xxCid := pathMetadata.LastSegment.RootCid()

	size := getresp.bytesSize

	logger.Debugf("using _redirects: custom %d file at %q", status, content4xxPath)
	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	addCacheControlHeaders(w, r, content4xxPath, 0, time.Time{}, content4xxCid, "")
	w.WriteHeader(status)
	_, err = io.CopyN(w, content4xxFile, size)
	return err
}

func hasOriginIsolation(r *http.Request) bool {
	_, subdomainGw := r.Context().Value(SubdomainHostnameKey).(string)
	_, dnslink := r.Context().Value(DNSLinkHostnameKey).(string)

	if subdomainGw || dnslink {
		return true
	}

	return false
}
