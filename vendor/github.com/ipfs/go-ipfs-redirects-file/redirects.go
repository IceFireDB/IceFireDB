// Package redirects provides Netlify style _redirects file format parsing.
package redirects

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/ucarion/urlpath"
)

// 64 KiB
const MaxFileSizeInBytes = 65536

// A Rule represents a single redirection or rewrite rule.
type Rule struct {
	// From is the path which is matched to perform the rule.
	From string

	// To is the destination which may be relative, or absolute
	// in order to proxy the request to another URL.
	To string

	// Status is one of the following:
	//
	// - 3xx a redirect
	// - 200 a rewrite
	// - defaults to 301 redirect
	//
	Status int
}

// IsRewrite returns true if the rule represents a rewrite (status 200).
func (r *Rule) IsRewrite() bool {
	return r.Status == 200
}

// IsProxy returns true if it's a proxy rule (aka contains a hostname).
func (r *Rule) IsProxy() bool {
	u, err := url.Parse(r.To)
	if err != nil {
		return false
	}

	return u.Host != ""
}

// MatchAndExpandPlaceholders expands placeholders in `r.To` and returns true if the provided path matches.
// Otherwise it returns false.
func (r *Rule) MatchAndExpandPlaceholders(urlPath string) bool {
	// get rule.From, trim trailing slash, ...
	fromPath := urlpath.New(strings.TrimSuffix(r.From, "/"))
	match, ok := fromPath.Match(urlPath)

	if !ok {
		return false
	}

	// We have a match!  Perform substitution and return the updated rule
	toPath := r.To
	toPath = replacePlaceholders(toPath, match)
	toPath = replaceSplat(toPath, match)

	r.To = toPath

	return true
}

func replacePlaceholders(to string, match urlpath.Match) string {
	if len(match.Params) > 0 {
		for key, value := range match.Params {
			to = strings.ReplaceAll(to, ":"+key, value)
		}
	}

	return to
}

func replaceSplat(to string, match urlpath.Match) string {
	return strings.ReplaceAll(to, ":splat", match.Trailing)
}

// Must parse utility.
func Must(v []Rule, err error) []Rule {
	if err != nil {
		panic(err)
	}

	return v
}

// Parse the given reader.
func Parse(r io.Reader) (rules []Rule, err error) {
	limiter := &io.LimitedReader{R: r, N: MaxFileSizeInBytes + 1}
	s := bufio.NewScanner(limiter)
	for s.Scan() {
		// detect when we've read one byte beyond MaxFileSizeInBytes
		// and return user-friendly error
		if limiter.N <= 0 {
			return nil, fmt.Errorf("redirects file size cannot exceed %d bytes", MaxFileSizeInBytes)
		}

		line := strings.TrimSpace(s.Text())

		// empty
		if line == "" {
			continue
		}

		// comment
		if strings.HasPrefix(line, "#") {
			continue
		}

		// fields
		fields := strings.Fields(line)

		// missing dst
		if len(fields) <= 1 {
			return nil, fmt.Errorf("missing 'to' path")
		}

		if len(fields) > 3 {
			return nil, fmt.Errorf("must match format 'from to [status]'")
		}

		// implicit status
		rule := Rule{Status: 301}

		// from (must parse as an absolute path)
		from, err := parseFrom(fields[0])
		if err != nil {
			return nil, errors.Wrapf(err, "parsing 'from'")
		}
		rule.From = from

		// to (must parse as an absolute path or an URL)
		to, err := parseTo(fields[1])
		if err != nil {
			return nil, errors.Wrapf(err, "parsing 'to'")
		}
		rule.To = to

		// status
		if len(fields) > 2 {
			code, err := parseStatus(fields[2])
			if err != nil {
				return nil, errors.Wrapf(err, "parsing status %q", fields[2])
			}

			rule.Status = code
		}

		rules = append(rules, rule)
	}

	err = s.Err()
	if err != nil {
		return nil, err
	}
	return rules, nil
}

// ParseString parses the given string.
func ParseString(s string) ([]Rule, error) {
	return Parse(strings.NewReader(s))
}

func parseFrom(s string) (string, error) {
	// enforce a single splat
	fromSplats := strings.Count(s, "*")
	if fromSplats > 0 {
		if !strings.HasSuffix(s, "*") {
			return "", fmt.Errorf("path must end with asterisk")
		}
		if fromSplats > 1 {
			return "", fmt.Errorf("path can have at most one asterisk")
		}
	}

	// confirm value is within URL path spec
	_, err := url.Parse(s)
	if err != nil {
		return "", err
	}

	if !strings.HasPrefix(s, "/") {
		return "", fmt.Errorf("path must begin with '/'")
	}
	return s, nil
}

func parseTo(s string) (string, error) {
	// confirm value is within URL path spec
	u, err := url.Parse(s)
	if err != nil {
		return "", err
	}

	// if the value is  a patch attached to full URL, only allow safelisted schemes
	if !strings.HasPrefix(s, "/") {
		if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "ipfs" && u.Scheme != "ipns" {
			return "", fmt.Errorf("invalid URL scheme")
		}
	}

	return s, nil
}

// parseStatus returns the status code.
func parseStatus(s string) (code int, err error) {
	if strings.HasSuffix(s, "!") {
		// See https://docs.netlify.com/routing/redirects/rewrites-proxies/#shadowing
		return 0, fmt.Errorf("forced redirects (or \"shadowing\") are not supported")
	}

	code, err = strconv.Atoi(s)
	if err != nil {
		return 0, err
	}

	if !isValidStatusCode(code) {
		return 0, fmt.Errorf("status code %d is not supported", code)
	}

	return code, nil
}

func isValidStatusCode(status int) bool {
	switch status {
	case 200, 301, 302, 303, 307, 308, 404, 410, 451:
		return true
	}
	return false
}
