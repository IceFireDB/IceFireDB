package nopfs

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"

	"github.com/multiformats/go-multihash"
)

// Entry represents a rule (or a line) in a denylist file.
type Entry struct {
	Line      uint64
	AllowRule bool
	Hints     map[string]string
	RawValue  string
	Multihash multihash.Multihash // set for ipfs-paths mostly.
	Path      BlockedPath
}

// String provides a single-line representation of the Entry.
func (e Entry) String() string {
	path := e.Path.Path
	if len(path) == 0 {
		path = "(empty)"
	}
	return fmt.Sprintf("Path: %s. Prefix: %t. AllowRule: %t.", path, e.Path.Prefix, e.AllowRule)
}

func (e Entry) Clone() Entry {
	hints := make(map[string]string, len(e.Hints))
	for k, v := range e.Hints {
		hints[k] = v
	}

	return Entry{
		Line:      e.Line,
		AllowRule: e.AllowRule,
		Hints:     hints,
		RawValue:  e.RawValue,
		Multihash: bytes.Clone(e.Multihash),
		Path:      e.Path,
	}
}

// Entries is a slice of Entry.
type Entries []Entry

// CheckPathStatus returns whether the given path has a match in one of the Entries.
func (entries Entries) CheckPathStatus(p string) (Status, Entry) {
	// start by the last one, since latter items have preference.
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		logger.Debugf("check-path: %s matches %s", e.Path.Path, p)
		if e.Path.Matches(p) {
			// if we find a negative rule that matches the path
			// then it is not blocked.
			if e.AllowRule {
				return StatusAllowed, e
			}
			return StatusBlocked, e
		}
	}
	return StatusNotFound, Entry{}
}

// BlockedPath represents the path part of a blocking rule.
type BlockedPath struct {
	Path   string
	Prefix bool
}

// NewBlockedPath takes a raw path, unscapes and sanitizes it, detecting and
// handling wildcards. It may also represent an "allowed" path on "allowed"
// rules.
func NewBlockedPath(rawPath string) (BlockedPath, error) {
	if rawPath == "" {
		return BlockedPath{
			Path:   "",
			Prefix: false,
		}, nil
	}

	// Deal with * before Unescaping, as the path may have an * at the end
	// that has been escaped and should not be interpreted.
	prefix := false
	if strings.HasSuffix(rawPath, "/*") {
		rawPath = strings.TrimSuffix(rawPath, "/*")
		prefix = true
	} else if strings.HasSuffix(rawPath, "*") {
		rawPath = strings.TrimSuffix(rawPath, "*")
		prefix = true
	}

	path, err := url.QueryUnescape(rawPath)
	if err != nil {
		return BlockedPath{}, err
	}

	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")

	return BlockedPath{
		Path:   path,
		Prefix: prefix,
	}, nil
}

// Matches returns whether the given path matched the blocked (or allowed) path.
func (bpath BlockedPath) Matches(path string) bool {
	// sanitize path
	path = strings.TrimSuffix(path, "/")
	path = strings.TrimPrefix(path, "/")

	// Matches all paths
	if bpath.Path == "*" {
		return true
	}

	// No path matches empty paths
	if bpath.Path == "" && path == "" {
		return true
	}

	// Prefix matches prefix
	// otherwise exact match
	// bpath.Path already sanitized
	if bpath.Prefix && strings.HasPrefix(path, bpath.Path) {
		return true
	} else if bpath.Path == path {
		return true
	}

	return false
}
