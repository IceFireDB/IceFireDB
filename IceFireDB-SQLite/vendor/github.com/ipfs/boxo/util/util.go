// Package util implements various utility functions used within ipfs
// that do not currently have a better place to live.
package util

import (
	"crypto/subtle"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	b58 "github.com/mr-tron/base58/base58"
	mh "github.com/multiformats/go-multihash"
)

// DefaultIpfsHash is the current default hash function used by IPFS.
const DefaultIpfsHash = mh.SHA2_256

// Debug is a global flag for debugging.
var Debug bool

// ErrNotImplemented signifies a function has not been implemented yet.
var ErrNotImplemented = errors.New("error: not implemented yet")

// ErrTimeout implies that a timeout has been triggered
var ErrTimeout = errors.New("error: call timed out")

// ErrSearchIncomplete implies that a search type operation didn't
// find the expected node, but did find 'a' node.
var ErrSearchIncomplete = errors.New("error: search incomplete")

// ErrCast is returned when a cast fails AND the program should not panic.
func ErrCast() error {
	debug.PrintStack()
	return errCast
}

var errCast = errors.New("cast error")

// ExpandPathnames takes a set of paths and turns them into absolute paths
func ExpandPathnames(paths []string) ([]string, error) {
	var out []string
	for _, p := range paths {
		abspath, err := filepath.Abs(p)
		if err != nil {
			return nil, err
		}
		out = append(out, abspath)
	}
	return out, nil
}

// NewTimeSeededRand returns a random bytes reader
// which has been initialized with the current time.
//
// Deprecated: use github.com/ipfs/go-test/random instead.
func NewTimeSeededRand() io.Reader {
	return NewSeededRand(time.Now().UnixNano())
}

// NewSeededRand returns a random bytes reader
// initialized with the given seed.
//
// Deprecated: use github.com/ipfs/go-test/random instead.
func NewSeededRand(seed int64) io.Reader {
	return rand.New(rand.NewSource(seed))
}

// GetenvBool is the way to check an env var as a boolean
func GetenvBool(name string) bool {
	v := strings.ToLower(os.Getenv(name))
	return v == "true" || v == "t" || v == "1"
}

// Partition splits a subject 3 parts: prefix, separator, suffix.
// The first occurrence of the separator will be matched.
// ie. Partition("Ready, steady, go!", ", ") -> ["Ready", ", ", "steady, go!"]
func Partition(subject string, sep string) (string, string, string) {
	if i := strings.Index(subject, sep); i != -1 {
		return subject[:i], subject[i : i+len(sep)], subject[i+len(sep):]
	}
	return subject, "", ""
}

// RPartition splits a subject 3 parts: prefix, separator, suffix.
// The last occurrence of the separator will be matched.
// ie. RPartition("Ready, steady, go!", ", ") -> ["Ready, steady", ", ", "go!"]
func RPartition(subject string, sep string) (string, string, string) {
	if i := strings.LastIndex(subject, sep); i != -1 {
		return subject[:i], subject[i : i+len(sep)], subject[i+len(sep):]
	}
	return subject, "", ""
}

// Hash is the global IPFS hash function. uses multihash SHA2_256, 256 bits
func Hash(data []byte) mh.Multihash {
	h, err := mh.Sum(data, DefaultIpfsHash, -1)
	if err != nil {
		// this error can be safely ignored (panic) because multihash only fails
		// from the selection of hash function. If the fn + length are valid, it
		// won't error.
		panic("multihash failed to hash using SHA2_256.")
	}
	return h
}

// IsValidHash checks whether a given hash is valid (b58 decodable, len > 0)
func IsValidHash(s string) bool {
	out, err := b58.Decode(s)
	if err != nil {
		return false
	}
	_, err = mh.Cast(out)
	return err == nil
}

// XOR takes two byte slices, XORs them together, returns the resulting slice.
func XOR(a, b []byte) []byte {
	_ = b[len(a)-1] // keeping same behaviour as previously but this looks like a bug

	c := make([]byte, len(a))
	subtle.XORBytes(c, a, b)
	return c
}
