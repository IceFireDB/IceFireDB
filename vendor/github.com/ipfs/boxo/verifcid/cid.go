package verifcid

import (
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

const (
	// DefaultMinDigestSize is the default minimum size for hash digests (except for identity hashes)
	DefaultMinDigestSize = 20
	// DefaultMaxDigestSize is the default maximum size for cryptographic hash digests.
	// This does not apply to identity hashes which are not cryptographic and use DefaultMaxIdentityDigestSize instead.
	DefaultMaxDigestSize = 128
	// DefaultMaxIdentityDigestSize is the default maximum size for identity CID digests.
	// Identity CIDs (with multihash code 0x00) are not cryptographic hashes - they embed
	// data directly in the CID. This separate limit prevents abuse while allowing
	// different size constraints than cryptographic digests.
	DefaultMaxIdentityDigestSize = 128
)

var (
	ErrPossiblyInsecureHashFunction = errors.New("potentially insecure hash functions not allowed")
	ErrDigestTooSmall               = errors.New("digest too small")
	ErrDigestTooLarge               = errors.New("digest too large")

	// Deprecated: Use ErrDigestTooSmall instead
	ErrBelowMinimumHashLength = ErrDigestTooSmall
	// Deprecated: Use ErrDigestTooLarge instead
	ErrAboveMaximumHashLength = ErrDigestTooLarge
)

// ValidateCid validates multihash allowance behind given CID.
func ValidateCid(allowlist Allowlist, c cid.Cid) error {
	pref := c.Prefix()
	if !allowlist.IsAllowed(pref.MhType) {
		return ErrPossiblyInsecureHashFunction
	}

	minSize := allowlist.MinDigestSize(pref.MhType)
	maxSize := allowlist.MaxDigestSize(pref.MhType)

	if pref.MhLength < minSize {
		return newErrDigestTooSmall(pref.MhType, pref.MhLength, minSize)
	}
	if pref.MhLength > maxSize {
		return newErrDigestTooLarge(pref.MhType, pref.MhLength, maxSize)
	}

	return nil
}

func getHashName(code uint64) string {
	name, ok := mh.Codes[code]
	if !ok {
		return fmt.Sprintf("unknown(%d)", code)
	}
	return name
}

func newErrDigestTooSmall(code uint64, got int, min int) error {
	return fmt.Errorf("%w: %s digest got %d bytes, minimum %d", ErrDigestTooSmall, getHashName(code), got, min)
}

func newErrDigestTooLarge(code uint64, got int, max int) error {
	return fmt.Errorf("%w: %s digest got %d bytes, maximum %d", ErrDigestTooLarge, getHashName(code), got, max)
}
