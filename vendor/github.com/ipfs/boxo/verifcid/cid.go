package verifcid

import (
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

var (
	ErrPossiblyInsecureHashFunction = errors.New("potentially insecure hash functions not allowed")
	ErrBelowMinimumHashLength       = fmt.Errorf("hashes must be at least %d bytes long", minimumHashLength)
	ErrAboveMaximumHashLength       = fmt.Errorf("hashes must be at most %d bytes long", maximumHashLength)
)

const (
	minimumHashLength = 20
	maximumHashLength = 128
)

// ValidateCid validates multihash allowance behind given CID.
func ValidateCid(allowlist Allowlist, c cid.Cid) error {
	pref := c.Prefix()
	if !allowlist.IsAllowed(pref.MhType) {
		return ErrPossiblyInsecureHashFunction
	}

	if pref.MhType != mh.IDENTITY && pref.MhLength < minimumHashLength {
		return ErrBelowMinimumHashLength
	}

	if pref.MhType != mh.IDENTITY && pref.MhLength > maximumHashLength {
		return ErrAboveMaximumHashLength
	}

	return nil
}
