package verifcid

import (
	"fmt"

	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// Deprecated: use github.com/ipfs/boxo/verifcid.ErrPossiblyInsecureHashFunction
var ErrPossiblyInsecureHashFunction = fmt.Errorf("potentially insecure hash functions not allowed")

// Deprecated: use github.com/ipfs/boxo/verifcid.ErrBelowMinimumHashLength
var ErrBelowMinimumHashLength = fmt.Errorf("hashes must be at least %d bytes long", minimumHashLength)

// Deprecated: use github.com/ipfs/boxo/verifcid.ErrAboveMaximumHashLength
var ErrAboveMaximumHashLength = fmt.Errorf("hashes must be at most %d bytes long", maximumHashLength)

const minimumHashLength = 20
const maximumHashLength = 128

var goodset = map[uint64]bool{
	mh.SHA2_256:                  true,
	mh.SHA2_512:                  true,
	mh.SHA3_224:                  true,
	mh.SHA3_256:                  true,
	mh.SHA3_384:                  true,
	mh.SHA3_512:                  true,
	mh.SHAKE_256:                 true,
	mh.DBL_SHA2_256:              true,
	mh.KECCAK_224:                true,
	mh.KECCAK_256:                true,
	mh.KECCAK_384:                true,
	mh.KECCAK_512:                true,
	mh.BLAKE3:                    true,
	mh.IDENTITY:                  true,
	mh.POSEIDON_BLS12_381_A1_FC1: true,
	mh.SHA2_256_TRUNC254_PADDED:  true,
	mh.X11:                       true,

	mh.SHA1: true, // not really secure but still useful
}

// Deprecated: use github.com/ipfs/boxo/verifcid.IsGoodHash
func IsGoodHash(code uint64) bool {
	good, found := goodset[code]
	if good {
		return true
	}

	if !found {
		if code >= mh.BLAKE2B_MIN+19 && code <= mh.BLAKE2B_MAX {
			return true
		}
		if code >= mh.BLAKE2S_MIN+19 && code <= mh.BLAKE2S_MAX {
			return true
		}
	}

	return false
}

// Deprecated: use github.com/ipfs/boxo/verifcid.ValidateCid
func ValidateCid(c cid.Cid) error {
	pref := c.Prefix()
	if !IsGoodHash(pref.MhType) {
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
