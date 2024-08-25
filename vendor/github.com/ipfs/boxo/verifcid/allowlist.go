package verifcid

import (
	mh "github.com/multiformats/go-multihash"
)

// DefaultAllowlist is the default list of hashes allowed in IPFS.
var DefaultAllowlist defaultAllowlist

// Allowlist defines an interface containing list of allowed multihashes.
type Allowlist interface {
	// IsAllowed checks for multihash allowance by the code.
	IsAllowed(code uint64) bool
}

// NewAllowlist constructs new [Allowlist] from the given map set.
func NewAllowlist(allowset map[uint64]bool) Allowlist {
	return allowlist{allowset: allowset}
}

// NewOverridingAllowlist is like [NewAllowlist] but it will fallback to an other [AllowList] if keys are missing.
// If override is nil it will return unsecure for unknown things.
func NewOverridingAllowlist(override Allowlist, allowset map[uint64]bool) Allowlist {
	return allowlist{override, allowset}
}

type allowlist struct {
	override Allowlist
	allowset map[uint64]bool
}

func (al allowlist) IsAllowed(code uint64) bool {
	if good, found := al.allowset[code]; found {
		return good
	}

	if al.override != nil {
		return al.override.IsAllowed(code)
	}

	return false
}

type defaultAllowlist struct{}

func (defaultAllowlist) IsAllowed(code uint64) bool {
	switch code {
	case mh.SHA2_256, mh.SHA2_512,
		mh.SHAKE_256,
		mh.DBL_SHA2_256,
		mh.BLAKE3,
		mh.IDENTITY,

		mh.SHA3_224, mh.SHA3_256, mh.SHA3_384, mh.SHA3_512,
		mh.KECCAK_224, mh.KECCAK_256, mh.KECCAK_384, mh.KECCAK_512,

		mh.SHA1: // not really secure but still useful for git
		return true
	default:
		if code >= mh.BLAKE2B_MIN+19 && code <= mh.BLAKE2B_MAX {
			return true
		}
		if code >= mh.BLAKE2S_MIN+19 && code <= mh.BLAKE2S_MAX {
			return true
		}

		return false
	}
}
