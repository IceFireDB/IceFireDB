// Package verifcid validates CIDs against configurable hash function
// allowlists.
//
// [ValidateCid] checks that a CID's multihash uses an allowed hash function
// and that the digest size falls within the permitted range. The
// [DefaultAllowlist] permits common secure hash functions (SHA2, SHA3, BLAKE2,
// BLAKE3) and identity CIDs with constrained digest sizes.
//
// # Custom Allowlists
//
// Use [NewAllowlist] to build a custom set of allowed hash functions, or
// [NewOverridingAllowlist] to extend an existing allowlist with overrides.
package verifcid
