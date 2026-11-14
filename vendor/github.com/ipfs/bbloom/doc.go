// Package bbloom implements a fast bloom filter with a real bitset.
//
// A Bloom filter is a space-efficient probabilistic data structure used to
// test set membership. It may return false positives but never false negatives:
// [Bloom.Has] returning true means the entry was probably added, while false
// means the entry was definitely not added.
//
// This implementation uses an inlined SipHash-2-4 for hashing, rounds the
// bitset up to the next power of two for fast masking, and provides both
// non-thread-safe and mutex-protected (TS-suffixed) variants of all operations.
//
// By default ([New]) the filter uses publicly known SipHash keys. When the
// filter will hold data controlled by untrusted parties, use [NewWithKeys]
// with random secret keys to prevent hash-flooding attacks.
//
// Filters can be serialized to JSON with [Bloom.JSONMarshal] and restored
// with [JSONUnmarshal].
package bbloom
