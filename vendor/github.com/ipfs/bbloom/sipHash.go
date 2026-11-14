// SPDX-License-Identifier: CC0-1.0
//
// Written in 2012 by Dmitry Chestnykh.
//
// To the extent possible under law, the author have dedicated all copyright
// and related and neighboring rights to this software to the public domain
// worldwide. This software is distributed without any warranty.
// http://creativecommons.org/publicdomain/zero/1.0/
//
// Package siphash implements SipHash-2-4, a fast short-input PRF
// created by Jean-Philippe Aumasson and Daniel J. Bernstein.

package bbloom

// SipHash-2-4 initialization constants.
const (
	sipC0 = 0x736f6d6570736575
	sipC1 = 0x646f72616e646f6d
	sipC2 = 0x6c7967656e657261
	sipC3 = 0x7465646279746573
)

// Default SipHash keys (the original hardcoded values: 0xdeadbeaf, 0xfaebdaed).
const (
	defaultK0 = uint64(0xdeadbeaf)
	defaultK1 = uint64(0xfaebdaed)
)

// sipHash returns the 64-bit SipHash-2-4 of the given byte slice using
// the bloom filter's k0/k1 keys, split into two parts for double-hashing.
func (bl *Bloom) sipHash(p []byte) (l, h uint64) {
	// Initialization.
	v0 := bl.k0 ^ sipC0
	v1 := bl.k1 ^ sipC1
	v2 := bl.k0 ^ sipC2
	v3 := bl.k1 ^ sipC3
	t := uint64(len(p)) << 56

	// Compression.
	for len(p) >= 8 {

		m := uint64(p[0]) | uint64(p[1])<<8 | uint64(p[2])<<16 | uint64(p[3])<<24 |
			uint64(p[4])<<32 | uint64(p[5])<<40 | uint64(p[6])<<48 | uint64(p[7])<<56

		v3 ^= m

		// Round 1.
		v0 += v1
		v1 = v1<<13 | v1>>51
		v1 ^= v0
		v0 = v0<<32 | v0>>32

		v2 += v3
		v3 = v3<<16 | v3>>48
		v3 ^= v2

		v0 += v3
		v3 = v3<<21 | v3>>43
		v3 ^= v0

		v2 += v1
		v1 = v1<<17 | v1>>47
		v1 ^= v2
		v2 = v2<<32 | v2>>32

		// Round 2.
		v0 += v1
		v1 = v1<<13 | v1>>51
		v1 ^= v0
		v0 = v0<<32 | v0>>32

		v2 += v3
		v3 = v3<<16 | v3>>48
		v3 ^= v2

		v0 += v3
		v3 = v3<<21 | v3>>43
		v3 ^= v0

		v2 += v1
		v1 = v1<<17 | v1>>47
		v1 ^= v2
		v2 = v2<<32 | v2>>32

		v0 ^= m
		p = p[8:]
	}

	// Compress last block.
	switch len(p) {
	case 7:
		t |= uint64(p[6]) << 48
		fallthrough
	case 6:
		t |= uint64(p[5]) << 40
		fallthrough
	case 5:
		t |= uint64(p[4]) << 32
		fallthrough
	case 4:
		t |= uint64(p[3]) << 24
		fallthrough
	case 3:
		t |= uint64(p[2]) << 16
		fallthrough
	case 2:
		t |= uint64(p[1]) << 8
		fallthrough
	case 1:
		t |= uint64(p[0])
	}

	v3 ^= t

	// Round 1.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// Round 2.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	v0 ^= t

	// Finalization.
	v2 ^= 0xff

	// Round 1.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// Round 2.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// Round 3.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// Round 4.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// return v0 ^ v1 ^ v2 ^ v3

	hash := v0 ^ v1 ^ v2 ^ v3
	h = hash >> bl.shift
	l = hash << bl.shift >> bl.shift
	if bl.hashVersion >= 1 {
		l |= 1 // force odd so l is coprime to 2^sizeExp (issue #11)
	}
	return l, h

}
