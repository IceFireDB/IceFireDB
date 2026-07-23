// Package keygen implements deterministic key generation algorithms.
package keygen

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"fmt"
	"io"
	"math/big"

	"filippo.io/bigmod"
)

// ECDSA generates an ECDSA key deterministically from a random secret following
// the [c2sp.org/det-keygen] specification.
//
// The secret must be at least 128 bits long and should be at least 192 bits
// long for multi-user security. The secret should not be reused for other
// purposes.
//
// This function instantiates HMAC_DRBG with SHA-256 according to NIST SP
// 800-90A Rev. 1, and uses it for a procedure equivalent to that in FIPS 186-5,
// Appendix A.2.2. For FIPS 186-5 compliance, the secret must contain at least
// 192, 288, and 384 bits of entropy for P-256, P-384, and P-521, respectively.
// (3/2 of the required security strength, per SP 800-90A Rev. 1, Section 8.6.7
// and SP 800-57 Part 1 Rev. 5, Section 5.6.1.1.) SHA-256 is appropriate for all
// three curves, as per SP 800-90Ar1, Section 10.1 and SP 800-57 Part 1 Rev. 5,
// Section 5.6.1.2.
//
// The output MAY CHANGE until this package reaches v1.0.0.
//
// [c2sp.org/det-keygen]: https://c2sp.org/det-keygen
func ECDSA(c elliptic.Curve, secret []byte) (*ecdsa.PrivateKey, error) {
	if len(secret) < 16 {
		return nil, fmt.Errorf("input secret must be at least 128 bits")
	}

	var personalization string
	switch c {
	case elliptic.P224():
		personalization = "det ECDSA key gen P-224"
	case elliptic.P256():
		personalization = "det ECDSA key gen P-256"
	case elliptic.P384():
		personalization = "det ECDSA key gen P-384"
	case elliptic.P521():
		personalization = "det ECDSA key gen P-521"
	default:
		return nil, fmt.Errorf("unsupported curve %s", c.Params().Name)
	}

	drbg := hmacDRBG(secret, []byte(personalization))

	N, err := bigmod.NewModulus(c.Params().N.Bytes())
	if err != nil {
		return nil, fmt.Errorf("internal error: %v", err)
	}
	b := make([]byte, N.Size())
	if err := drbg(b); err != nil {
		return nil, fmt.Errorf("internal error: HMAC_DRBG error: %v", err)
	}

	// Right shift the bytes buffer to match the bit length of N. It would be
	// safer and easier to mask off the extra bits on the left, but this is more
	// strictly compliant with FIPS 186-5 (which reads the first N bits from the
	// DRBG) and matches what RFC 6979's bits2int does.
	if excess := len(b)*8 - N.BitLen(); excess != 0 {
		// Just to be safe, assert that this only happens for the one curve that
		// doesn't have a round number of bits, and for the expected number of
		// excess bits.
		if c != elliptic.P521() {
			return nil, fmt.Errorf("internal error: unexpectedly masking off bits")
		}
		if excess != 7 {
			return nil, fmt.Errorf("internal error: unexpected excess bits")
		}
		b = rightShift(b, excess)
	}

	// FIPS 186-5, Appendix A.4.2, checks x <= N - 2 and then adds one. Checking
	// 0 < x <= N - 1 is strictly equivalent but is more API-friendly, since
	// SetBytes already checks for overflows and doesn't require an addition.
	// (None of this matters anyway because the chance of selecting zero is
	// cryptographically negligible.) Equivalent processes are explicitly
	// allowed by FIPS 186-5, Appendix A.2.2, Process point 4.
	x := bigmod.NewNat()
	if _, err := x.SetBytes(b, N); err != nil || x.IsZero() == 1 {
		// Only P-256 has a non-negligible chance of looping.
		if c != elliptic.P256() {
			return nil, fmt.Errorf("internal error: unexpected loop")
		}
		if testingOnlyRejectionSamplingLooped != nil {
			testingOnlyRejectionSamplingLooped()
		}

		if err := drbg(b); err != nil {
			return nil, fmt.Errorf("internal error: HMAC_DRBG error: %v", err)
		}
		if _, err := x.SetBytes(b, N); err != nil || x.IsZero() == 1 {
			// The chance of looping twice is cryptographically negligible.
			return nil, fmt.Errorf("internal error: looped twice")
		}
	}

	return privateKey(c, x.Bytes(N))
}

// testingOnlyRejectionSamplingLooped is called when rejection sampling in
// ECDSA rejects a candidate for being higher than the modulus.
var testingOnlyRejectionSamplingLooped func()

// rightShift implements the right shift necessary for bits2int.
func rightShift(b []byte, shift int) []byte {
	if shift < 0 || shift >= 8 {
		panic("ecdsa: internal error: tried to shift by more than 8 bits")
	}
	b = bytes.Clone(b)
	for i := len(b) - 1; i >= 0; i-- {
		b[i] >>= shift
		if i > 0 {
			b[i] |= b[i-1] << (8 - shift)
		}
	}
	return b
}

// ECDSALegacy generates an ECDSA key deterministically from a random stream in
// a way compatible with Go 1.19's ecdsa.GenerateKey.
//
// It uses the procedure given in FIPS 186-5, Appendix A.2.1.
//
// Note that ECDSALegacy may leak bits of the key through timing side-channels.
func ECDSALegacy(c elliptic.Curve, rand io.Reader) (*ecdsa.PrivateKey, error) {
	params := c.Params()
	// Note that for P-521 this will actually be 63 bits more than the order, as
	// division rounds down, but the extra bit is inconsequential and we want to
	// retain compatibility with Go 1.19 as was implemented.
	b := make([]byte, params.N.BitLen()/8+8)
	_, err := io.ReadFull(rand, b)
	if err != nil {
		return nil, err
	}

	one := big.NewInt(1)
	x := new(big.Int).SetBytes(b)
	n := new(big.Int).Sub(params.N, one)
	x.Mod(x, n)
	x.Add(x, one)

	priv := new(ecdsa.PrivateKey)
	priv.PublicKey.Curve = c
	priv.D = x
	priv.PublicKey.X, priv.PublicKey.Y = c.ScalarBaseMult(x.Bytes())
	return priv, nil
}
