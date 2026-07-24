package keygen

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/binary"
	"errors"
	"math/big"

	"filippo.io/bigmod"
)

// RSA generates an RSA key deterministically from a random secret following the
// [c2sp.org/det-keygen] specification.
//
// The secret must be at least 128 bits long and should be at least 192 bits
// long for multi-user security. The secret should not be reused for other
// purposes.
//
// This function instantiates HMAC_DRBG with SHA-256 according to NIST SP
// 800-90A Rev. 1, and uses it for a procedure equivalent to that in FIPS 186-5,
// Appendix A.1.3. For FIPS 186-5 compliance, the secret must contain at least
// 168, 192, 288, and 384 bits of entropy for 2048+, 3072+, 7680+, and 15360+
// bits, respectively. (3/2 of the required security strength, per SP 800-90A
// Rev. 1, Section 8.6.7 and SP 800-57 Part 1 Rev. 5, Section 5.6.1.1.) SHA-256
// is appropriate for all three curves, as per SP 800-90Ar1, Section 10.1 and SP
// 800-57 Part 1 Rev. 5, Section 5.6.1.2.
//
// The output MAY CHANGE until this package reaches v1.0.0.
//
// [c2sp.org/det-keygen]: https://c2sp.org/det-keygen
func RSA(bits int, secret []byte) (*rsa.PrivateKey, error) {
	if len(secret) < 16 {
		return nil, errors.New("input secret must be at least 128 bits")
	}
	if bits < 2048 || bits > 65520 || bits%16 != 0 {
		return nil, errors.New("key size must be between 2048 and 65520 bits and a multiple of 16")
	}

	personalization := []byte("det RSA key gen")
	personalization = binary.BigEndian.AppendUint16(personalization, uint16(bits))

	drbg := hmacDRBG(secret, personalization)

	for {
		p, err := randomPrime(drbg, bits/2)
		if err != nil {
			return nil, err
		}
		q, err := randomPrime(drbg, bits/2)
		if err != nil {
			return nil, err
		}

		P, err := bigmod.NewModulus(p)
		if err != nil {
			return nil, err
		}
		Q, err := bigmod.NewModulus(q)
		if err != nil {
			return nil, err
		}

		if Q.Nat().ExpandFor(P).Equal(P.Nat()) == 1 {
			return nil, errors.New("generated p == q, random source is broken")
		}

		N, err := bigmod.NewModulusProduct(p, q)
		if err != nil {
			return nil, err
		}
		if N.BitLen() != bits {
			return nil, errors.New("internal error: modulus size incorrect")
		}

		// d can be safely computed as e⁻¹ mod φ(N) where φ(N) = (p-1)(q-1), and
		// indeed that's what both the original RSA paper and the pre-FIPS
		// crypto/rsa implementation did.
		//
		// However, FIPS 186-5, A.1.1(3) requires computing it as e⁻¹ mod λ(N)
		// where λ(N) = lcm(p-1, q-1).
		//
		// This makes d smaller by 1.82 bits on average, which is irrelevant both
		// because we exclusively use the CRT for private operations and because
		// we use constant time windowed exponentiation. On the other hand, it
		// requires computing a GCD of two even numbers, and then a division,
		// both complex variable-time operations.
		λ, err := totient(P, Q)
		if err == errDivisorTooLarge {
			// The divisor is too large, try again with different primes.
			continue
		}
		if err != nil {
			return nil, err
		}

		e := bigmod.NewNat().SetUint(65537)
		d, ok := bigmod.NewNat().InverseVarTime(e, λ)
		if !ok {
			// This checks that GCD(e, lcm(p-1, q-1)) = 1, which is equivalent
			// to checking GCD(e, p-1) = 1 and GCD(e, q-1) = 1 separately in
			// FIPS 186-5, Appendix A.1.3, steps 4.5 and 5.6.
			//
			// We waste a prime by retrying the whole process, since 65537 is
			// probably only a factor of one of p-1 or q-1, but the probability
			// of this check failing is ≈ 2⁻¹⁵, so it doesn't matter.
			if testingOnlyNoInverse != nil {
				testingOnlyNoInverse(P, Q)
			}
			continue
		}

		if e.ExpandFor(λ).Mul(d, λ).IsOne() == 0 {
			return nil, errors.New("internal error: e*d != 1 mod λ(N)")
		}

		// FIPS 186-5, A.1.1(3) requires checking that d > 2^(nlen / 2).
		//
		// The probability of this check failing when d is derived from
		// (e, p, q) is roughly
		//
		//   2^(nlen/2) / λ(N) ≈ 2^(-nlen/2 + 1.82)
		//
		// so less than 2⁻¹²⁶ for keys larger than 256 bits.
		//
		// We still need to check to comply with FIPS 186-5, but knowing it has
		// negligible chance of failure we can defer the check to the end of key
		// generation and return an error if it fails.

		k, err := newPrivateKey(N, 65537, d, P, Q)
		if err != nil {
			return nil, err
		}

		return k, nil
	}
}

var testingOnlyNoInverse func(P, Q *bigmod.Modulus)

func newPrivateKey(N *bigmod.Modulus, e uint, d *bigmod.Nat, P, Q *bigmod.Modulus) (*rsa.PrivateKey, error) {
	priv := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{
			N: new(big.Int).SetBytes(N.Nat().Bytes(N)),
			E: int(e),
		},
		D: new(big.Int).SetBytes(d.Bytes(N)),
		Primes: []*big.Int{
			new(big.Int).SetBytes(P.Nat().Bytes(P)),
			new(big.Int).SetBytes(Q.Nat().Bytes(Q)),
		},
	}
	priv.Precompute()
	if err := priv.Validate(); err != nil {
		return nil, err
	}
	return priv, nil
}

var testingOnlyGCDBitLen func(int)

// errDivisorTooLarge is returned by [totient] when gcd(p-1, q-1) is too large.
var errDivisorTooLarge = errors.New("divisor too large")

// totient computes the Carmichael totient function λ(N) = lcm(p-1, q-1).
func totient(p, q *bigmod.Modulus) (*bigmod.Modulus, error) {
	a, b := p.Nat().SubOne(p), q.Nat().SubOne(q)

	// lcm(a, b) = a×b / gcd(a, b) = a × (b / gcd(a, b))

	// Our GCD requires at least one of the numbers to be odd.
	// We know that a / 2 and b / 2 are odd because p and q are 7 mod 8.
	// For LCM we only need to preserve the larger prime power of each
	// prime factor, so we can shift out 2 from either of them.
	// For odd x, y and m >= n, lcm(x×2, y×2) = lcm(x×2, y).
	b = b.ShiftRightByOne()

	gcd, err := bigmod.NewNat().GCDVarTime(a, b)
	if err != nil {
		return nil, err
	}
	if gcd.IsOdd() == 0 {
		return nil, errors.New("internal error: gcd(a, b) is even")
	}

	// To avoid implementing multiple-precision division, we just try again if
	// the divisor doesn't fit in a single word. This would have a chance of
	// 2⁻⁶⁴ on 64-bit platforms, and 2⁻³² on 32-bit platforms, but testing 2⁻⁶⁴
	// edge cases is impractical, and we'd rather not behave differently on
	// different platforms, so we reject divisors above 2³²-1. Note that we also
	// add back the factor of 2 we shifted out above.
	gcdBitLen := gcd.BitLenVarTime() + 1
	if testingOnlyGCDBitLen != nil {
		testingOnlyGCDBitLen(gcdBitLen)
	}
	if gcdBitLen > 32 {
		return nil, errDivisorTooLarge
	}
	if gcd.IsZero() == 1 || gcd.Bits()[0] == 0 {
		return nil, errors.New("internal error: gcd(a, b) is zero")
	}
	if rem := b.DivShortVarTime(gcd.Bits()[0]); rem != 0 {
		return nil, errors.New("internal error: b is not divisible by gcd(a, b)")
	}

	return bigmod.NewModulusProduct(a.Bytes(p), b.Bytes(q))
}

var testingOnlyRejectedCandidates func(int)

// randomPrime returns a random prime number of the given bit size following
// the process in FIPS 186-5, Appendix A.1.3.
func randomPrime(drbg func([]byte) error, bits int) ([]byte, error) {
	var rejectedCandidates int
	b := make([]byte, (bits+7)/8)
	for {
		if err := drbg(b); err != nil {
			return nil, err
		}
		// Clear the most significant bits to reach the desired size. We use a
		// mask rather than right-shifting b[0] to make it easier to inject test
		// candidates, which can be represented as simple big-endian integers.
		excess := len(b)*8 - bits
		b[0] &= 0b1111_1111 >> excess

		// Don't let the value be too small: set the most significant two bits.
		// Setting the top two bits, rather than just the top bit, means that
		// when two of these values are multiplied together, the result isn't
		// ever one bit short.
		if excess < 7 {
			b[0] |= 0b1100_0000 >> excess
		} else {
			b[0] |= 0b0000_0001
			b[1] |= 0b1000_0000
		}

		// Make the value odd since an even number certainly isn't prime. Also,
		// make (p - 1) / 2 odd to simplify [millerRabinIteration] and the GCD
		// in [totient]. The third bit is set to comply with steps 4.3 and 5.3.
		b[len(b)-1] |= 0b0000_0111

		// We don't need to check for p >= √2 × 2^(bits-1) (steps 4.4 and 5.4)
		// because we set the top two bits above, so
		//
		//   p > 2^(bits-1) + 2^(bits-2) = 3⁄2 × 2^(bits-1) > √2 × 2^(bits-1)
		//

		// Step 5.5 requires checking that |p - q| > 2^(nlen/2 - 100).
		//
		// The probability of |p - q| ≤ k where p and q are uniformly random in
		// the range (a, b) is 1 - (b-a-k)^2 / (b-a)^2, so the probability of
		// this check failing during key generation is 2⁻⁹⁷.
		//
		// We still need to check to comply with FIPS 186-5, but knowing it has
		// negligible chance of failure we can defer the check to the end of key
		// generation and return an error if it fails. See [checkPrivateKey].

		if isPrime(b) {
			if testingOnlyRejectedCandidates != nil {
				testingOnlyRejectedCandidates(rejectedCandidates)
			}
			return b, nil
		}
		rejectedCandidates++
	}
}

// isPrime runs the Miller-Rabin Probabilistic Primality Test from
// FIPS 186-5, Appendix B.3.1.
//
// w must be a random integer equal to 3 mod 4 in big-endian order.
// isPrime might return false positives for adversarially chosen values.
//
// isPrime is not constant-time.
func isPrime(w []byte) bool {
	mr, err := millerRabinSetup(w)
	if err != nil {
		// w is zero, one, or even.
		return false
	}

	// Before Miller-Rabin, rule out most composites with trial divisions.
	for i := 0; i < len(primes); i += 3 {
		p1, p2, p3 := primes[i], primes[i+1], primes[i+2]
		r := mr.w.Nat().DivShortVarTime(p1 * p2 * p3)
		if r%p1 == 0 || r%p2 == 0 || r%p3 == 0 {
			return false
		}
	}

	// iterations is the number of Miller-Rabin rounds, each with a
	// randomly-selected base.
	//
	// The worst case false positive rate for a single iteration is 1/4 per
	// https://eprint.iacr.org/2018/749, so if w were selected adversarially, we
	// would need up to 64 iterations to get to a negligible (2⁻¹²⁸) chance of
	// false positive.
	//
	// However, since this function is only used for randomly-selected w in the
	// context of RSA key generation, we can use a smaller number of iterations.
	// The exact number depends on the size of the prime (and the implied
	// security level). See BoringSSL for the full formula.
	// https://cs.opensource.google/boringssl/boringssl/+/master:crypto/fipsmodule/bn/prime.c.inc;l=208-283;drc=3a138e43
	bits := mr.w.BitLen()
	var iterations int
	switch {
	case bits >= 3747:
		iterations = 3
	case bits >= 1345:
		iterations = 4
	case bits >= 476:
		iterations = 5
	case bits >= 400:
		iterations = 6
	case bits >= 347:
		iterations = 7
	case bits >= 308:
		iterations = 8
	case bits >= 55:
		iterations = 27
	default:
		iterations = 34
	}

	b := make([]byte, (bits+7)/8)
	for {
		rand.Read(b)
		excess := len(b)*8 - bits
		b[0] &= 0b1111_1111 >> excess
		result, err := millerRabinIteration(mr, b)
		if err != nil {
			// b was rejected.
			continue
		}
		if result == millerRabinCOMPOSITE {
			return false
		}
		iterations--
		if iterations == 0 {
			return true
		}
	}
}

// primes are the first prime numbers (except 2), such that the product of any
// three primes fits in a uint32.
//
// More primes cause fewer Miller-Rabin tests of composites (nothing can help
// with the final test on the actual prime) but have diminishing returns: these
// 255 primes catch 84.9% of composites, the next 255 would catch 1.5% more.
// Adding primes can still be marginally useful since they only compete with the
// (much more expensive) first Miller-Rabin round for candidates that were not
// rejected by the previous primes.
var primes = []uint{
	3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
	59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127,
	131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199,
	211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283,
	293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383,
	389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467,
	479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577,
	587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661,
	673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769,
	773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877,
	881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983,
	991, 997, 1009, 1013, 1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069,
	1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181,
	1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283,
	1289, 1291, 1297, 1301, 1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373, 1381, 1399,
	1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451, 1453, 1459, 1471, 1481, 1483, 1487,
	1489, 1493, 1499, 1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579, 1583,
	1597, 1601, 1607, 1609, 1613, 1619,
}

type millerRabin struct {
	w *bigmod.Modulus
	m []byte
}

// millerRabinSetup prepares state that's reused across multiple iterations of
// the Miller-Rabin test.
func millerRabinSetup(w []byte) (*millerRabin, error) {
	mr := &millerRabin{}

	// Check that w is 3 mod 4, and precompute Montgomery parameters.
	if len(w) == 0 || w[len(w)-1]&0b11 != 0b11 {
		return nil, errors.New("candidate is not 3 mod 4")
	}
	wm, err := bigmod.NewModulus(w)
	if err != nil {
		return nil, err
	}
	mr.w = wm

	// Compute m = (w-1)/2^a, where m is odd.
	// Since w is 3 mod 4, a is always 1, so m = (w-1)/2.
	m := mr.w.Nat().SubOne(mr.w).ShiftRightByOne()

	// Store mr.m as a big-endian byte slice with leading zero bytes removed,
	// for use with [bigmod.Nat.Exp].
	mr.m = m.Bytes(mr.w)
	for mr.m[0] == 0 {
		mr.m = mr.m[1:]
	}

	return mr, nil
}

const millerRabinCOMPOSITE = false
const millerRabinPOSSIBLYPRIME = true

func millerRabinIteration(mr *millerRabin, bb []byte) (bool, error) {
	// Reject b ≤ 1 or b ≥ w − 1.
	if len(bb) != (mr.w.BitLen()+7)/8 {
		return false, errors.New("incorrect length")
	}
	b := bigmod.NewNat()
	if _, err := b.SetBytes(bb, mr.w); err != nil {
		return false, err
	}
	if b.IsZero() == 1 || b.IsOne() == 1 || b.IsMinusOne(mr.w) == 1 {
		return false, errors.New("out-of-range candidate")
	}

	// Compute b^(m*2^i) mod w for successive i.
	// If b^m mod w = 1, b is a possible prime.
	// If b^(m*2^i) mod w = -1 for some 0 <= i < a, b is a possible prime.
	// Otherwise b is composite.

	// Since a = 1 for our w, we only need to check b^m mod w = +/-1.
	z := bigmod.NewNat().Exp(b, mr.m, mr.w)
	if z.IsOne() == 1 || z.IsMinusOne(mr.w) == 1 {
		return millerRabinPOSSIBLYPRIME, nil
	}

	return millerRabinCOMPOSITE, nil
}
