package keygen

import (
	"crypto/hmac"
	"crypto/sha256"
)

// hmacDRBG implements HMAC_DRBG instantiated with SHA-256.
//
// It returns a function instead of an io.Reader because the size of each
// request influences the output, so two 32-byte requests are not equivalent to
// one 64-byte request.
func hmacDRBG(entropy, personalization []byte) func([]byte) error {
	// V = 0x01 0x01 0x01 ... 0x01
	V := make([]byte, sha256.Size)
	for i := range V {
		V[i] = 0x01
	}

	// K = 0x00 0x00 0x00 ... 0x00
	K := make([]byte, sha256.Size)

	// K = HMAC_K(V || 0x00 || entropy || personalization)
	h := hmac.New(sha256.New, K)
	h.Write(V)
	h.Write([]byte{0x00})
	h.Write(entropy)
	h.Write(personalization)
	K = h.Sum(K[:0])

	// V = HMAC_K(V)
	h = hmac.New(sha256.New, K)
	h.Write(V)
	V = h.Sum(V[:0])

	firstLoop := true
	return func(b []byte) error {
		if firstLoop {
			// K = HMAC_K(V || 0x01 || entropy || personalization)
			h.Reset()
			h.Write(V)
			h.Write([]byte{0x01})
			h.Write(entropy)
			h.Write(personalization)
			K = h.Sum(K[:0])

			firstLoop = false
		} else {
			// K = HMAC_K(V || 0x00)
			h.Reset()
			h.Write(V)
			h.Write([]byte{0x00})
			K = h.Sum(K[:0])
		}

		// V = HMAC_K(V)
		h = hmac.New(sha256.New, K)
		h.Write(V)
		V = h.Sum(V[:0])

		tlen := 0
		for tlen < len(b) {
			// V = HMAC_K(V)
			// T = T || V
			h.Reset()
			h.Write(V)
			V = h.Sum(V[:0])
			tlen += copy(b[tlen:], V)
		}
		return nil
	}
}
