//go:build go1.25

package keygen

import (
	"crypto/ecdsa"
	"crypto/elliptic"
)

func privateKey(curve elliptic.Curve, data []byte) (*ecdsa.PrivateKey, error) {
	return ecdsa.ParseRawPrivateKey(curve, data)
}
