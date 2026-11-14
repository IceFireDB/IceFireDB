//go:build !go1.25

package keygen

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"math/big"
)

func privateKey(curve elliptic.Curve, d []byte) (*ecdsa.PrivateKey, error) {
	priv := new(ecdsa.PrivateKey)
	priv.PublicKey.Curve = curve
	priv.D = new(big.Int).SetBytes(d)
	priv.PublicKey.X, priv.PublicKey.Y = curve.ScalarBaseMult(d)
	return priv, nil
}
