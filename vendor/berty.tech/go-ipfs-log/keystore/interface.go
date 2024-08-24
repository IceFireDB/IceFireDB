package keystore // import "berty.tech/go-ipfs-log/keystore"

import (
	"context"

	crypto "github.com/libp2p/go-libp2p-core/crypto"
)

type Interface interface {
	HasKey(ctx context.Context, id string) (bool, error)

	CreateKey(ctx context.Context, id string) (crypto.PrivKey, error)

	GetKey(ctx context.Context, id string) (crypto.PrivKey, error)

	Sign(pubKey crypto.PrivKey, bytes []byte) ([]byte, error)

	Verify(signature []byte, publicKey crypto.PubKey, data []byte) error
}
