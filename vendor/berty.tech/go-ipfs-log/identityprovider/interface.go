package identityprovider // import "berty.tech/go-ipfs-log/identityprovider"

import (
	"context"

	"berty.tech/go-ipfs-log/keystore"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
)

type CreateIdentityOptions struct {
	IdentityKeysPath string
	Type             string
	Keystore         keystore.Interface
	//Migrate          func(*MigrateOptions) error
	ID string
}

type Interface interface {
	// GetID returns id of identity (to be signed by orbit-db public key).
	GetID(context.Context, *CreateIdentityOptions) (string, error)

	// SignIdentity returns signature of OrbitDB public key signature.
	SignIdentity(ctx context.Context, data []byte, id string) ([]byte, error)

	// GetType returns the type for this identity provider.
	GetType() string

	// VerifyIdentity checks an identity.
	VerifyIdentity(identity *Identity) error

	// Sign will sign a value.
	Sign(ctx context.Context, identity *Identity, bytes []byte) ([]byte, error)

	// UnmarshalPublicKey will provide a crypto.PubKey from a key bytes.
	UnmarshalPublicKey(data []byte) (crypto.PubKey, error)
}
