package identityprovider // import "berty.tech/go-ipfs-log/identityprovider"

import (
	"context"
	"encoding/hex"

	"github.com/btcsuite/btcd/btcec"
	"github.com/libp2p/go-libp2p/core/crypto"

	"berty.tech/go-ipfs-log/errmsg"
	"berty.tech/go-ipfs-log/keystore"
)

var supportedTypes = map[string]func(*CreateIdentityOptions) Interface{
	"orbitdb": NewOrbitDBIdentityProvider,
}

type Identities struct {
	keyStore keystore.Interface
}

func getHandlerFor(typeName string) (func(*CreateIdentityOptions) Interface, error) {
	if !IsSupported(typeName) {
		return nil, errmsg.ErrIdentityProviderNotSupported
	}

	return supportedTypes[typeName], nil
}

func newIdentities(keyStore keystore.Interface) *Identities {
	return &Identities{
		keyStore: keyStore,
	}
}

func (i *Identities) Sign(ctx context.Context, identity *Identity, data []byte) ([]byte, error) {
	privKey, err := i.keyStore.GetKey(ctx, identity.ID)
	if err != nil {
		return nil, errmsg.ErrKeyNotInKeystore.Wrap(err)
	}

	sig, err := i.keyStore.Sign(privKey, data)
	if err != nil {
		return nil, errmsg.ErrSigSign.Wrap(err)
	}

	return sig, nil
}

// Verify checks a signature.
func (i *Identities) Verify(signature []byte, publicKey crypto.PubKey, data []byte) (bool, error) {
	// TODO: Check why this is related to an identity
	return publicKey.Verify(data, signature)
}

//type MigrateOptions struct {
//	TargetPath string
//	TargetID   string
//}

func compressedToUncompressedS256Key(pubKeyBytes []byte) ([]byte, error) {
	pubKey, err := btcec.ParsePubKey(pubKeyBytes, btcec.S256())
	if err != nil {
		return nil, errmsg.ErrNotSecp256k1PubKey.Wrap(err)
	}

	if !btcec.IsCompressedPubKey(pubKeyBytes) {
		return pubKeyBytes, nil
	}

	return pubKey.SerializeUncompressed(), nil
}

// CreateIdentity creates a new Identity.
func (i *Identities) CreateIdentity(ctx context.Context, options *CreateIdentityOptions) (*Identity, error) {
	NewIdentityProvider, err := getHandlerFor(options.Type)
	if err != nil {
		return nil, errmsg.ErrIdentityProviderNotSupported.Wrap(err)
	}

	identityProvider := NewIdentityProvider(options)
	id, err := identityProvider.GetID(ctx, options)
	if err != nil {
		return nil, errmsg.ErrIdentityUnknown.Wrap(err)
	}

	// FIXME ?
	//if options.Migrate != nil {
	//	if err := options.Migrate(&MigrateOptions{ TargetPath: i.keyStore.Path, TargetID: id }); err != nil {
	//		return nil, err
	//	}
	//}

	publicKey, idSignature, err := i.signID(ctx, id)
	if err != nil {
		return nil, errmsg.ErrSigSign.Wrap(err)
	}

	publicKeyBytes, err := publicKey.Raw()
	if err != nil {
		return nil, errmsg.ErrNotSecp256k1PubKey.Wrap(err)
	}

	// JS version of IPFS Log expects an uncompressed Secp256k1 key
	if publicKey.Type().String() == "Secp256k1" {
		publicKeyBytes, err = compressedToUncompressedS256Key(publicKeyBytes)
		if err != nil {
			return nil, errmsg.ErrNotSecp256k1PubKey.Wrap(err)
		}
	}

	pubKeyIDSignature, err := identityProvider.SignIdentity(ctx, append(publicKeyBytes, idSignature...), options.ID)
	if err != nil {
		return nil, errmsg.ErrIdentityCreationFailed.Wrap(err)
	}

	return &Identity{
		ID:        id,
		PublicKey: publicKeyBytes,
		Signatures: &IdentitySignature{
			ID:        idSignature,
			PublicKey: pubKeyIDSignature,
		},
		Type:     identityProvider.GetType(),
		Provider: identityProvider,
	}, nil
}

func (i *Identities) signID(ctx context.Context, id string) (crypto.PubKey, []byte, error) {
	privKey, err := i.keyStore.GetKey(ctx, id)
	if err != nil {
		privKey, err = i.keyStore.CreateKey(ctx, id)

		if err != nil {
			return nil, nil, errmsg.ErrSigSign.Wrap(err)
		}
	}

	idSignature, err := i.keyStore.Sign(privKey, []byte(id))
	if err != nil {
		return nil, nil, errmsg.ErrSigSign.Wrap(err)
	}

	return privKey.GetPublic(), idSignature, nil
}

// VerifyIdentity checks an identity.
func (i *Identities) VerifyIdentity(identity *Identity) error {
	pubKey, err := identity.GetPublicKey()
	if err != nil {
		return errmsg.ErrPubKeyDeserialization.Wrap(err)
	}

	idBytes, err := hex.DecodeString(identity.ID)
	if err != nil {
		return errmsg.ErrIdentityDeserialization.Wrap(err)
	}

	err = i.keyStore.Verify(
		identity.Signatures.ID,
		pubKey,
		idBytes,
	)
	if err != nil {
		return errmsg.ErrSigNotVerified.Wrap(err)
	}

	identityProvider, err := getHandlerFor(identity.Type)
	if err != nil {
		return errmsg.ErrSigNotVerified.Wrap(err)
	}

	return identityProvider(nil).VerifyIdentity(identity)
}

// CreateIdentity creates a new identity.
func CreateIdentity(ctx context.Context, options *CreateIdentityOptions) (*Identity, error) {
	ks := options.Keystore
	if ks == nil {
		return nil, errmsg.ErrKeystoreNotDefined
	}

	identities := newIdentities(ks)

	return identities.CreateIdentity(ctx, options)
}

// IsSupported checks if an identity type is supported.
func IsSupported(typeName string) bool {
	_, ok := supportedTypes[typeName]

	return ok
}

// AddIdentityProvider registers an new identity provider.
func AddIdentityProvider(identityProvider func(*CreateIdentityOptions) Interface) error {
	if identityProvider == nil {
		return errmsg.ErrIdentityProviderNotDefined
	}

	supportedTypes[identityProvider(nil).GetType()] = identityProvider

	return nil
}

// RemoveIdentityProvider unregisters an identity provider.
func RemoveIdentityProvider(name string) {
	delete(supportedTypes, name)
}
