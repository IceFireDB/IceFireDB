package enc

import (
	"berty.tech/go-ipfs-log/errmsg"
	"crypto/rand"
	"fmt"
	"golang.org/x/crypto/nacl/secretbox"
	"golang.org/x/crypto/sha3"
)

const (
	SecretBoxNonceSize = 24
	SecretBoxKeySize   = 32
)

var (
	ErrInvalidKey    = fmt.Errorf("invalid key")
	ErrInvalidNonce  = fmt.Errorf("invalid nonce")
	ErrCannotDecrypt = fmt.Errorf("unable to decrypt message")
	ErrCannotEncrypt = fmt.Errorf("unable to encrypt message")
)

type boxed [SecretBoxKeySize]byte

func (s *boxed) DeriveNonce(input []byte) ([]byte, error) {
	nonce := make([]byte, SecretBoxNonceSize)
	hash := sha3.New256()

	if _, err := hash.Write(input); err != nil {
		return nil, errmsg.ErrEncrypt.Wrap(fmt.Errorf("unable to compute a valid nonce: %w", err))
	}

	sum := hash.Sum(nil)

	for i := 0; i < SecretBoxNonceSize; i++ {
		nonce[i] = sum[i]
	}

	return nonce, nil
}

func (s *boxed) Open(payload []byte) ([]byte, error) {
	if len(payload) < (secretbox.Overhead + SecretBoxNonceSize) {
		return nil, ErrCannotDecrypt
	}

	return s.OpenWithNonce(payload[SecretBoxNonceSize:], payload[0:SecretBoxNonceSize])
}

func (s *boxed) Seal(encrypted []byte) ([]byte, error) {
	var nonce = make([]byte, SecretBoxNonceSize)

	size, err := rand.Read(nonce[:])
	if size != SecretBoxNonceSize {
		err = fmt.Errorf("size read: %d (required %d)", size, SecretBoxNonceSize)
	}
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	sealed, err := s.SealWithNonce(encrypted, nonce)
	if err != nil {
		return nil, err
	}

	return append(nonce, sealed...), nil
}

func (s *boxed) OpenWithNonce(payload []byte, nonce []byte) ([]byte, error) {
	if len(nonce) != SecretBoxNonceSize {
		return nil, ErrInvalidNonce
	}

	nonceArr := [SecretBoxNonceSize]byte{}
	for i, c := range nonce {
		nonceArr[i] = c
	}

	dec, ok := secretbox.Open(nil, payload, &nonceArr, (*[32]byte)(s))
	if !ok || dec == nil {
		return nil, ErrCannotDecrypt
	}

	return dec, nil
}

func (s *boxed) SealWithNonce(encrypted []byte, nonce []byte) ([]byte, error) {
	if len(nonce) != SecretBoxNonceSize {
		return nil, ErrInvalidNonce
	}

	nonceArr := [SecretBoxNonceSize]byte{}
	for i, c := range nonce {
		nonceArr[i] = c
	}

	enc := secretbox.Seal(nil, encrypted, &nonceArr, (*[32]byte)(s))
	if enc == nil {
		return nil, ErrCannotEncrypt
	}

	return enc, nil
}

func NewSecretbox(key []byte) (SharedKey, error) {
	if len(key) != SecretBoxKeySize {
		return nil, ErrInvalidKey
	}

	var keyArr [SecretBoxKeySize]byte
	for i, c := range key {
		keyArr[i] = c
	}

	return (*boxed)(&keyArr), nil
}
