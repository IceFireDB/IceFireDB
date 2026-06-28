package ipfs_synckv

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
)

var errCiphertextTooShort = errors.New("ipfs-synckv: ciphertext shorter than nonce")

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func encrypt(data []byte, key []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, data, nil), nil
}

func decrypt(data []byte, key []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, errCiphertextTooShort
	}
	nonce, cipherText := data[:nonceSize], data[nonceSize:]
	return gcm.Open(nil, nonce, cipherText, nil)
}
