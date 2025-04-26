package ipfs_synckv

import (
	"encoding/binary"
	"time"

	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
)

// UnixTimePrefixedRandomNonce takes an int for the nonce size and returns a byte slice of length size.
// A byte slice is created for the nonce and filled with random data from `crypto/rand`, then the
// first 4 bytes of the nonce are overwritten with LittleEndian encoding of `time.Now().Unix()`
// The purpose of this function is to avoid an unlikely collision in randomly generating nonces
// by prefixing the nonce with time series data.
func UnixTimePrefixedRandomNonce(size int) []byte {
	nonce := make([]byte, size)
	rand.Read(nonce)
	timeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeBytes, uint64(time.Now().Unix()))
	copy(nonce, timeBytes[:4])
	return nonce
}

func encrypt(data []byte, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}

	nonce := UnixTimePrefixedRandomNonce(gcm.NonceSize())

	cipherText := gcm.Seal(nonce, nonce, data, nil)
	return cipherText
}

func decrypt(data []byte, key []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}
	nonceSize := gcm.NonceSize()
	nonce, cipherText := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, cipherText, nil)
	if err != nil {
		panic(err.Error())
	}
	return plaintext
}
