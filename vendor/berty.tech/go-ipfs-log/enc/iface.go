package enc

type SharedKey interface {
	DeriveNonce(input []byte) ([]byte, error)

	Open(payload []byte) ([]byte, error)
	Seal(encrypted []byte) ([]byte, error)

	OpenWithNonce(payload []byte, nonce []byte) ([]byte, error)
	SealWithNonce(encrypted []byte, nonce []byte) ([]byte, error)
}
