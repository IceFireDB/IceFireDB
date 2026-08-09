package ipfs_synckv

import (
	"bytes"
	"testing"
)

func testKey() []byte {
	// 32 bytes -> AES-256
	return []byte("0123456789abcdef0123456789abcdef")
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key := testKey()
	plain := []byte("hello icefiredb")
	ct, err := encrypt(plain, key)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	got, err := decrypt(ct, key)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round trip = %q, want %q", got, plain)
	}
}

func TestDecryptCorruptReturnsErrorNoPanic(t *testing.T) {
	key := testKey()
	// Too short to even contain a nonce, plus garbage — must not panic.
	for _, bad := range [][]byte{
		nil,
		{0x00},
		[]byte("not a real ciphertext at all"),
	} {
		if _, err := decrypt(bad, key); err == nil {
			t.Fatalf("decrypt(%q) = nil error, want error", bad)
		}
	}
}

func TestDecryptTamperedReturnsError(t *testing.T) {
	key := testKey()
	ct, err := encrypt([]byte("authentic data"), key)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	ct[len(ct)-1] ^= 0xff // flip a byte in the auth tag / ciphertext
	if _, err := decrypt(ct, key); err == nil {
		t.Fatalf("decrypt of tampered ciphertext = nil error, want error")
	}
}

func TestNonceUniqueness(t *testing.T) {
	key := testKey()
	seen := make(map[string]struct{})
	for i := 0; i < 1000; i++ {
		ct, err := encrypt([]byte("x"), key)
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		nonce := string(ct[:12]) // GCM standard nonce size
		if _, dup := seen[nonce]; dup {
			t.Fatalf("duplicate nonce after %d iterations", i)
		}
		seen[nonce] = struct{}{}
	}
}
