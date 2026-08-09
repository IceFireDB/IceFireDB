package main

import (
	"testing"
)

func TestOpenStorageInvalidBackendReturnsError(t *testing.T) {
	// An unregistered backend name must produce an error, not a nil-returning success.
	cfg, l, d, err := openStorage(t.TempDir(), "no-such-backend-xyz")
	if err == nil {
		if l != nil {
			l.Close()
		}
		t.Fatalf("openStorage with invalid backend = nil error, want error")
	}
	if cfg != nil || l != nil || d != nil {
		t.Fatalf("openStorage error path returned non-nil values: cfg=%v l=%v d=%v", cfg, l, d)
	}
}

func TestOpenStorageValidBackendSucceeds(t *testing.T) {
	cfg, l, d, err := openStorage(t.TempDir(), "goleveldb")
	if err != nil {
		t.Fatalf("openStorage(goleveldb) = %v, want nil", err)
	}
	if cfg == nil || l == nil || d == nil {
		t.Fatalf("openStorage success returned a nil: cfg=%v l=%v d=%v", cfg, l, d)
	}
	l.Close()
}
