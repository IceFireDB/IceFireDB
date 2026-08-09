package main

import (
	"bytes"
	"fmt"
	"testing"

	lediscfg "github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store"
	"github.com/ledisdb/ledisdb/store/driver"

	// Register the badger driver so store.Open("badger") works.
	_ "github.com/IceFireDB/IceFireDB/driver/badger"
)

// openTestDriver opens a fresh ledis storage driver of the given backend
// name in a temp dir and returns its low-level driver.IDB.
func openTestDriver(t *testing.T, name string) driver.IDB {
	t.Helper()
	cfg := lediscfg.NewConfigDefault()
	cfg.DataDir = t.TempDir()
	cfg.DBName = name
	sdb, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open %s store: %v", name, err)
	}
	t.Cleanup(func() { _ = sdb.Close() })
	return sdb.GetDriver()
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	// 2500 keys deliberately crosses the 1000-record batch boundary in restore.
	const n = 2500
	for _, name := range []string{"goleveldb", "badger"} {
		t.Run(name, func(t *testing.T) {
			src := openTestDriver(t, name)
			for i := 0; i < n; i++ {
				key := []byte(fmt.Sprintf("key-%05d", i))
				val := []byte(fmt.Sprintf("val-%05d", i))
				if err := src.Put(key, val); err != nil {
					t.Fatalf("put: %v", err)
				}
			}

			snap, err := src.NewSnapshot()
			if err != nil {
				t.Fatalf("new snapshot: %v", err)
			}
			var buf bytes.Buffer
			if err := writeSnapshot(snap, &buf); err != nil {
				t.Fatalf("write snapshot: %v", err)
			}
			snap.Close()

			dst := openTestDriver(t, name)
			if err := restoreSnapshot(dst, &buf); err != nil {
				t.Fatalf("restore: %v", err)
			}
			for i := 0; i < n; i++ {
				key := []byte(fmt.Sprintf("key-%05d", i))
				want := fmt.Sprintf("val-%05d", i)
				got, err := dst.Get(key)
				if err != nil {
					t.Fatalf("get %s: %v", key, err)
				}
				if string(got) != want {
					t.Fatalf("%s: key %s = %q, want %q", name, key, got, want)
				}
			}
		})
	}
}
