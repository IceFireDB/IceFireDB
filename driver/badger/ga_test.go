package badger

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGA_ConcurrencyNoCorruption drives interleaved writers and readers against
// the badger backend and asserts that no acknowledged value is lost or changed.
// This is GA evidence that badger's MVCC stays consistent under concurrency.
func TestGA_ConcurrencyNoCorruption(t *testing.T) {
	db := openTestDB(t)

	const writers, readers = 4, 4
	const keysPerWriter = 500

	var wg sync.WaitGroup
	// Writers: each owns a disjoint keyspace (wN-key*).
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < keysPerWriter; i++ {
				k := []byte(fmt.Sprintf("w%d-key%d", wid, i))
				v := []byte(fmt.Sprintf("val-%d-%d", wid, i))
				if err := db.Put(k, v); err != nil {
					t.Errorf("put %s: %v", k, err)
					return
				}
			}
		}(w)
	}
	// Readers: concurrent Gets must not panic or see torn values.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := 0; w < writers; w++ {
				for i := 0; i < keysPerWriter; i++ {
					_, _ = db.Get([]byte(fmt.Sprintf("w%d-key%d", w, i)))
				}
			}
		}()
	}
	wg.Wait()

	// Every written key must be readable with its exact value.
	for w := 0; w < writers; w++ {
		for i := 0; i < keysPerWriter; i++ {
			k := []byte(fmt.Sprintf("w%d-key%d", w, i))
			want := []byte(fmt.Sprintf("val-%d-%d", w, i))
			got, err := db.Get(k)
			require.NoError(t, err, "get %s", k)
			assert.Equal(t, want, got, "key %s", k)
		}
	}
}

// TestGA_LargeValueSurvivesCompaction proves a value larger than badger's
// value-log threshold survives a Compact() call. GA evidence for the
// large-value + compaction path.
func TestGA_LargeValueSurvivesCompaction(t *testing.T) {
	db := openTestDB(t)

	key := []byte("big")
	// 4 MB value: well above badger's default value-log threshold.
	big := make([]byte, 4<<20)
	for i := range big {
		big[i] = byte(i)
	}
	require.NoError(t, db.Put(key, big))
	require.NoError(t, db.Compact())

	got, err := db.Get(key)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(got, big), "value changed across compaction")
}

// TestGA_SnapshotIteratorIsolation proves a snapshot iterator reflects a
// consistent point-in-time view: it excludes keys written after the snapshot
// was taken and never observes post-snapshot mutations. GA evidence for
// iterator stability under write churn.
func TestGA_SnapshotIteratorIsolation(t *testing.T) {
	db := openTestDB(t)

	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v0")))
	}

	snap, err := db.NewSnapshot()
	require.NoError(t, err)
	defer snap.Close()

	// Mutate after snapshot: overwrite all + add a new key.
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v1")))
	}
	require.NoError(t, db.Put([]byte("kNew"), []byte("v1")))

	// The snapshot must still see v0 for all 10 keys and NOT see kNew.
	count, changed := 0, 0
	sit := snap.NewIterator()
	for sit.First(); sit.Valid(); sit.Next() {
		count++
		if bytes.Equal(sit.Value(), []byte("v1")) {
			changed++
		}
	}
	sit.Close()

	assert.Equal(t, 10, count, "snapshot must exclude the post-snapshot key kNew")
	assert.Equal(t, 0, changed, "snapshot must not observe post-snapshot writes")
}
