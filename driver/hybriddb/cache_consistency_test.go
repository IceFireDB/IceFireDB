package hybriddb

import (
	"sync"
	"testing"
	"time"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
)

func newTestDB(t *testing.T) *DB {
	tempDir := t.TempDir()
	cfg := &config.Config{
		LevelDB: config.LevelDBConfig{
			CacheSize:       64 * 1024 * 1024,
			BlockSize:       4 * 1024,
			WriteBufferSize: 4 * 1024 * 1024,
			MaxOpenFiles:    1000,
			Compression:     true,
		},
	}

	db, err := Store{}.Open(tempDir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db.(*DB)
}

func getFromColdStorage(db *DB, key []byte) ([]byte, error) {
	v, err := db.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return v, err
}

func TestCacheConsistency_PutThenGet(t *testing.T) {
	db := newTestDB(t)

	key := []byte("consistency-key-1")
	value := []byte("consistency-value-1")

	err := db.Put(key, value)
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, cached)

	coldValue, err := getFromColdStorage(db, key)
	require.NoError(t, err)
	assert.Equal(t, value, coldValue)

	assert.Equal(t, value, cached, "Cache and cold storage should have same value")
}

func TestCacheConsistency_PutOverwrite(t *testing.T) {
	db := newTestDB(t)

	key := []byte("consistency-key-overwrite")
	value1 := []byte("value-1")
	value2 := []byte("value-2")

	err := db.Put(key, value1)
	require.NoError(t, err)

	err = db.Put(key, value2)
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value2, cached, "Should get latest value after overwrite")

	coldValue, err := getFromColdStorage(db, key)
	require.NoError(t, err)
	assert.Equal(t, value2, coldValue, "Cold storage should have latest value")

	assert.Equal(t, value2, cached, "Cache and cold storage should be consistent after overwrite")
}

func TestCacheConsistency_Delete(t *testing.T) {
	db := newTestDB(t)

	key := []byte("consistency-key-delete")
	value := []byte("value-to-delete")

	err := db.Put(key, value)
	require.NoError(t, err)

	_, _ = db.Get(key)

	err = db.Delete(key)
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, cached, "Cache should return nil after delete")

	coldValue, err := getFromColdStorage(db, key)
	require.NoError(t, err)
	assert.Nil(t, coldValue, "Cold storage should return nil after delete")

	assert.Equal(t, cached, coldValue, "Cache and cold storage should both return nil after delete")
}

func TestCacheConsistency_DeleteNonExistent(t *testing.T) {
	db := newTestDB(t)

	key := []byte("non-existent-key")

	err := db.Delete(key)
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, cached)

	coldValue, err := getFromColdStorage(db, key)
	require.NoError(t, err)
	assert.Nil(t, coldValue)
}

func TestCacheConsistency_SyncPut(t *testing.T) {
	db := newTestDB(t)

	key := []byte("sync-consistency-key")
	value := []byte("sync-consistency-value")

	err := db.SyncPut(key, value)
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, cached)

	coldValue, err := getFromColdStorage(db, key)
	require.NoError(t, err)
	assert.Equal(t, value, coldValue)
}

func TestCacheConsistency_SyncDelete(t *testing.T) {
	db := newTestDB(t)

	key := []byte("sync-delete-consistency-key")

	err := db.SyncPut(key, []byte("sync-delete-value"))
	require.NoError(t, err)

	_, _ = db.Get(key)

	err = db.SyncDelete(key)
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, cached)

	coldValue, err := getFromColdStorage(db, key)
	require.NoError(t, err)
	assert.Nil(t, coldValue)
}

func TestCacheConsistency_ListCacheInvalidation(t *testing.T) {
	db := newTestDB(t)

	key := []byte("list-key")
	value := []byte("list-value")
	listMetaKey := append(append([]byte{}, key...), LMetaType)

	err := db.Put(key, value)
	require.NoError(t, err)

	_, _ = db.Get(key)

	_, _ = db.Get(listMetaKey)

	err = db.Put(key, []byte("new-value"))
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("new-value"), cached)

	listMetaCached, _ := db.Get(listMetaKey)
	assert.Nil(t, listMetaCached, "List meta cache should be invalidated after Put")
}

func TestCacheConsistency_ListCacheInvalidationOnDelete(t *testing.T) {
	db := newTestDB(t)

	key := []byte("list-key-delete")
	value := []byte("list-value")
	listMetaKey := append(append([]byte{}, key...), LMetaType)

	err := db.Put(key, value)
	require.NoError(t, err)

	_, _ = db.Get(key)
	_, _ = db.Get(listMetaKey)

	err = db.Delete(key)
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, cached)

	listMetaCached, _ := db.Get(listMetaKey)
	assert.Nil(t, listMetaCached, "List meta cache should be invalidated after Delete")
}

func TestCacheConsistency_InvalidateListCache(t *testing.T) {
	db := newTestDB(t)

	key := []byte("test-list-key")
	value := []byte("test-value")

	err := db.Put(key, value)
	require.NoError(t, err)

	db.InvalidateListCache(key)

	_, _ = db.Get(key)

	listMetaKey := append(append([]byte{}, key...), LMetaType)
	_, _ = db.Get(listMetaKey)

	db.InvalidateListCache(key)

	listMetaCached, _ := db.Get(listMetaKey)
	assert.Nil(t, listMetaCached, "List meta cache should be invalidated")
}

func TestCacheConsistency_InvalidateNilKey(t *testing.T) {
	db := newTestDB(t)

	db.InvalidateListCache(nil)
	db.InvalidateListCache([]byte{})

	assert.NotPanics(t, func() {
		db.InvalidateListCache(nil)
		db.InvalidateListCache([]byte{})
	})
}

func TestCacheConsistency_MultipleOperations(t *testing.T) {
	db := newTestDB(t)

	keys := make([][]byte, 10)
	values := make([][]byte, 10)

	for i := 0; i < 10; i++ {
		keys[i] = []byte("multi-key-" + string(rune('a'+i)))
		values[i] = []byte("multi-value-" + string(rune('0'+i)))
	}

	for i := 0; i < 10; i++ {
		err := db.Put(keys[i], values[i])
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		cached, err := db.Get(keys[i])
		require.NoError(t, err)
		assert.Equal(t, values[i], cached)

		coldValue, err := getFromColdStorage(db, keys[i])
		require.NoError(t, err)
		assert.Equal(t, values[i], coldValue)
	}

	for i := 0; i < 5; i++ {
		newValue := []byte("updated-value-" + string(rune('0'+i)))
		err := db.Put(keys[i], newValue)
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		cached, err := db.Get(keys[i])
		require.NoError(t, err)
		assert.Equal(t, []byte("updated-value-"+string(rune('0'+i))), cached)
	}

	for i := 0; i < 5; i++ {
		err := db.Delete(keys[i])
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		cached, err := db.Get(keys[i])
		require.NoError(t, err)
		assert.Nil(t, cached)
	}
}

func TestCacheConsistency_ConcurrentPutAndGet(t *testing.T) {
	db := newTestDB(t)

	numGoroutines := 10
	opsPerGoroutine := 50
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := []byte("concurrent-key-" + string(rune('a'+goroutineID%26)) + "-" + string(rune('0'+j%10)))
				value := []byte("value-" + string(rune('A'+goroutineID%26)))

				err := db.Put(key, value)
				assert.NoError(t, err)

				cached, err := db.Get(key)
				assert.NoError(t, err)
				assert.NotNil(t, cached)

				coldValue, err := getFromColdStorage(db, key)
				assert.NoError(t, err)
				assert.Equal(t, coldValue, cached, "Cache and cold storage should be consistent")
			}
		}(i)
	}

	wg.Wait()
}

func TestCacheConsistency_ConcurrentPutAndDelete(t *testing.T) {
	db := newTestDB(t)

	key := []byte("concurrent-delete-key")
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			_ = db.Put(key, []byte("value"))
			_ = db.Delete(key)
		}(i)
	}

	wg.Wait()

	cached, err := db.Get(key)
	require.NoError(t, err)
	coldValue, _ := getFromColdStorage(db, key)
	assert.Equal(t, coldValue, cached, "Cache and cold storage should be consistent")
}

func TestCacheConsistency_ConcurrentOverwrite(t *testing.T) {
	db := newTestDB(t)

	key := []byte("concurrent-overwrite-key")
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				value := []byte("value-" + string(rune('A'+goroutineID)))
				err := db.Put(key, value)
				assert.NoError(t, err)

				cached, err := db.Get(key)
				assert.NoError(t, err)
				assert.NotNil(t, cached)
			}
		}(i)
	}

	wg.Wait()

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.NotNil(t, cached)

	coldValue, _ := getFromColdStorage(db, key)
	assert.Equal(t, coldValue, cached, "Cache and cold storage should be consistent")
}

func TestCacheConsistency_MixedOperations(t *testing.T) {
	db := newTestDB(t)

	numKeys := 10
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte("mixed-op-key-" + string(rune('a'+i)))
	}

	for _, key := range keys {
		value := []byte("value-" + string(key[len(key)-1]))
		err := db.Put(key, value)
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = db.Get(keys[idx])
			_ = db.Delete(keys[idx])
		}(i)
	}
	wg.Wait()

	for _, key := range keys {
		cached, _ := db.Get(key)
		coldValue, _ := getFromColdStorage(db, key)
		assert.Equal(t, coldValue, cached, "Cache and cold storage should be consistent")
	}
}

func TestCacheConsistency_PutDeletePut(t *testing.T) {
	db := newTestDB(t)

	key := []byte("put-delete-put-key")
	value1 := []byte("value-1")
	value2 := []byte("value-2")

	err := db.Put(key, value1)
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value1, cached)

	err = db.Delete(key)
	require.NoError(t, err)

	cached, err = db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, cached)

	err = db.Put(key, value2)
	require.NoError(t, err)

	cached, err = db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value2, cached)

	coldValue, err := getFromColdStorage(db, key)
	require.NoError(t, err)
	assert.Equal(t, value2, coldValue)
}

func TestCacheConsistency_CacheExpiry(t *testing.T) {
	db := newTestDB(t)
	db.config.CacheTTL = 100 * time.Millisecond

	key := []byte("expiry-key")
	value := []byte("expiry-value")

	err := db.Put(key, value)
	require.NoError(t, err)

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, cached)

	time.Sleep(150 * time.Millisecond)

	cached, err = db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, cached, "Should still get value from cold storage after cache expiry")

	stats := db.GetStats()
	assert.GreaterOrEqual(t, stats.CacheMisses, int64(1), "Should have cache miss after expiry")
}

func TestCacheConsistency_CloseAndReopen(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		LevelDB: config.LevelDBConfig{
			CacheSize:       64 * 1024 * 1024,
			BlockSize:       4 * 1024,
			WriteBufferSize: 4 * 1024 * 1024,
			MaxOpenFiles:    1000,
			Compression:     true,
		},
	}

	key := []byte("reopen-key")
	value := []byte("reopen-value")

	db1, err := Store{}.Open(tempDir, cfg)
	require.NoError(t, err)

	err = db1.Put(key, value)
	require.NoError(t, err)

	_, _ = db1.Get(key)

	err = db1.Close()
	require.NoError(t, err)

	db2, err := Store{}.Open(tempDir, cfg)
	require.NoError(t, err)
	defer db2.Close()

	cached, err := db2.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, cached)

	coldValue, err := getFromColdStorage(db2.(*DB), key)
	require.NoError(t, err)
	assert.Equal(t, value, coldValue)
}

func TestCacheConsistency_WriteBatch(t *testing.T) {
	db := newTestDB(t)

	wb := db.NewWriteBatch()
	defer wb.Close()

	numKeys := 20
	for i := 0; i < numKeys; i++ {
		key := []byte("batch-key-" + string(rune('a'+i)))
		value := []byte("batch-value-" + string(rune('0'+i%10)))
		wb.Put(key, value)
	}

	err := wb.Commit()
	require.NoError(t, err)

	for i := 0; i < numKeys; i++ {
		key := []byte("batch-key-" + string(rune('a'+i)))
		value := []byte("batch-value-" + string(rune('0'+i%10)))

		cached, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, value, cached)

		coldValue, err := getFromColdStorage(db, key)
		require.NoError(t, err)
		assert.Equal(t, value, coldValue)
	}
}

func TestCacheConsistency_SnapshotIsolation(t *testing.T) {
	db := newTestDB(t)

	key := []byte("snapshot-isolation-key")
	value1 := []byte("value-1")
	value2 := []byte("value-2")

	err := db.Put(key, value1)
	require.NoError(t, err)

	snapshot, err := db.NewSnapshot()
	require.NoError(t, err)
	defer snapshot.Close()

	_, _ = db.Get(key)

	err = db.Put(key, value2)
	require.NoError(t, err)

	snapshotValue, err := snapshot.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value1, snapshotValue, "Snapshot should see old value")

	cached, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value2, cached, "Current DB should see new value")

	coldValue, err := getFromColdStorage(db, key)
	require.NoError(t, err)
	assert.Equal(t, value2, coldValue, "Cold storage should have new value")
}

func TestCacheConsistency_StatsTracking(t *testing.T) {
	db := newTestDB(t)

	statsBefore := db.GetStats()

	keys := [][]byte{
		[]byte("stats-key-1"),
		[]byte("stats-key-2"),
		[]byte("stats-key-3"),
	}

	for i, key := range keys {
		err := db.Put(key, []byte("value-"+string(rune('1'+i))))
		require.NoError(t, err)
	}

	for _, key := range keys {
		_, _ = db.Get(key)
		_, _ = db.Get(key)
	}

	statsAfter := db.GetStats()

	assert.GreaterOrEqual(t, statsAfter.ReadOps-statsBefore.ReadOps, int64(6), "Should have at least 6 read operations")
	assert.GreaterOrEqual(t, statsAfter.WriteOps-statsBefore.WriteOps, int64(3), "Should have at least 3 write operations")
	assert.GreaterOrEqual(t, statsAfter.CacheMisses, int64(3), "Should have at least 3 cache misses")
}

func TestCacheConsistency_InvalidKeySize(t *testing.T) {
	db := newTestDB(t)

	largeKey := make([]byte, 1024*1024+1)
	for i := range largeKey {
		largeKey[i] = 'k'
	}

	err := db.Put(largeKey, []byte("value"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")

	largeValue := make([]byte, 64*1024*1024+1)
	for i := range largeValue {
		largeValue[i] = 'v'
	}

	err = db.Put([]byte("key"), largeValue)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

func TestCacheConsistency_EmptyKey(t *testing.T) {
	db := newTestDB(t)

	err := db.Put([]byte(""), []byte("value"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")
}

func TestCacheConsistency_NonExistentKey(t *testing.T) {
	db := newTestDB(t)

	cached, err := db.Get([]byte("totally-non-existent-key"))
	require.NoError(t, err)
	assert.Nil(t, cached)

	coldValue, err := getFromColdStorage(db, []byte("totally-non-existent-key"))
	require.NoError(t, err)
	assert.Nil(t, coldValue)
}

func TestCacheConsistency_LMetaTypeConstant(t *testing.T) {
	assert.Equal(t, 1, int(LMetaType), "LMetaType should be 1")
}
