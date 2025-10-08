package hybriddb

import (
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_Open(t *testing.T) {
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
	defer db.Close()

	assert.NotNil(t, db)
	assert.IsType(t, &DB{}, db)
}

func TestDB_PutGet(t *testing.T) {
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
	defer db.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Test Put
	err = db.Put(key, value)
	require.NoError(t, err)

	// Test Get
	retrieved, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved)

	// Test cache hit
	retrieved, err = db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved)
}

func TestDB_Delete(t *testing.T) {
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
	defer db.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Put a value
	err = db.Put(key, value)
	require.NoError(t, err)

	// Verify it exists
	retrieved, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved)

	// Delete the value
	err = db.Delete(key)
	require.NoError(t, err)

	// Verify it's gone
	retrieved, err = db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, retrieved)

	// Verify cache is also cleared
	retrieved, err = db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, retrieved)
}

func TestDB_SyncOperations(t *testing.T) {
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
	defer db.Close()

	key := []byte("sync-key")
	value := []byte("sync-value")

	// Test SyncPut
	err = db.SyncPut(key, value)
	require.NoError(t, err)

	// Verify value
	retrieved, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved)

	// Test SyncDelete
	err = db.SyncDelete(key)
	require.NoError(t, err)

	// Verify deletion
	retrieved, err = db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, retrieved)
}

func TestDB_WriteBatch(t *testing.T) {
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
	defer db.Close()

	wb := db.NewWriteBatch()
	defer wb.Close()

	key1 := []byte("batch-key1")
	value1 := []byte("batch-value1")
	key2 := []byte("batch-key2")
	value2 := []byte("batch-value2")

	// Add operations to batch
	wb.Put(key1, value1)
	wb.Put(key2, value2)

	// Commit batch
	err = wb.Commit()
	require.NoError(t, err)

	// Verify values
	retrieved1, err := db.Get(key1)
	require.NoError(t, err)
	assert.Equal(t, value1, retrieved1)

	retrieved2, err := db.Get(key2)
	require.NoError(t, err)
	assert.Equal(t, value2, retrieved2)
}

func TestDB_Iterator(t *testing.T) {
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
	defer db.Close()

	// Insert test data
	testData := map[string]string{
		"a": "1",
		"b": "2", 
		"c": "3",
	}

	for k, v := range testData {
		err = db.Put([]byte(k), []byte(v))
		require.NoError(t, err)
	}

	// Test iterator
	iter := db.NewIterator()
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		value := string(iter.Value())
		
		expectedValue, exists := testData[key]
		assert.True(t, exists)
		assert.Equal(t, expectedValue, value)
		count++
	}

	assert.Equal(t, len(testData), count)
}

func TestDB_Snapshot(t *testing.T) {
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
	defer db.Close()

	key := []byte("snapshot-key")
	value := []byte("snapshot-value")

	// Put initial value
	err = db.Put(key, value)
	require.NoError(t, err)

	// Create snapshot
	snapshot, err := db.NewSnapshot()
	require.NoError(t, err)
	defer snapshot.Close()

	// Modify data after snapshot
	newValue := []byte("new-value")
	err = db.Put(key, newValue)
	require.NoError(t, err)

	// Snapshot should still see old value
	snapshotValue, err := snapshot.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, snapshotValue)

	// Current DB should see new value
	// We need to ensure cache is invalidated, so we'll get from cold storage
	currentValue, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, newValue, currentValue, "Current DB should see modified value")
}

func TestDB_Compact(t *testing.T) {
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
	defer db.Close()

	// Compact should not error
	err = db.Compact()
	require.NoError(t, err)
}

func TestDB_Metrics(t *testing.T) {
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

	dbInterface, err := Store{}.Open(tempDir, cfg)
	require.NoError(t, err)
	defer dbInterface.Close()

	// Type assert to access Metrics method
	db, ok := dbInterface.(*DB)
	require.True(t, ok, "Expected *DB type")

	title, metrics := db.Metrics()
	
	assert.Equal(t, "hybriddb cache", title)
	assert.NotNil(t, metrics)
	// Metrics might be empty if cache is disabled, so we don't check length
}

func TestDB_ConcurrentAccess(t *testing.T) {
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
	defer db.Close()

	done := make(chan bool)
	numGoroutines := 10
	opsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < opsPerGoroutine; j++ {
				key := []byte(string(rune(id)) + "-" + string(rune(j)))
				value := []byte(string(rune(id)) + "-value-" + string(rune(j)))

				err := db.Put(key, value)
				assert.NoError(t, err)

				retrieved, err := db.Get(key)
				assert.NoError(t, err)
				assert.Equal(t, value, retrieved)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

func TestDB_Repair(t *testing.T) {
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

	// Create and close a database first
	db, err := Store{}.Open(tempDir, cfg)
	require.NoError(t, err)
	
	key := []byte("repair-key")
	value := []byte("repair-value")
	err = db.Put(key, value)
	require.NoError(t, err)
	db.Close()

	// Test repair
	err = Store{}.Repair(tempDir, cfg)
	require.NoError(t, err)
}

func TestDB_EdgeCases(t *testing.T) {
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
	defer db.Close()

	// Test empty key - should fail validation
	err = db.Put([]byte(""), []byte("empty-key"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")
	
	value, err := db.Get([]byte(""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")

	// Test nil value
	err = db.Put([]byte("nil-value"), nil)
	require.NoError(t, err)
	
	value, err = db.Get([]byte("nil-value"))
	require.NoError(t, err)
	// LevelDB stores empty values as empty byte slices, not nil
	assert.Equal(t, []byte{}, value)

	// Test non-existent key
	value, err = db.Get([]byte("non-existent"))
	require.NoError(t, err)
	assert.Nil(t, value)
}