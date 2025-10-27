package hybriddb

import (
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
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

	// Insert initial data
	initialData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range initialData {
		err = db.Put([]byte(k), []byte(v))
		require.NoError(t, err)
	}

	// Create snapshot
	snapshot, err := db.NewSnapshot()
	require.NoError(t, err)
	defer snapshot.Close()

	// Modify data after snapshot
	modifiedData := map[string]string{
		"key1": "new-value1",
		"key4": "value4", // New key
	}

	for k, v := range modifiedData {
		err = db.Put([]byte(k), []byte(v))
		require.NoError(t, err)
	}

	// Delete a key
	err = db.Delete([]byte("key2"))
	require.NoError(t, err)

	// Snapshot should see original data
	for k, expectedValue := range initialData {
		value, err := snapshot.Get([]byte(k))
		require.NoError(t, err)
		assert.Equal(t, []byte(expectedValue), value, "Snapshot should see original value for key %s", k)
	}

	// Snapshot should not see new data
	value, err := snapshot.Get([]byte("key4"))
	require.NoError(t, err)
	assert.Nil(t, value, "Snapshot should not see new key")

	// Current DB should see modified data
	for k, expectedValue := range modifiedData {
		value, err := db.Get([]byte(k))
		require.NoError(t, err)
		assert.Equal(t, []byte(expectedValue), value, "Current DB should see modified value for key %s", k)
	}

	// Current DB should not see deleted key
	value, err = db.Get([]byte("key2"))
	require.NoError(t, err)
	assert.Nil(t, value, "Current DB should not see deleted key")
}

func TestSnapshot_Iterator(t *testing.T) {
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

	// Create snapshot
	snapshot, err := db.NewSnapshot()
	require.NoError(t, err)
	defer snapshot.Close()

	// Modify data after snapshot
	err = db.Put([]byte("d"), []byte("4"))
	require.NoError(t, err)
	err = db.Delete([]byte("b"))
	require.NoError(t, err)

	// Test snapshot iterator
	iter := snapshot.NewIterator()
	defer iter.Close()

	// Count items in snapshot
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		value := string(iter.Value())

		expectedValue, exists := testData[key]
		assert.True(t, exists, "Snapshot iterator should only see original keys")
		assert.Equal(t, expectedValue, value)
		count++
	}

	assert.Equal(t, len(testData), count, "Snapshot iterator should see all original keys")

	// Test current DB iterator
	currentIter := db.NewIterator()
	defer currentIter.Close()

	currentCount := 0
	for currentIter.First(); currentIter.Valid(); currentIter.Next() {
		currentCount++
	}

	// Should have original keys minus deleted plus new
	expectedCurrentCount := len(testData) - 1 + 1
	assert.Equal(t, expectedCurrentCount, currentCount, "Current DB iterator should see modified data")
}

func TestSnapshot_Close(t *testing.T) {
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

	// Create snapshot
	snapshot, err := db.NewSnapshot()
	require.NoError(t, err)

	// Should be able to use snapshot
	value, err := snapshot.Get([]byte("test"))
	require.NoError(t, err)
	assert.Nil(t, value)

	// Close snapshot
	snapshot.Close()

	// After closing, operations should still work (no panic)
	// but behavior is undefined, so we just ensure no panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Snapshot operations panicked after close: %v", r)
			}
		}()

		_, _ = snapshot.Get([]byte("test"))
		_ = snapshot.NewIterator()
	}()
}

func TestSnapshot_Concurrent(t *testing.T) {
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

	// Insert initial data
	err = db.Put([]byte("initial"), []byte("value"))
	require.NoError(t, err)

	// Create multiple snapshots
	snapshots := make([]driver.ISnapshot, 5)
	for i := 0; i < 5; i++ {
		snapshot, err := db.NewSnapshot()
		require.NoError(t, err)
		snapshots[i] = snapshot
	}

	// Modify data
	err = db.Put([]byte("initial"), []byte("modified"))
	require.NoError(t, err)
	err = db.Put([]byte("new"), []byte("value"))
	require.NoError(t, err)

	// All snapshots should see original data
	for i, snapshot := range snapshots {
		value, err := snapshot.Get([]byte("initial"))
		require.NoError(t, err)
		assert.Equal(t, []byte("value"), value, "Snapshot %d should see original value", i)

		value, err = snapshot.Get([]byte("new"))
		require.NoError(t, err)
		assert.Nil(t, value, "Snapshot %d should not see new key", i)

		snapshot.Close()
	}
}
