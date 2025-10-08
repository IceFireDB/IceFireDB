package hybriddb

import (
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteBatch_BasicOperations(t *testing.T) {
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

	// Test Put
	key1 := []byte("batch-key1")
	value1 := []byte("batch-value1")
	key2 := []byte("batch-key2")
	value2 := []byte("batch-value2")

	wb.Put(key1, value1)
	wb.Put(key2, value2)

	// Values should not be visible before commit
	val, err := db.Get(key1)
	require.NoError(t, err)
	assert.Nil(t, val)

	val, err = db.Get(key2)
	require.NoError(t, err)
	assert.Nil(t, val)

	// Commit batch
	err = wb.Commit()
	require.NoError(t, err)

	// Values should be visible after commit
	val, err = db.Get(key1)
	require.NoError(t, err)
	assert.Equal(t, value1, val)

	val, err = db.Get(key2)
	require.NoError(t, err)
	assert.Equal(t, value2, val)

	// Cache should be invalidated
	val, err = db.Get(key1)
	require.NoError(t, err)
	assert.Equal(t, value1, val)
}

func TestWriteBatch_MixedOperations(t *testing.T) {
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

	// Setup initial data
	err = db.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)
	err = db.Put([]byte("key2"), []byte("value2"))
	require.NoError(t, err)

	// Create batch with mixed operations
	wb := db.NewWriteBatch()
	defer wb.Close()

	// Update existing key
	wb.Put([]byte("key1"), []byte("updated-value1"))
	// Delete existing key
	wb.Delete([]byte("key2"))
	// Add new key
	wb.Put([]byte("key3"), []byte("value3"))

	// Commit batch
	err = wb.Commit()
	require.NoError(t, err)

	// Verify results
	val, err := db.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("updated-value1"), val)

	val, err = db.Get([]byte("key2"))
	require.NoError(t, err)
	assert.Nil(t, val)

	val, err = db.Get([]byte("key3"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value3"), val)
}

func TestWriteBatch_SyncCommit(t *testing.T) {
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

	key := []byte("sync-key")
	value := []byte("sync-value")

	wb.Put(key, value)

	// Test SyncCommit
	err = wb.SyncCommit()
	require.NoError(t, err)

	// Verify value
	val, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, val)
}

func TestWriteBatch_Rollback(t *testing.T) {
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

	key := []byte("rollback-key")
	value := []byte("rollback-value")

	wb.Put(key, value)

	// Rollback should clear the batch
	err = wb.Rollback()
	require.NoError(t, err)

	// Commit should do nothing after rollback
	err = wb.Commit()
	require.NoError(t, err)

	// Value should not be written
	val, err := db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestWriteBatch_Close(t *testing.T) {
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

	// Add some operations
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))

	// Close the batch
	wb.Close()

	// After closing, operations should not panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("WriteBatch operations panicked after close: %v", r)
			}
		}()
		
		wb.Put([]byte("key3"), []byte("value3"))
		wb.Delete([]byte("key1"))
		_ = wb.Commit()
		_ = wb.SyncCommit()
		_ = wb.Rollback()
		_ = wb.Data()
	}()

	// Values should not be written since batch was closed before commit
	val, err := db.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestWriteBatch_Data(t *testing.T) {
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

	// Add operations
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Delete([]byte("key2"))
	wb.Put([]byte("key3"), []byte("value3"))

	// Get batch data
	data := wb.Data()
	assert.NotNil(t, data)
	assert.Greater(t, len(data), 0)

	// Commit should still work
	err = wb.Commit()
	require.NoError(t, err)
}

func TestWriteBatch_Concurrent(t *testing.T) {
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

	numBatches := 10
	opsPerBatch := 100
	done := make(chan bool, numBatches)

	for i := 0; i < numBatches; i++ {
		go func(batchID int) {
			wb := db.NewWriteBatch()
			defer wb.Close()

			for j := 0; j < opsPerBatch; j++ {
				key := []byte(string(rune(batchID)) + "-" + string(rune(j)))
				value := []byte(string(rune(batchID)) + "-value-" + string(rune(j)))

				wb.Put(key, value)
			}

			err := wb.Commit()
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all batches to complete
	for i := 0; i < numBatches; i++ {
		<-done
	}

	// Verify all data was written
	for i := 0; i < numBatches; i++ {
		for j := 0; j < opsPerBatch; j++ {
			key := []byte(string(rune(i)) + "-" + string(rune(j)))
			expectedValue := []byte(string(rune(i)) + "-value-" + string(rune(j)))

			value, err := db.Get(key)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)
		}
	}
}