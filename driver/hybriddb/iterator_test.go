package hybriddb

import (
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
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
	testData := []struct {
		key   string
		value string
	}{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
		{"d", "4"},
	}

	for _, item := range testData {
		err = db.Put([]byte(item.key), []byte(item.value))
		require.NoError(t, err)
	}

	// Test forward iteration
	iter := db.NewIterator()
	defer iter.Close()

	// Test First
	iter.First()
	assert.True(t, iter.Valid())
	assert.Equal(t, "a", string(iter.Key()))
	assert.Equal(t, "1", string(iter.Value()))

	// Test Next
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, "b", string(iter.Key()))
	assert.Equal(t, "2", string(iter.Value()))

	// Test Last
	iter.Last()
	assert.True(t, iter.Valid())
	assert.Equal(t, "d", string(iter.Key()))
	assert.Equal(t, "4", string(iter.Value()))

	// Test Prev
	iter.Prev()
	assert.True(t, iter.Valid())
	assert.Equal(t, "c", string(iter.Key()))
	assert.Equal(t, "3", string(iter.Value()))

	// Test Seek
	iter.Seek([]byte("b"))
	assert.True(t, iter.Valid())
	assert.Equal(t, "b", string(iter.Key()))
	assert.Equal(t, "2", string(iter.Value()))

	// Test Seek to non-existent key
	iter.Seek([]byte("e"))
	assert.False(t, iter.Valid())

	// Test full iteration
	iter.First()
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	assert.Equal(t, len(testData), count)
}

func TestIterator_EmptyDB(t *testing.T) {
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

	iter := db.NewIterator()
	defer iter.Close()

	// Test with empty database
	iter.First()
	assert.False(t, iter.Valid())

	iter.Last()
	assert.False(t, iter.Valid())

	iter.Seek([]byte("any"))
	assert.False(t, iter.Valid())
}

func TestIterator_Close(t *testing.T) {
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

	iter := db.NewIterator()
	
	// Should be able to use iterator before closing
	iter.First()
	assert.False(t, iter.Valid()) // Empty DB
	
	// Close should work without errors
	err = iter.Close()
	assert.NoError(t, err)
	
	// Double close should not panic
	err = iter.Close()
	assert.NoError(t, err)
}