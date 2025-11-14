package ipfs_synckv

import (
	"os"
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P test in short mode")
	}
	tmpDir, err := os.MkdirTemp("", "ipfs-synckv-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir, cfg)
	require.NoError(t, err)
	defer db.Close()

	// Insert test data
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for k, v := range testData {
		require.NoError(t, db.Put([]byte(k), v))
	}

	// Test iterator
	iter := db.NewIterator()
	defer iter.Close()

	count := 0
	iter.First()
	for iter.Valid() {
		count++
		key := iter.Key()
		value := iter.Value()
		expected, exists := testData[string(key)]
		require.True(t, exists)
		require.Equal(t, expected, value)
		iter.Next()
	}
	require.Equal(t, len(testData), count)
}

func TestIteratorPrefix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P test in short mode")
	}
	tmpDir, err := os.MkdirTemp("", "ipfs-synckv-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir, cfg)
	require.NoError(t, err)
	defer db.Close()

	// Insert test data
	testData := map[string][]byte{
		"prefix1_key1": []byte("value1"),
		"prefix1_key2": []byte("value2"),
		"prefix2_key1": []byte("value3"),
	}

	for k, v := range testData {
		require.NoError(t, db.Put([]byte(k), v))
	}

	// Test prefix iterator
	iter := db.NewIterator()
	iter.Seek([]byte("prefix1"))
	defer iter.Close()

	count := 0
	for iter.Valid() {
		key := iter.Key()
		if string(key) >= "prefix2" {
			break
		}
		count++
		value := iter.Value()
		expected, exists := testData[string(key)]
		require.True(t, exists)
		require.Equal(t, expected, value)
		require.Contains(t, string(key), "prefix1")
		iter.Next()
	}
	require.Equal(t, 2, count)
}
