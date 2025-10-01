package duckdb

import (
	"os"
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/require"
	_ "github.com/marcboeker/go-duckdb"
)

func TestIteratorBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
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

func TestIteratorSeek(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Insert test data with different prefixes
	testData := map[string][]byte{
		"prefix1_key1": []byte("value1"),
		"prefix1_key2": []byte("value2"),
		"prefix2_key1": []byte("value3"),
		"prefix3_key1": []byte("value4"),
	}

	for k, v := range testData {
		require.NoError(t, db.Put([]byte(k), v))
	}

	// Test seeking to prefix1
	iter := db.NewIterator()
	defer iter.Close()

	iter.Seek([]byte("prefix1"))

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

func TestIteratorSeekExact(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
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

	// Seek to exact key
	iter := db.NewIterator()
	defer iter.Close()

	iter.Seek([]byte("key2"))
	require.True(t, iter.Valid())
	require.Equal(t, []byte("key2"), iter.Key())
	require.Equal(t, []byte("value2"), iter.Value())
}

func TestIteratorFirstLast(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Insert test data in specific order
	testData := []struct {
		key   string
		value []byte
	}{
		{"a_key", []byte("value_a")},
		{"b_key", []byte("value_b")},
		{"c_key", []byte("value_c")},
	}

	for _, item := range testData {
		require.NoError(t, db.Put([]byte(item.key), item.value))
	}

	// Test First
	iter := db.NewIterator()
	defer iter.Close()

	iter.First()
	require.True(t, iter.Valid())
	require.Equal(t, []byte("a_key"), iter.Key())
	require.Equal(t, []byte("value_a"), iter.Value())

	// Test Last
	iter2 := db.NewIterator()
	defer iter2.Close()

	iter2.Last()
	require.True(t, iter2.Valid())
	require.Equal(t, []byte("c_key"), iter2.Key())
	require.Equal(t, []byte("value_c"), iter2.Value())
}

func TestIteratorEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Test iterator on empty database
	iter := db.NewIterator()
	defer iter.Close()

	iter.First()
	require.False(t, iter.Valid())

	iter.Seek([]byte("anykey"))
	require.False(t, iter.Valid())
}

func TestIteratorClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Insert some data
	require.NoError(t, db.Put([]byte("key1"), []byte("value1")))

	// Create and close iterator
	iter := db.NewIterator()
	require.NoError(t, iter.Close())

	// Should be able to close multiple times
	require.NoError(t, iter.Close())
}