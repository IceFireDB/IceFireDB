package duckdb

import (
	"os"
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/require"
	_ "github.com/marcboeker/go-duckdb"
)

func TestDBOpen(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)
}

func TestDBPutGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	err = db.Put(key, value)
	require.NoError(t, err)

	gotValue, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, gotValue)
}

func TestDBPutGetMultiple(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	// Put all data
	for k, v := range testData {
		err = db.Put([]byte(k), v)
		require.NoError(t, err)
	}

	// Get all data back
	for k, expected := range testData {
		gotValue, err := db.Get([]byte(k))
		require.NoError(t, err)
		require.Equal(t, expected, gotValue)
	}
}

func TestDBDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	err = db.Put(key, value)
	require.NoError(t, err)

	// Verify the value exists
	gotValue, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, gotValue)

	err = db.Delete(key)
	require.NoError(t, err)

	// Verify the value is deleted
	gotValue, err = db.Get(key)
	require.NoError(t, err)
	require.Nil(t, gotValue)
}

func TestDBSyncPutDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Test SyncPut
	err = db.SyncPut(key, value)
	require.NoError(t, err)

	gotValue, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, gotValue)

	// Test SyncDelete
	err = db.SyncDelete(key)
	require.NoError(t, err)

	gotValue, err = db.Get(key)
	require.NoError(t, err)
	require.Nil(t, gotValue)
}

func TestDBOverwrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	key := []byte("test-key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Put first value
	err = db.Put(key, value1)
	require.NoError(t, err)

	gotValue, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, value1, gotValue)

	// Overwrite with second value
	err = db.Put(key, value2)
	require.NoError(t, err)

	gotValue, err = db.Get(key)
	require.NoError(t, err)
	require.Equal(t, value2, gotValue)
}

func TestDBEmptyValue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	key := []byte("test-key")
	emptyValue := []byte("")

	err = db.Put(key, emptyValue)
	require.NoError(t, err)

	gotValue, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, emptyValue, gotValue)
}

func TestDBNonexistentKey(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	nonexistentKey := []byte("nonexistent-key")

	gotValue, err := db.Get(nonexistentKey)
	require.NoError(t, err)
	require.Nil(t, gotValue)
}

func TestDBCompact(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Compact should not return an error
	err = db.Compact()
	require.NoError(t, err)
}

func TestDBGetStorageEngine(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	engine := db.GetStorageEngine()
	require.NotNil(t, engine)
}