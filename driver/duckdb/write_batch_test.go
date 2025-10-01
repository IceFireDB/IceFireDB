package duckdb

import (
	"os"
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/require"
	_ "github.com/marcboeker/go-duckdb"
)

func TestWriteBatchBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Create write batch
	wb := db.NewWriteBatch()
	defer wb.Close()

	// Add operations
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))
	wb.Delete([]byte("key3")) // Delete non-existent key

	// Commit the batch
	err = wb.Commit()
	require.NoError(t, err)

	// Verify the operations
	value1, err := db.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), value1)

	value2, err := db.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), value2)
}

func TestWriteBatchRollback(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Add some initial data
	err = db.Put([]byte("existing"), []byte("value"))
	require.NoError(t, err)

	// Create write batch and add operations
	wb := db.NewWriteBatch()
	defer wb.Close()

	wb.Put([]byte("key1"), []byte("value1"))
	wb.Delete([]byte("existing"))

	// Rollback the batch
	err = wb.Rollback()
	require.NoError(t, err)

	// Verify that operations were not applied
	value1, err := db.Get([]byte("key1"))
	require.NoError(t, err)
	require.Nil(t, value1) // Should not exist

	existing, err := db.Get([]byte("existing"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), existing) // Should still exist
}

func TestWriteBatchSyncCommit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Create write batch
	wb := db.NewWriteBatch()
	defer wb.Close()

	// Add operations
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))

	// Sync commit the batch
	err = wb.SyncCommit()
	require.NoError(t, err)

	// Verify the operations
	value1, err := db.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), value1)

	value2, err := db.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), value2)
}

func TestWriteBatchMultipleOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Add some initial data
	err = db.Put([]byte("to_delete"), []byte("delete_me"))
	require.NoError(t, err)

	// Create write batch with mixed operations
	wb := db.NewWriteBatch()
	defer wb.Close()

	// Multiple puts
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))
	wb.Put([]byte("key3"), []byte("value3"))

	// Delete existing key
	wb.Delete([]byte("to_delete"))

	// Overwrite key
	wb.Put([]byte("key1"), []byte("new_value1"))

	// Commit
	err = wb.Commit()
	require.NoError(t, err)

	// Verify all operations
	value1, err := db.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("new_value1"), value1)

	value2, err := db.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), value2)

	value3, err := db.Get([]byte("key3"))
	require.NoError(t, err)
	require.Equal(t, []byte("value3"), value3)

	deleted, err := db.Get([]byte("to_delete"))
	require.NoError(t, err)
	require.Nil(t, deleted)
}

func TestWriteBatchEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Create empty write batch
	wb := db.NewWriteBatch()
	defer wb.Close()

	// Commit empty batch should succeed
	err = wb.Commit()
	require.NoError(t, err)

	// Rollback empty batch should succeed
	wb2 := db.NewWriteBatch()
	defer wb2.Close()

	err = wb2.Rollback()
	require.NoError(t, err)
}

func TestWriteBatchData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir+"/test.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Create write batch
	wb := db.NewWriteBatch()
	defer wb.Close()

	// Data method should return nil for DuckDB implementation
	data := wb.Data()
	require.Nil(t, data)

	// Should still work after operations
	wb.Put([]byte("key1"), []byte("value1"))
	data = wb.Data()
	require.Nil(t, data)
}