package duckdb

import (
	"os"
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/require"
	_ "github.com/marcboeker/go-duckdb"
)

func TestStoreString(t *testing.T) {
	store := Store{}
	require.Equal(t, "duckdb", store.String())
}

func TestStoreRepair(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}

	// Test repair on non-existent database
	err = store.Repair(tmpDir+"/new.db", cfg)
	require.NoError(t, err)

	// Verify we can open the repaired database
	db, err := store.Open(tmpDir+"/new.db", cfg)
	require.NoError(t, err)
	defer db.Close()

	// Test basic operations on repaired database
	err = db.Put([]byte("test-key"), []byte("test-value"))
	require.NoError(t, err)

	value, err := db.Get([]byte("test-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("test-value"), value)
}

func TestStoreRepairExisting(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}

	// Create a database first
	db, err := store.Open(tmpDir+"/existing.db", cfg)
	require.NoError(t, err)

	// Add some data
	err = db.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	// Repair the existing database
	err = store.Repair(tmpDir+"/existing.db", cfg)
	require.NoError(t, err)

	// Reopen and verify data still exists
	db2, err := store.Open(tmpDir+"/existing.db", cfg)
	require.NoError(t, err)
	defer db2.Close()

	value, err := db2.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), value)
}

func TestStoreMultipleDatabases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}

	// Create multiple databases
	db1, err := store.Open(tmpDir+"/db1.db", cfg)
	require.NoError(t, err)
	defer db1.Close()

	db2, err := store.Open(tmpDir+"/db2.db", cfg)
	require.NoError(t, err)
	defer db2.Close()

	// Add different data to each database
	err = db1.Put([]byte("key1"), []byte("db1-value"))
	require.NoError(t, err)

	err = db2.Put([]byte("key1"), []byte("db2-value"))
	require.NoError(t, err)

	// Verify data isolation
	value1, err := db1.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("db1-value"), value1)

	value2, err := db2.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("db2-value"), value2)
}