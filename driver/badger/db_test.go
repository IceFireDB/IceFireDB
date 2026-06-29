package badger

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	idb, err := Store{}.Open(t.TempDir(), config.NewConfigDefault())
	require.NoError(t, err)
	db, ok := idb.(*DB)
	require.True(t, ok, "Open should return *DB")
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestBadgerDB_CRUD(t *testing.T) {
	db := openTestDB(t)

	key, val := []byte("k1"), []byte("v1")

	// Get of an absent key must return (nil, nil) — the contract the ledis
	// command layer relies on, not an error.
	got, err := db.Get([]byte("missing"))
	require.NoError(t, err)
	assert.Nil(t, got)

	require.NoError(t, db.Put(key, val))
	got, err = db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, val, got)

	require.NoError(t, db.Delete(key))
	got, err = db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, got)

	// SyncPut / SyncDelete behave like Put / Delete.
	require.NoError(t, db.SyncPut(key, val))
	got, err = db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, val, got)
	require.NoError(t, db.SyncDelete(key))
	got, err = db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestBadgerDB_WriteBatch(t *testing.T) {
	db := openTestDB(t)

	wb := db.NewWriteBatch()
	defer wb.Close()
	for i := 0; i < 50; i++ {
		wb.Put([]byte(fmt.Sprintf("b%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
	}
	require.NoError(t, wb.Commit())

	for i := 0; i < 50; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("b%02d", i)))
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("v%02d", i)), got)
	}

	// After Rollback the batch is reset and reusable; uncommitted puts vanish.
	wb.Put([]byte("rollme"), []byte("x"))
	require.NoError(t, wb.Rollback())
	require.NoError(t, wb.Commit())
	got, err := db.Get([]byte("rollme"))
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestBadgerDB_Iterator(t *testing.T) {
	db := openTestDB(t)

	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put([]byte(fmt.Sprintf("it%02d", i)), []byte(fmt.Sprintf("v%d", i))))
	}

	it := db.NewIterator()
	defer it.Close()

	var keys []string
	for it.First(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.Len(t, keys, 10)
	// Badger iterates in ascending key order.
	for i := 0; i < 10; i++ {
		assert.Equal(t, fmt.Sprintf("it%02d", i), keys[i])
	}

	// Seek positions at the first key >= target.
	it.Seek([]byte("it05"))
	require.True(t, it.Valid())
	assert.Equal(t, "it05", string(it.Key()))
	assert.Equal(t, "v5", string(it.Value()))
}

func TestBadgerDB_Snapshot(t *testing.T) {
	db := openTestDB(t)

	for i := 0; i < 5; i++ {
		require.NoError(t, db.Put([]byte(fmt.Sprintf("s%d", i)), []byte(fmt.Sprintf("v%d", i))))
	}

	snap, err := db.NewSnapshot()
	require.NoError(t, err)
	defer snap.Close()

	got, err := snap.Get([]byte("s2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got)

	sit := snap.NewIterator()
	defer sit.Close()
	count := 0
	for sit.First(); sit.Valid(); sit.Next() {
		count++
	}
	assert.Equal(t, 5, count)
}

func TestBadgerDB_GetStorageEngine(t *testing.T) {
	db := openTestDB(t)
	eng := db.GetStorageEngine()
	require.NotNil(t, eng)
	_, ok := eng.(*badger.DB)
	assert.True(t, ok, "GetStorageEngine should return *badger.DB, got %T", eng)
}
