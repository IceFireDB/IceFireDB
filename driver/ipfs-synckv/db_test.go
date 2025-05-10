package ipfs_synckv

import (
	"os"
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/require"
)

func TestDBOpen(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ipfs-synckv-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir, cfg)
	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)
}

func TestDBPutGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ipfs-synckv-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir, cfg)
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

func TestDBDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ipfs-synckv-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir, cfg)
	require.NoError(t, err)
	defer db.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	err = db.Put(key, value)
	require.NoError(t, err)

	err = db.Delete(key)
	require.NoError(t, err)

	gotValue, err := db.Get(key)
	require.NoError(t, err)
	require.Nil(t, gotValue)
}

func TestDBMetrics(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ipfs-synckv-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := config.NewConfigDefault()
	store := Store{}
	db, err := store.Open(tmpDir, cfg)
	require.NoError(t, err)
	defer db.Close()

	title, metrics := db.(*DB).Metrics()
	require.Equal(t, "hybriddb cache", title)
	require.NotEmpty(t, metrics)
}
