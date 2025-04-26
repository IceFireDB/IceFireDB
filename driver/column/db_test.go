package column

import (
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/stretchr/testify/require"
)

func TestDBOperations(t *testing.T) {
	store := &Store{}
	db, err := store.Open("", &config.Config{})
	require.NoError(t, err)
	defer db.Close()

	// Test basic put/get
	t.Run("PutGet", func(t *testing.T) {
		err := db.Put([]byte("key1"), []byte("value1"))
		require.NoError(t, err)

		val, err := db.Get([]byte("key1"))
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), val)
	})

	// Test delete
	t.Run("Delete", func(t *testing.T) {
		err := db.Delete([]byte("key1"))
		require.NoError(t, err)

		val, err := db.Get([]byte("key1"))
		require.NoError(t, err)
		require.Nil(t, val)
	})

	// Test iteration
	t.Run("Iteration", func(t *testing.T) {
		db.Put([]byte("key1"), []byte("value1"))
		db.Put([]byte("key2"), []byte("value2"))

		iter := db.NewIterator()
		defer iter.Close()

		keys := make(map[string]bool)
		count := 0
		iter.First()
		for iter.Valid() {
			key := iter.Key()
			if key != nil {
				keys[string(key)] = true
				count++
			}
			iter.Next()
		}
		require.Equal(t, 2, count)
		require.Equal(t, 2, len(keys))
		require.True(t, keys["key1"])
		require.True(t, keys["key2"])
	})
}
