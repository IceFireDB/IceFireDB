package column

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteBatch(t *testing.T) {
	store := &Store{}
	db, err := store.Open("", nil)
	require.NoError(t, err)
	defer db.Close()

	t.Run("BasicOperations", func(t *testing.T) {
		batch := db.NewWriteBatch()
		defer batch.Close()

		// Test batch put
		batch.Put([]byte("batch1"), []byte("value1"))

		// Test batch delete 
		batch.Delete([]byte("batch1"))

		// Test commit
		err := batch.Commit()
		require.NoError(t, err)

		// Verify operations
		val, err := db.Get([]byte("batch1"))
		require.NoError(t, err)
		require.Nil(t, val)
	})

	t.Run("MultipleOperations", func(t *testing.T) {
		batch := db.NewWriteBatch()
		defer batch.Close()

		// Add multiple operations
		for i := 0; i < 10; i++ {
			key := []byte{byte(i)}
			value := []byte{byte(i + 10)}
			batch.Put(key, value)
		}

		err := batch.Commit()
		require.NoError(t, err)

		// Verify all operations were applied
		for i := 0; i < 10; i++ {
			key := []byte{byte(i)}
			expected := []byte{byte(i + 10)}
			val, err := db.Get(key)
			require.NoError(t, err)
			require.Equal(t, expected, val)
		}
	})
}
