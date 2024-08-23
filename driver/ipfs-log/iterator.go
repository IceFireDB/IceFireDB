package ipfs_log

import (
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// Iterator wraps the leveldb iterator to provide a higher-level interface.
type Iterator struct {
	it iterator.Iterator // The underlying leveldb iterator.
}

// Key returns the key of the current iterator position.
func (it *Iterator) Key() []byte {
	return it.it.Key()
}

// Value returns the value of the current iterator position.
func (it *Iterator) Value() []byte {
	return it.it.Value()
}

// Close releases the iterator, making it unusable.
func (it *Iterator) Close() error {
	if it.it != nil {
		it.it.Release() // Release the underlying iterator.
		it.it = nil     // Set the iterator to nil to prevent further use.
	}
	return nil
}

// Valid returns whether the iterator is positioned at a valid item.
func (it *Iterator) Valid() bool {
	return it.it.Valid()
}

// Next advances the iterator to the next item.
func (it *Iterator) Next() {
	it.it.Next()
}

// Prev moves the iterator to the previous item.
func (it *Iterator) Prev() {
	it.it.Prev()
}

// First moves the iterator to the first item.
func (it *Iterator) First() {
	it.it.First()
}

// Last moves the iterator to the last item.
func (it *Iterator) Last() {
	it.it.Last()
}

// Seek moves the iterator to the first key that is greater than or equal to the given key.
func (it *Iterator) Seek(key []byte) {
	it.it.Seek(key)
}
