package ipfs_synckv

import (
	"context"

	"github.com/IceFireDB/icefiredb-ipfs-log/stores/levelkv"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type Iterator struct {
	it     iterator.Iterator
	db     *DB // Reference to the main DB to access encryption key
	ipfsDB *levelkv.LevelKV
	ctx    context.Context
}

func (it *Iterator) Key() []byte {
	return it.it.Key()
}

func (it *Iterator) Value() []byte {
	key := it.it.Key()
	// Try to get the value from IPFS first for consistency
	value, err := it.ipfsDB.Get(key)
	if err == nil && value != nil {
		if it.db.encryptionKey != nil {
			return decrypt(value, it.db.encryptionKey)
		}
		return value
	}
	// Fall back to local value if IPFS fails or returns nil
	return it.it.Value()
}

func (it *Iterator) Close() error {
	if it.it != nil {
		it.it.Release()
		it.it = nil
	}
	return nil
}

func (it *Iterator) Valid() bool {
	return it.it.Valid()
}

func (it *Iterator) Next() {
	it.it.Next()
}

func (it *Iterator) Prev() {
	it.it.Prev()
}

func (it *Iterator) First() {
	it.it.First()
}

func (it *Iterator) Last() {
	it.it.Last()
}

func (it *Iterator) Seek(key []byte) {
	it.it.Seek(key)
}
