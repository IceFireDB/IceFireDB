package badger

import (
	"github.com/dgraph-io/badger/v3"
)

type Iterator struct {
	db      *badger.DB
	it      *badger.Iterator
}

func (it *Iterator) Key() []byte {
	return it.it.Item().KeyCopy(nil)
}

func (it *Iterator) Value() []byte {
	v, _ := it.it.Item().ValueCopy(nil)
	return v
}

func (it *Iterator) Close() error {
	if it.it != nil {
		it.it.Close()
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
	tnx := it.db.NewTransaction(false)
	defer tnx.Discard()

	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	opts.PrefetchValues = false
	revit := tnx.NewIterator(opts)
	
	if it.Valid() {
		key := it.Key()
		revit.Seek(key)
		revit.Next()
		if revit.Valid() {
			newKey := revit.Item().Key()
			it.it.Seek(newKey)
		}
	}
}

func (it *Iterator) First() {
	it.it.Rewind()
}

func (it *Iterator) Last() {
	tnx := it.db.NewTransaction(false)
	defer tnx.Discard()

	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	opts.PrefetchValues = false
	revit := tnx.NewIterator(opts)

	revit.Rewind()
	key := revit.Item().Key()

	it.it.Seek(key)
}

func (it *Iterator) Seek(key []byte) {
	it.it.Seek(key)
}
