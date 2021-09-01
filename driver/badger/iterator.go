package badger

import (
	"github.com/dgraph-io/badger/v3"
)

type Iterator struct {
	db      *badger.DB
	it      *badger.Iterator
}

func (it *Iterator) Key() []byte {
	printf("it key")
	return it.it.Item().KeyCopy(nil)
}

func (it *Iterator) Value() []byte {
	printf("it value")
	v, _ := it.it.Item().ValueCopy(nil)
	return v
}

func (it *Iterator) Close() error {
	printf("it close")
	if it.it != nil {
		it.it.Close()
		it.it = nil
	}
	return nil
}

func (it *Iterator) Valid() bool {
	printf("it valid")
	return it.it.Valid()
}

func (it *Iterator) Next() {
	printf("it next")
	it.it.Next()
}

func (it *Iterator) Prev() {
	printf("it prev")
	tnx := it.db.NewTransaction(false)
	defer tnx.Discard()

	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	opts.PrefetchValues = false
	revit := tnx.NewIterator(opts)
	defer revit.Close()

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
	printf("it first")
	it.it.Rewind()
}

func (it *Iterator) Last() {
	printf("it last")
	tnx := it.db.NewTransaction(false)
	defer tnx.Discard()

	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	opts.PrefetchValues = false
	revit := tnx.NewIterator(opts)
	defer revit.Close()

	revit.Rewind()
	key := revit.Item().Key()

	it.it.Seek(key)
}

func (it *Iterator) Seek(key []byte) {
	printf("it seek")
	it.it.Seek(key)
}
