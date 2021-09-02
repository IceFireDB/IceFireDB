package badger

import (
	"github.com/dgraph-io/badger/v3"
)

type Iterator struct {
	db  *badger.DB
	txn *badger.Txn
	it  *badger.Iterator
}

func (it *Iterator) Key() []byte {
	key := it.it.Item().KeyCopy(nil)
	return key
}

func (it *Iterator) Value() []byte {
	v := []byte{}
	var err error
	v, err = it.it.Item().ValueCopy(v)
	if err != nil {
		return nil
	}
	return v
}

func (it *Iterator) Close() error {
	if it.it != nil {
		it.it.Close()
		it.it = nil
		it.txn.Discard()
	}
	return nil
}

func (it *Iterator) Valid() bool {
	ok := it.it.Valid()
	return ok
}

func (it *Iterator) Next() {
	it.it.Next()
}

func (it *Iterator) Prev() {
	tnx := it.db.NewTransactionAt(timeTs(), false)
	defer tnx.Discard()

	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	opts.PrefetchValues = false
	revit := tnx.NewIterator(opts)
	defer revit.Close()

	if it.it.Valid() {
		key := it.it.Item().KeyCopy(nil)
		revit.Seek(key)
		revit.Next()
		if revit.Valid() {
			newKey := revit.Item().KeyCopy(nil)
			it.it.Seek(newKey)
		}
	}
}

func (it *Iterator) First() {
	it.it.Rewind()
}

func (it *Iterator) Last() {
	tnx := it.db.NewTransactionAt(timeTs(), false)
	defer tnx.Discard()

	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	opts.PrefetchValues = false
	revit := tnx.NewIterator(opts)
	defer revit.Close()

	revit.Rewind()
	key := revit.Item().KeyCopy(nil)

	it.it.Seek(key)
}

func (it *Iterator) Seek(key []byte) {
	it.it.Seek(key)
}
