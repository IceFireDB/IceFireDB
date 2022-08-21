package crdt

import (
	"github.com/ipfs/go-datastore/query"
	"log"
)

type Iterator struct {
	db      *DB
	results query.Results
	current query.Result
}

// Returns the current iterator key
func (it *Iterator) Key() []byte {
	// it.current.Key[0] == /
	return []byte(it.current.Key[1:])
}

// Returns the current iterator, value
func (it *Iterator) Value() []byte {
	return it.current.Value
}

func (it *Iterator) Close() error {
	return it.results.Close()
}

func (it *Iterator) Valid() bool {
	return it.current.Error == nil && len(it.current.Key) > 0 && len(it.current.Value) > 0
}

func (it *Iterator) Next() {
	it.current = <-it.results.Next()
}

func (it *Iterator) Prev() {
}

func (it *Iterator) First() {
}

func (it *Iterator) Last() {
}

func (it *Iterator) Seek(key []byte) {
	// Due to the CRDT lookup mechanism,
	// the key starting with 0 4 0 4 bytes is judged
	// and the last three bytes are removed
	var k []byte
	if len(key) > 4 && key[0] == 0x0 && key[1] == 0x4 && key[2] == 0x0 && key[3] == 0x4 {
		k = it.db.BytesToHex(key[:len(key)-3])
	} else {
		k = it.db.EncodeKey(key)
	}

	result, err := it.db.db.Query(it.db.ctx, query.Query{
		Filters: []query.Filter{
			query.FilterKeyPrefix{Prefix: "/" + string(k)},
		},
	})
	if err != nil {
		log.Println(err)
		return
	}
	it.results = result
	it.current = <-it.results.Next()
}
