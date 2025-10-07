package crdt

import (
	"fmt"
	"github.com/ipfs/go-datastore/query"
	"github.com/sirupsen/logrus"
)

type Iterator struct {
	db      *DB
	seekKey string
	results query.Results
	current query.Result
}

// Returns the current iterator key
func (it *Iterator) Key() []byte {
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
	return it.current.Error == nil && len(it.current.Key) > 0
}

func (it *Iterator) Next() {
	it.current = <-it.results.Next()
}

func (it *Iterator) Prev() {
	it.current = <-it.results.Next()
}

func (it *Iterator) First() {
	it.Seek([]byte(it.seekKey))
}

func (it *Iterator) Last() {
	q := query.Query{
		Orders: []query.Order{query.OrderByKeyDescending{}},
		Filters: []query.Filter{
			query.FilterKeyCompare{Key: it.seekKey, Op: query.LessThanOrEqual},
		},
	}
	if len(it.seekKey) > 5 {
		q.Filters = append(q.Filters, query.FilterKeyPrefix{Prefix: it.seekKey[:5]})
	}
	result, err := it.db.db.Query(it.db.ctx, q)
	if err != nil {
		logrus.Errorf("failed to query database: %v", err)
		return
	}
	it.results = result
	it.current = <-it.results.Next()
}

func (it *Iterator) Seek(key []byte) {
	it.seekKey = fmt.Sprintf("/%s", string(it.db.EncodeKey(key)))
	q := query.Query{
		Orders: []query.Order{query.OrderByKey{}},
		Filters: []query.Filter{
			query.FilterKeyCompare{Key: it.seekKey, Op: query.GreaterThanOrEqual},
		},
	}
	if len(it.seekKey) > 5 {
		q.Filters = append(q.Filters, query.FilterKeyPrefix{Prefix: it.seekKey[:5]})
	}

	result, err := it.db.db.Query(it.db.ctx, q)
	if err != nil {
		logrus.Errorf("failed to query database: %v", err)
		return
	}
	it.results = result
	it.current = <-it.results.Next()
}
