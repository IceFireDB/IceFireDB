package buntdb_memory

import "github.com/tidwall/buntdb"

type iterator struct {
	db       *buntdb.DB
	tx       *buntdb.Tx
	keys     []string
	values   []string
	pos      int
	seekKey  string
	released bool
	err      error
}

func (it *iterator) First() {
	it.pos = -1
	it.seekKey = ""
	it.Next()
}

func (it *iterator) Last() {
	if it.released || it.err != nil {
		return
	}

	if it.tx == nil {
		it.tx, it.err = it.db.Begin(false)
		if it.err != nil {
			return
		}

		// Initialize iteration in reverse
		it.err = it.tx.DescendKeys("", func(key, value string) bool {
			it.keys = append(it.keys, key)
			it.values = append(it.values, value)
			return true
		})
		if it.err != nil {
			return
		}
	}

	it.pos = len(it.keys) - 1
	if it.pos >= 0 {
		it.Next()
	}
}

func (it *iterator) Prev() {
	if it.released || it.err != nil {
		return
	}
	it.pos--
}

func (it *iterator) Next() {
	if it.released || it.err != nil {
		return
	}

	if it.tx == nil {
		it.tx, it.err = it.db.Begin(false)
		if it.err != nil {
			return
		}

		// Initialize iteration
		it.err = it.tx.AscendKeys("", func(key, value string) bool {
			if it.seekKey != "" && key < it.seekKey {
				return true // skip until we reach seek position
			}
			it.keys = append(it.keys, key)
			it.values = append(it.values, value)
			return true
		})
		if it.err != nil {
			return
		}
	}

	it.pos++
}

func (it *iterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	return []byte(it.keys[it.pos])
}

func (it *iterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.values) {
		return nil
	}
	return []byte(it.values[it.pos])
}

func (it *iterator) Seek(key []byte) {
	it.seekKey = string(key)
	it.pos = -1 // Reset position
	it.Next()
}

func (it *iterator) Release() {
	if it.tx != nil && !it.released {
		_ = it.tx.Rollback()
		it.released = true
	}
}

func (it *iterator) Error() error {
	return it.err
}

func (it *iterator) Valid() bool {
	return it.pos >= 0 && it.pos < len(it.keys) && it.err == nil
}

func (it *iterator) Close() error {
	it.Release()
	return it.err
}
