package orbitdb

import (
	"strings"
)



type Iterator struct {
	allkey  []string
	cap int
	index  int

	allkv  map[string][]byte
}

func (it *Iterator) Key() []byte {
	s := it.allkey[it.index]
	return []byte(s)
}

func (it *Iterator) Value() []byte {
	s := it.allkey[it.index]
	return []byte(it.allkv[s])
}

func (it *Iterator) Close() error {
	//if it.it != nil {
	//	it.it.Release()
	//	it.it = nil
	//}
	return nil
}

func (it *Iterator) Valid() bool {
	if it.index >= it.cap  ||  it.index < 0{
		return false
	}

	return true

	//return it.it.Valid()
}

func (it *Iterator) Next() {
	it.index++
}

func (it *Iterator) Prev() {
	it.index--
}

func (it *Iterator) First() {
	it.index  = 0
}

func (it *Iterator) Last() {
	it.index = it.cap
}

func (it *Iterator) Seek(key []byte) {
	var ret int
	s := string(key)

	for i, key2 := range it.allkey {
		ret = strings.Compare(s, key2)
		if ret == 0{
			it.index = i
		}

	}


	//it.it.Seek(key)
}

