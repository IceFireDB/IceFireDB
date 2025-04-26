package column

type Iterator struct {
	keys []string
	pos  int
}

func (it *Iterator) Close() error {
	return nil
}

func (it *Iterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	return []byte(it.keys[it.pos])
}

func (it *Iterator) Value() []byte {
	return nil
}

func (it *Iterator) Valid() bool {
	return it.pos >= 0 && it.pos < len(it.keys)
}

func (it *Iterator) Next() {
	it.pos++
}

func (it *Iterator) Prev() {
	it.pos--
}

func (it *Iterator) Seek(key []byte) {
	for i, k := range it.keys {
		if k == string(key) {
			it.pos = i
			return
		}
	}
	it.pos = len(it.keys)
}

func (it *Iterator) First() {
	if len(it.keys) > 0 {
		it.pos = 0
	} else {
		it.pos = -1
	}
}

func (it *Iterator) Last() {
	if len(it.keys) > 0 {
		it.pos = len(it.keys) - 1
	} else {
		it.pos = -1
	}
}
