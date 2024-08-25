package iter

// Map invokes f on each element of iter.
func Map[T any, U any](iter Iter[T], f func(t T) U) *MapIter[T, U] {
	return &MapIter[T, U]{iter: iter, f: f}
}

type MapIter[T any, U any] struct {
	iter Iter[T]
	f    func(T) U

	done bool
	val  U
}

func (m *MapIter[T, U]) Next() bool {
	if m.done {
		return false
	}

	ok := m.iter.Next()
	m.done = !ok

	if m.done {
		return false
	}

	m.val = m.f(m.iter.Val())

	return true
}

func (m *MapIter[T, U]) Val() U {
	return m.val
}

func (m *MapIter[T, U]) Close() error {
	return m.iter.Close()
}
