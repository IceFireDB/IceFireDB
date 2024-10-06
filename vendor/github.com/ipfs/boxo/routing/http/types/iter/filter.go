package iter

// Filter returns an iterator that filters out values that don't satisfy the predicate f.
func Filter[T any](iter Iter[T], f func(t T) bool) *FilterIter[T] {
	return &FilterIter[T]{iter: iter, f: f}
}

type FilterIter[T any] struct {
	iter Iter[T]
	f    func(T) bool

	done bool
	val  T
}

func (f *FilterIter[T]) Next() bool {
	if f.done {
		return false
	}

	ok := f.iter.Next()
	f.done = !ok

	if f.done {
		return false
	}

	f.val = f.iter.Val()

	if f.f(f.val) {
		return true
	}

	return f.Next()
}

func (f *FilterIter[T]) Val() T {
	return f.val
}

func (f *FilterIter[T]) Close() error {
	return f.iter.Close()
}
