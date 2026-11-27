package iter

// Limit returns an iterator that yields at most limit values from iter.
// A limit of 0 or less means no limit, in which case the returned
// iterator behaves like the one passed in.
func Limit[T any](iter Iter[T], limit int) *LimitIter[T] {
	return &LimitIter[T]{iter: iter, limit: limit}
}

// LimitIter is an [Iter] that stops after a fixed number of values, even
// if the underlying iterator has more. Close cascades to that iterator.
type LimitIter[T any] struct {
	iter  Iter[T]
	limit int
	count int
}

func (l *LimitIter[T]) Next() bool {
	if l.limit > 0 && l.count >= l.limit {
		return false
	}
	if !l.iter.Next() {
		return false
	}
	l.count++
	return true
}

func (l *LimitIter[T]) Val() T {
	return l.iter.Val()
}

func (l *LimitIter[T]) Close() error {
	return l.iter.Close()
}
