package iter

// FromSlice returns an iterator over the given slice.
func FromSlice[T any](s []T) *SliceIter[T] {
	return &SliceIter[T]{Slice: s, i: -1}
}

type SliceIter[T any] struct {
	Slice []T
	i     int
	val   T
}

func (s *SliceIter[T]) Next() bool {
	s.i++
	if s.i >= len(s.Slice) {
		return false
	}
	s.val = s.Slice[s.i]
	return true
}

func (s *SliceIter[T]) Val() T {
	return s.val
}

func (s *SliceIter[T]) Close() error {
	return nil
}
