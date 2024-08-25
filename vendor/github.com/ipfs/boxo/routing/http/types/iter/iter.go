package iter

import "fmt"

// Iter is an iterator of arbitrary values.
// Iterators are generally not goroutine-safe, to make them safe just read from them into a channel.
// For our use cases, these usually have a single reader. This motivates iterators instead of channels,
// since the overhead of goroutines+channels has a significant performance cost.
// Using an iterator, you can read results directly without necessarily involving the Go scheduler.
//
// There are a lot of options for an iterator interface, this one was picked for ease-of-use
// and for highest probability of consumers using it correctly.
// E.g. because there is a separate method for the value, it's easier to use in a loop but harder to implement.
//
// Hopefully in the future, Go will include an iterator in the language and we can remove this.
type Iter[T any] interface {
	// Next sets the iterator to the next value, returning true if an attempt was made to get the next value.
	Next() bool
	Val() T
	// Close closes the iterator and any underlying resources. Failure to close an iterator may result in resource leakage (goroutines, FDs, conns, etc.).
	Close() error
}

type ResultIter[T any] interface{ Iter[Result[T]] }

type Result[T any] struct {
	Val T
	Err error
}

// ToResultIter returns an iterator that wraps each value in a Result.
func ToResultIter[T any](iter Iter[T]) Iter[Result[T]] {
	return Map(iter, func(t T) Result[T] {
		return Result[T]{Val: t}
	})
}

func ReadAll[T any](iter Iter[T]) []T {
	if iter == nil {
		return nil
	}
	defer iter.Close()
	var vs []T
	for iter.Next() {
		vs = append(vs, iter.Val())
	}
	return vs
}

func ReadAllResults[T any](iter ResultIter[T]) ([]T, error) {
	var (
		vs []T
		i  int
	)

	for iter.Next() {
		res := iter.Val()
		if res.Err != nil {
			return nil, fmt.Errorf("error on result %d: %w", i, res.Err)
		}
		vs = append(vs, res.Val)
		i++
	}

	return vs, nil
}
