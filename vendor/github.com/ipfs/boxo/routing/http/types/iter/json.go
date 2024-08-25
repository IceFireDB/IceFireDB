package iter

import (
	"encoding/json"
	"errors"
	"io"
)

// FromReaderJSON returns an iterator over the given reader that reads whitespace-delimited JSON values.
func FromReaderJSON[T any](r io.Reader) *JSONIter[T] {
	return &JSONIter[T]{Decoder: json.NewDecoder(r), Reader: r}
}

// JSONIter iterates over whitespace-delimited JSON values of a byte stream.
// This closes the reader if it is a closer, to faciliate easy reading of HTTP responses.
type JSONIter[T any] struct {
	Decoder *json.Decoder
	Reader  io.Reader

	done bool
	res  Result[T]
}

func (j *JSONIter[T]) Next() bool {
	var val T

	if j.done {
		return false
	}

	err := j.Decoder.Decode(&val)

	j.res.Val, j.res.Err = val, err

	// EOF is not an error, it just marks the end of iteration
	if errors.Is(err, io.EOF) {
		j.done = true
		j.res.Err = nil
		return false
	}

	// stop iterating on an error
	if j.res.Err != nil {
		j.done = true
	}

	return true
}

func (j *JSONIter[T]) Val() Result[T] {
	return j.res
}

func (j *JSONIter[T]) Close() error {
	j.done = true
	if closer, ok := j.Reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
