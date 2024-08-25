// Package chunk implements streaming block splitters.
// Splitters read data from a reader and provide byte slices (chunks)
// The size and contents of these slices depend on the splitting method
// used.
package chunk

import (
	"errors"
	"io"
	"math/bits"

	logging "github.com/ipfs/go-log/v2"
	pool "github.com/libp2p/go-buffer-pool"
)

var log = logging.Logger("chunk")

// maxOverAllocBytes is the maximum unused space a chunk can have without being
// reallocated to a smaller size to fit the data.
const maxOverAllocBytes = 1024

// A Splitter reads bytes from a Reader and creates "chunks" (byte slices)
// that can be used to build DAG nodes.
type Splitter interface {
	Reader() io.Reader
	NextBytes() ([]byte, error)
}

// SplitterGen is a splitter generator, given a reader.
type SplitterGen func(r io.Reader) Splitter

// DefaultSplitter returns a SizeSplitter with the DefaultBlockSize.
func DefaultSplitter(r io.Reader) Splitter {
	return NewSizeSplitter(r, DefaultBlockSize)
}

// SizeSplitterGen returns a SplitterGen function which will create
// a splitter with the given size when called.
func SizeSplitterGen(size int64) SplitterGen {
	return func(r io.Reader) Splitter {
		return NewSizeSplitter(r, size)
	}
}

// Chan returns a channel that receives each of the chunks produced
// by a splitter, along with another one for errors.
func Chan(s Splitter) (<-chan []byte, <-chan error) {
	out := make(chan []byte)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)

		// all-chunks loop (keep creating chunks)
		for {
			b, err := s.NextBytes()
			if err != nil {
				errs <- err
				return
			}

			out <- b
		}
	}()
	return out, errs
}

type sizeSplitterv2 struct {
	r    io.Reader
	size uint32
	err  error
}

// NewSizeSplitter returns a new size-based Splitter with the given block size.
func NewSizeSplitter(r io.Reader, size int64) Splitter {
	return &sizeSplitterv2{
		r:    r,
		size: uint32(size),
	}
}

// NextBytes produces a new chunk.
func (ss *sizeSplitterv2) NextBytes() ([]byte, error) {
	if ss.err != nil {
		return nil, ss.err
	}

	full := pool.Get(int(ss.size))
	n, err := io.ReadFull(ss.r, full)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			ss.err = io.EOF
			return reallocChunk(full, n), nil
		}
		pool.Put(full)
		return nil, err
	}
	return full, nil
}

func reallocChunk(full []byte, n int) []byte {
	// Do not return an empty buffer.
	if n == 0 {
		pool.Put(full)
		return nil
	}

	// If chunk is close enough to fully used.
	if cap(full)-n <= maxOverAllocBytes {
		return full[:n]
	}

	var small []byte
	// If reallocating to the nearest power of two saves space without leaving
	// too much unused space.
	powTwoSize := 1 << bits.Len32(uint32(n-1))
	if powTwoSize-n <= maxOverAllocBytes {
		small = make([]byte, n, powTwoSize)
	} else {
		small = make([]byte, n)
	}
	copy(small, full)
	pool.Put(full)
	return small
}

// Reader returns the io.Reader associated to this Splitter.
func (ss *sizeSplitterv2) Reader() io.Reader {
	return ss.r
}
