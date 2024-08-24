package yamux

import (
	"fmt"
	"io"
	"sync"

	pool "github.com/libp2p/go-buffer-pool"
)

// asyncSendErr is used to try an async send of an error
func asyncSendErr(ch chan error, err error) {
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

// asyncNotify is used to signal a waiting goroutine
func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// min computes the minimum of a set of values
func min(values ...uint32) uint32 {
	m := values[0]
	for _, v := range values[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

// The segmented buffer looks like:
//
//      |     data      | empty space       |
//       < window (10)                     >
//       < len (5)     > < cap (5)         >
//
// As data is read, the buffer gets updated like so:
//
//         |     data   | empty space       |
//          < window (8)                   >
//          < len (3)  > < cap (5)         >
//
// It can then grow as follows (given a "max" of 10):
//
//
//         |     data   | empty space          |
//          < window (10)                     >
//          < len (3)  > < cap (7)            >
//
// Data can then be written into the empty space, expanding len,
// and shrinking cap:
//
//         |     data       | empty space      |
//          < window (10)                     >
//          < len (5)      > < cap (5)        >
//
type segmentedBuffer struct {
	cap uint32
	len uint32
	bm  sync.Mutex
	// read position in b[0].
	// We must not reslice any of the buffers in b, as we need to put them back into the pool.
	readPos int
	b       [][]byte
}

// NewSegmentedBuffer allocates a ring buffer.
func newSegmentedBuffer(initialCapacity uint32) segmentedBuffer {
	return segmentedBuffer{cap: initialCapacity, b: make([][]byte, 0)}
}

// Len is the amount of data in the receive buffer.
func (s *segmentedBuffer) Len() uint32 {
	s.bm.Lock()
	defer s.bm.Unlock()
	return s.len
}

// If the space to write into + current buffer size has grown to half of the window size,
// grow up to that max size, and indicate how much additional space was reserved.
func (s *segmentedBuffer) GrowTo(max uint32, force bool) (bool, uint32) {
	s.bm.Lock()
	defer s.bm.Unlock()

	currentWindow := s.cap + s.len
	if currentWindow >= max {
		return force, 0
	}
	delta := max - currentWindow

	if delta < (max/2) && !force {
		return false, 0
	}

	s.cap += delta
	return true, delta
}

func (s *segmentedBuffer) Read(b []byte) (int, error) {
	s.bm.Lock()
	defer s.bm.Unlock()
	if len(s.b) == 0 {
		return 0, io.EOF
	}
	data := s.b[0][s.readPos:]
	n := copy(b, data)
	if n == len(data) {
		pool.Put(s.b[0])
		s.b[0] = nil
		s.b = s.b[1:]
		s.readPos = 0
	} else {
		s.readPos += n
	}
	if n > 0 {
		s.len -= uint32(n)
	}
	return n, nil
}

func (s *segmentedBuffer) checkOverflow(l uint32) error {
	s.bm.Lock()
	defer s.bm.Unlock()
	if s.cap < l {
		return fmt.Errorf("receive window exceeded (remain: %d, recv: %d)", s.cap, l)
	}
	return nil
}

func (s *segmentedBuffer) Append(input io.Reader, length uint32) error {
	if err := s.checkOverflow(length); err != nil {
		return err
	}

	dst := pool.Get(int(length))
	n, err := io.ReadFull(input, dst)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	s.bm.Lock()
	defer s.bm.Unlock()
	if n > 0 {
		s.len += uint32(n)
		s.cap -= uint32(n)
		s.b = append(s.b, dst[0:n])
	}
	return err
}
