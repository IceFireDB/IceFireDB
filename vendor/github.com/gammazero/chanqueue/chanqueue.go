package chanqueue

import (
	"sync"

	"github.com/gammazero/deque"
)

// ChanQueue uses a queue to buffer data between input and output channels.
type ChanQueue[T any] struct {
	input     <-chan T
	inRdWr    chan T
	output    chan T
	length    chan int
	baseCap   int
	capacity  int
	closeOnce sync.Once
}

type Option[T any] func(*ChanQueue[T])

// WithBaseCapacity sets the base capacity of the internal buffer so that at
// least the specified number of items can always be stored without resizing
// the buffer.
func WithBaseCapacity[T any](n int) func(*ChanQueue[T]) {
	return func(c *ChanQueue[T]) {
		if n > 1 {
			c.baseCap = n
		}
	}
}

// WithCapacity sets the limit on the number of unread items that ChanQueue
// will hold. Unbuffered behavior is not supported (use a normal channel for
// that), and a value of zero or less configures the default of no limit.
//
// Example:
//
//	cq := chanqueue.New(chanqueue.WithCapacity[int](64))
func WithCapacity[T any](n int) func(*ChanQueue[T]) {
	return func(c *ChanQueue[T]) {
		if n < 1 {
			n = -1
		}
		c.capacity = n
	}
}

// WithInput uses an existing channel as the input channel, which is the
// channel used to write to the queue. This is used when buffering items that
// must be read from an existing channel. Be aware that calling Close or
// Shutdown will close this channel.
//
// Example:
//
//	in := make(chan int)
//	cq := chanqueue.New(chanqueue.WithInput[int](in))
func WithInput[T any](in chan T) func(*ChanQueue[T]) {
	return func(c *ChanQueue[T]) {
		if in != nil {
			c.input = in
			c.inRdWr = in
		}
	}
}

// WithInputRdOnly uses an existing channel as the input channel, which is the
// channel used to write to the queue. This is used when buffering items that
// must be read from an existing channel. Unlike WithInput, calling Close or
// Shutdown will not close this channel. It must be closed externally.
//
// Example:
//
//	in := make(chan int)
//	cq := chanqueue.New(chanqueue.WithInputRdOnly[int](in))
//	defer close(in) // cq.Close() does nothing
func WithInputRdOnly[T any](in <-chan T) func(*ChanQueue[T]) {
	return func(c *ChanQueue[T]) {
		if in != nil {
			c.input = in
		}
	}
}

// WithOutput uses an existing channel as the output channel, which is the
// channel used to read from the queue. This is used when buffering items that
// must be written to an existing channel. Be aware that ChanQueue will
// close this channel when no more items are available.
//
// Example:
//
//	out := make(chan int)
//	cq := chanqueue.New(chanqueue.WithOutput[int](out))
func WithOutput[T any](out chan T) func(*ChanQueue[T]) {
	return func(c *ChanQueue[T]) {
		if out != nil {
			c.output = out
		}
	}
}

// New creates a new ChanQueue that, by default, holds an unbounded number
// of items of the specified type.
func New[T any](options ...Option[T]) *ChanQueue[T] {
	cq := &ChanQueue[T]{
		length:   make(chan int),
		capacity: -1,
	}
	for _, opt := range options {
		opt(cq)
	}
	if cq.input == nil {
		in := make(chan T)
		cq.input = in
		cq.inRdWr = in
	}
	if cq.output == nil {
		cq.output = make(chan T)
	}
	go cq.bufferData()
	return cq
}

// NewRing creates a new ChanQueue with the specified buffer capacity, and
// circular buffer behavior. When the buffer is full, writing an additional
// item discards the oldest buffered item.
func NewRing[T any](options ...Option[T]) *ChanQueue[T] {
	cq := &ChanQueue[T]{
		length:   make(chan int),
		capacity: -1,
	}
	for _, opt := range options {
		opt(cq)
	}
	if cq.capacity < 1 {
		// Unbounded ring is the same as an unbounded queue.
		return New(options...)
	}
	if cq.input == nil {
		in := make(chan T)
		cq.input = in
		cq.inRdWr = in
	}
	if cq.output == nil {
		cq.output = make(chan T)
	}
	if cq.capacity == 1 {
		go cq.oneBufferData()
	} else {
		go cq.ringBufferData()
	}
	return cq
}

// In returns the write side of the channel.
func (cq *ChanQueue[T]) In() chan<- T {
	return cq.inRdWr
}

// Out returns the read side of the channel.
func (cq *ChanQueue[T]) Out() <-chan T {
	return cq.output
}

// Len returns the number of items buffered in the channel.
func (cq *ChanQueue[T]) Len() int {
	return <-cq.length
}

// Cap returns the capacity of the ChanQueue. Returns -1 if unbounded.
func (cq *ChanQueue[T]) Cap() int {
	return cq.capacity
}

// Close closes the input channel. This is the same as calling the builtin
// close on the input channel, except Close can be called multiple times..
// Additional input will panic, output will continue to be readable until there
// is no more data, and then the output channel is closed.
func (cq *ChanQueue[T]) Close() {
	cq.closeOnce.Do(func() {
		if cq.inRdWr != nil {
			close(cq.inRdWr)
		}
	})
}

// Shutdown calls Close then drains the channel to ensure that the internal
// goroutine finishes.
func (cq *ChanQueue[T]) Shutdown() {
	cq.Close()
	for range cq.output {
	}
}

func (cq *ChanQueue[T]) limitBaseCapToCapacity() {
	if cq.capacity > 0 && cq.baseCap > cq.capacity {
		cq.baseCap = cq.capacity
	}
}

// bufferData is the goroutine that transfers data from the In() chan to the
// buffer and from the buffer to the Out() chan.
func (cq *ChanQueue[T]) bufferData() {
	var buffer deque.Deque[T]
	var output chan T
	var next, zero T
	inputChan := cq.input
	input := inputChan

	cq.limitBaseCapToCapacity()
	buffer.SetBaseCap(cq.baseCap)

	for input != nil || output != nil {
		select {
		case elem, open := <-input:
			if open {
				// Push data from input chan to buffer.
				buffer.PushBack(elem)
			} else {
				// Input chan closed; do not select input chan.
				input = nil
				inputChan = nil
			}
		case output <- next:
			// Wrote buffered data to output chan. Remove item from buffer.
			buffer.PopFront()
		case cq.length <- buffer.Len():
		}

		if buffer.Len() == 0 {
			// No buffered data; do not select output chan.
			output = nil
			next = zero // set to zero to GC value
		} else {
			// Try to write it to output chan.
			output = cq.output
			next = buffer.Front()
		}

		if cq.capacity != -1 {
			// If buffer at capacity, then stop accepting input.
			if buffer.Len() >= cq.capacity {
				input = nil
			} else {
				input = inputChan
			}
		}
	}

	close(cq.output)
	close(cq.length)
}

// ringBufferData is the goroutine that transfers data from the In() chan to
// the buffer and from the buffer to the Out() chan, with circular buffer
// behavior of discarding the oldest item when writing to a full buffer.
func (cq *ChanQueue[T]) ringBufferData() {
	var buffer deque.Deque[T]
	var output chan T
	var next, zero T
	input := cq.input

	cq.limitBaseCapToCapacity()
	buffer.SetBaseCap(cq.baseCap)

	for input != nil || output != nil {
		select {
		case elem, open := <-input:
			if open {
				// Push data from input chan to buffer.
				buffer.PushBack(elem)
				if buffer.Len() > cq.capacity {
					buffer.PopFront()
				}
			} else {
				// Input chan closed; do not select input chan.
				input = nil
			}
		case output <- next:
			// Wrote buffered data to output chan. Remove item from buffer.
			buffer.PopFront()
		case cq.length <- buffer.Len():
		}

		if buffer.Len() == 0 {
			// No buffered data; do not select output chan.
			output = nil
			next = zero // set to zero to GC value
		} else {
			// Try to write it to output chan.
			output = cq.output
			next = buffer.Front()
		}
	}

	close(cq.output)
	close(cq.length)
}

// oneBufferData is the same as ringBufferData, but with a buffer size of 1.
func (cq *ChanQueue[T]) oneBufferData() {
	var bufLen int
	var output chan T
	var next, zero T
	input := cq.input

	for input != nil || output != nil {
		select {
		case elem, open := <-input:
			if open {
				// Push data from input chan to buffer.
				next = elem
				bufLen = 1
				// Try to write it to output chan.
				output = cq.output
			} else {
				// Input chan closed; do not select input chan.
				input = nil
			}
		case output <- next:
			// Wrote buffered data to output chan. Remove item from buffer.
			bufLen = 0
			next = zero // set to zero to GC value
			// No buffered data; do not select output chan.
			output = nil
		case cq.length <- bufLen:
		}
	}

	close(cq.output)
	close(cq.length)
}
