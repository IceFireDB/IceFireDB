# chanqueue
[![GoDoc](https://pkg.go.dev/badge/github.com/gammazero/chanqueue)](https://pkg.go.dev/github.com/gammazero/chanqueue)
[![Build Status](https://github.com/gammazero/chanqueue/actions/workflows/go.yml/badge.svg)](https://github.com/gammazero/chanqueue/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/gammazero/chanqueue)](https://goreportcard.com/report/github.com/gammazero/chanqueue)
[![codecov](https://codecov.io/gh/gammazero/chanqueue/branch/master/graph/badge.svg)](https://codecov.io/gh/gammazero/chanqueue)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> Concurrently access a dynamic queue using channels.

ChanQueue implements a queue that uses channels for input and output to provide concurrent access to a dynamically-sized queue. This allows the queue to be used like a channel, in a thread-safe manner. Closing the input channel closes the output channel when all queued items are read, consistent with channel behavior. In other words a ChanQueue is a dynamically buffered channel with up to infinite capacity.

ChanQueue also supports circular buffer behavior when created using `NewRing`. When the buffer is full, writing an additional item discards the oldest buffered item.

When specifying an unlimited buffer capacity use caution as the buffer is still limited by the resources available on the host system.

The ChanQueue buffer auto-resizes according to the number of items buffered. For more information on the internal queue, see: https://github.com/gammazero/deque

ChanQueue uses generics to contain items of the type specified. To create a ChanQueue that holds a specific type, provide a type argument to `New`, or to options passed to `New`. For example:
```go
intQueue := chanqueue.New[int]()
stringQueue := chanqueue.New[string]()
limitedQueue := chanqueue.New(chanqueue.WithCapacity[string](256))
```

ChanQueue can be used to provide buffering between existing channels. Using an existing read chan, write chan, or both are supported:
```go
in := make(chan int)
out := make(chan int)

// Create a buffer between in and out channels.
chanqueue.New(chanqueue.WithInput[int](in), chanqueue.WithOutput[int](out))
// ...
close(in) // this will close cq when all output is read.
```

## Example

```go
cq := chanqueue.New[int]()
result := make(chan int)

// Start consumer.
go func(r <-chan int) {
    var sum int
    for i := range r {
        sum += i
    }
    result <- sum
}(cq.Out())

// Write numbers to queue.
for i := 1; i <= 10; i++ {
    cq.In() <- i
}
cq.Close()

// Wait for consumer to send result.
val := <-result
fmt.Println("Result:", val)
```
