package webtransport

import (
	"context"
	"fmt"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type acceptQueue[T any] struct {
	// The channel is used to notify consumers about new incoming items.
	// Needs to be buffered to preserve the notification if an item is enqueued
	// between checking the queue and waiting on this channel.
	c chan struct{}

	mx sync.Mutex
	// Contains all the streams waiting to be accepted.
	// There's no explicit limit to the length of the queue, but it is implicitly
	// limited by the stream flow control provided by QUIC.
	queue []T
}

func newAcceptQueue[T any]() *acceptQueue[T] {
	return &acceptQueue[T]{c: make(chan struct{}, 1)}
}

func (q *acceptQueue[T]) add(str T) {
	q.mx.Lock()
	q.queue = append(q.queue, str)
	q.mx.Unlock()

	select {
	case q.c <- struct{}{}:
	default:
	}
}

func (q *acceptQueue[T]) next() (T, bool) {
	q.mx.Lock()
	defer q.mx.Unlock()

	if len(q.queue) == 0 {
		return *new(T), false
	}
	str := q.queue[0]
	q.queue = q.queue[1:]
	return str, true
}

type incomingStream interface {
	*Stream | *ReceiveStream
	closeWithSession(error)
}

type incomingStreamsMap[T incomingStream] struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	acceptQueue     acceptQueue[T]
	queueMaxStreams func(uint64)

	maxStreamsMx     sync.Mutex
	queuedMaxStreams uint64

	mx sync.Mutex
	m  map[quic.StreamID]T

	numStreams     uint64 // total number of streams opened by the peer
	maxStreams     uint64 // stream limit, as advertised to the peer
	maxOpenStreams uint64 // maximum number of concurrently open streams
}

func newIncomingStreamsMap[T incomingStream](maxOpenStreams uint64, queueMaxStreams func(uint64)) *incomingStreamsMap[T] {
	maxOpenStreams = min(maxOpenStreams, maxStreamsLimit)
	if queueMaxStreams == nil {
		queueMaxStreams = func(uint64) {}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	return &incomingStreamsMap[T]{
		ctx:              ctx,
		cancel:           cancel,
		acceptQueue:      *newAcceptQueue[T](),
		queueMaxStreams:  queueMaxStreams,
		queuedMaxStreams: maxOpenStreams,
		maxStreams:       maxOpenStreams,
		maxOpenStreams:   maxOpenStreams,
		m:                make(map[quic.StreamID]T),
	}
}

func (s *incomingStreamsMap[T]) AddStream(id quic.StreamID, str T) error {
	s.mx.Lock()
	if closeErr := context.Cause(s.ctx); closeErr != nil {
		s.mx.Unlock()
		str.closeWithSession(closeErr)
		return nil
	}
	if s.numStreams >= s.maxStreams {
		s.mx.Unlock()
		return &http3.Error{
			ErrorCode:    http3.ErrCode(WTFlowControlErrorCode),
			ErrorMessage: fmt.Sprintf("webtransport: peer opened more than %d streams", s.maxStreams),
		}
	}
	s.numStreams++
	s.m[id] = str
	s.mx.Unlock()

	s.acceptQueue.add(str)
	return nil
}

func (s *incomingStreamsMap[T]) RemoveStream(id quic.StreamID) {
	s.mx.Lock()
	if _, ok := s.m[id]; !ok {
		s.mx.Unlock()
		return
	}
	delete(s.m, id)

	var maxStreams uint64
	var shouldQueue bool
	if uint64(len(s.m)) < s.maxOpenStreams && s.maxStreams < maxStreamsLimit {
		maxStreams = min(s.numStreams+s.maxOpenStreams-uint64(len(s.m)), maxStreamsLimit)
		if maxStreams > s.maxStreams {
			s.maxStreams = maxStreams
			shouldQueue = true
		}
	}
	s.mx.Unlock()
	if shouldQueue {
		s.maxStreamsMx.Lock()
		if maxStreams > s.queuedMaxStreams {
			s.queuedMaxStreams = maxStreams
			// WT_MAX_STREAMS must not be queued out of order
			s.queueMaxStreams(maxStreams)
		}
		s.maxStreamsMx.Unlock()
	}
}

func (s *incomingStreamsMap[T]) AcceptStream(ctx context.Context) (T, error) {
	var zero T
	if closeErr := context.Cause(s.ctx); closeErr != nil {
		return zero, closeErr
	}

	for {
		// If there's a stream in the accept queue, return it immediately.
		if str, ok := s.acceptQueue.next(); ok {
			return str, nil
		}
		// No stream in the accept queue. Wait until we accept one.
		select {
		case <-s.ctx.Done():
			return zero, context.Cause(s.ctx)
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-s.acceptQueue.c:
		}
	}
}

func (s *incomingStreamsMap[T]) CloseSession(err error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if context.Cause(s.ctx) != nil {
		return
	}
	s.cancel(err)

	for _, str := range s.m {
		str.closeWithSession(err)
	}
	s.m = nil
}
