package webtransport

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// StreamLimitReachedError is returned from OpenStream and OpenUniStream
// when it is not possible to open a new stream because the peer's stream limit is reached.
type StreamLimitReachedError struct{}

func (e StreamLimitReachedError) Error() string { return "too many open streams" }

var errMaxStreamsNotIncreased = errors.New("webtransport: WT_MAX_STREAMS capsule didn't increase stream limit")

const (
	maxOutgoingStreams = 1 << 60
	invalidStreamID    = quic.StreamID(-1)
)

type outgoingStream interface {
	closeWithSession(error)
}

type outgoingStreamsMap[T outgoingStream] struct {
	openStream     func() (T, quic.StreamID, error)
	openStreamSync func(context.Context) (T, quic.StreamID, error)
	queueBlocked   func(uint64)

	mx         sync.Mutex
	closeErr   error
	streamCtxs map[int]context.CancelFunc

	openQueue   []chan struct{}
	numStreams  uint64 // total number of streams opened for this session
	maxStreams  uint64 // stream limit, as advertised by the peer
	blockedSent bool   // was a WT_STREAMS_BLOCKED sent for the current maxStreams

	m map[quic.StreamID]T
}

func newOutgoingStreamsMap[T outgoingStream](
	maxStreams uint64,
	openStream func() (T, quic.StreamID, error),
	openStreamSync func(context.Context) (T, quic.StreamID, error),
	queueBlocked func(uint64),
) *outgoingStreamsMap[T] {
	return &outgoingStreamsMap[T]{
		openStream:     openStream,
		openStreamSync: openStreamSync,
		queueBlocked:   queueBlocked,
		streamCtxs:     make(map[int]context.CancelFunc),
		maxStreams:     maxStreams,
		m:              make(map[quic.StreamID]T),
	}
}

func newOutgoingBidiStreamsMap(
	conn *quic.Conn,
	sessionID sessionID,
	maxStreams uint64,
	sendFC *outgoingDataFlowController,
	receiveFC *incomingDataFlowController,
	queueCapsule func(capsule),
	onFlowControlError func(error),
) *outgoingStreamsMap[*Stream] {
	streamHdr := newOutgoingStreamHeader(webTransportFrameType, sessionID)
	var streams *outgoingStreamsMap[*Stream]
	streams = newOutgoingStreamsMap(
		maxStreams,
		func() (*Stream, quic.StreamID, error) {
			qstr, err := conn.OpenStream()
			if err != nil {
				return nil, 0, err
			}
			id := qstr.StreamID()
			str := newStream(qstr, streamHdr, sendFC, receiveFC, queueCapsule, onFlowControlError, func() { streams.removeStream(id) }, 0)
			return str, id, nil
		},
		func(ctx context.Context) (*Stream, quic.StreamID, error) {
			qstr, err := conn.OpenStreamSync(ctx)
			if err != nil {
				return nil, invalidStreamID, err
			}
			id := qstr.StreamID()
			str := newStream(qstr, streamHdr, sendFC, receiveFC, queueCapsule, onFlowControlError, func() { streams.removeStream(id) }, 0)
			return str, id, nil
		},
		func(limit uint64) { queueCapsule(streamsBlockedBidiCapsule{MaximumStreams: limit}) },
	)
	return streams
}

func newOutgoingUniStreamsMap(
	conn *quic.Conn,
	sessionID sessionID,
	maxStreams uint64,
	fc *outgoingDataFlowController,
	queueCapsule func(capsule),
) *outgoingStreamsMap[*SendStream] {
	streamHdr := newOutgoingStreamHeader(webTransportUniStreamType, sessionID)
	var streams *outgoingStreamsMap[*SendStream]
	streams = newOutgoingStreamsMap(
		maxStreams,
		func() (*SendStream, quic.StreamID, error) {
			qstr, err := conn.OpenUniStream()
			if err != nil {
				return nil, 0, err
			}
			id := qstr.StreamID()
			str := newSendStream(qstr, streamHdr, fc, queueCapsule, func() { streams.removeStream(id) })
			return str, id, nil
		},
		func(ctx context.Context) (*SendStream, quic.StreamID, error) {
			qstr, err := conn.OpenUniStreamSync(ctx)
			if err != nil {
				return nil, invalidStreamID, err
			}
			id := qstr.StreamID()
			str := newSendStream(qstr, streamHdr, fc, queueCapsule, func() { streams.removeStream(id) })
			return str, id, nil
		},
		func(limit uint64) { queueCapsule(streamsBlockedUniCapsule{MaximumStreams: limit}) },
	)
	return streams
}

func newOutgoingStreamHeader(streamType uint64, sessionID sessionID) []byte {
	hdr := make([]byte, 0, 2+quicvarint.Len(uint64(sessionID)))
	hdr = quicvarint.Append(hdr, streamType)
	return quicvarint.Append(hdr, uint64(sessionID))
}

func (s *outgoingStreamsMap[T]) removeStream(id quic.StreamID) {
	s.mx.Lock()
	delete(s.m, id)
	s.mx.Unlock()
}

func (s *outgoingStreamsMap[T]) OpenStream() (T, error) {
	s.mx.Lock()

	var zero T
	if s.closeErr != nil {
		s.mx.Unlock()
		return zero, s.closeErr
	}
	if len(s.openQueue) > 0 {
		s.mx.Unlock()
		return zero, &StreamLimitReachedError{}
	}

	if s.numStreams >= s.maxStreams {
		sendBlocked := !s.blockedSent
		maxStreams := s.maxStreams
		if sendBlocked {
			s.blockedSent = true
		}
		s.mx.Unlock()
		if sendBlocked {
			s.queueBlocked(maxStreams)
		}
		return zero, &StreamLimitReachedError{}
	}
	s.numStreams++

	str, id, err := s.openStream()
	if err != nil {
		s.numStreams--
		s.maybeUnblockOpenSync()
		s.mx.Unlock()
		return zero, err
	}
	s.m[id] = str
	s.mx.Unlock()
	return str, nil
}

func (s *outgoingStreamsMap[T]) addStreamCtxCancel(cancel context.CancelFunc) (id int) {
rand:
	id = rand.Int()
	if _, ok := s.streamCtxs[id]; ok {
		goto rand
	}
	s.streamCtxs[id] = cancel
	return id
}

func (s *outgoingStreamsMap[T]) OpenStreamSync(ctx context.Context) (T, error) {
	s.mx.Lock()
	var zero T
	if s.closeErr != nil {
		s.mx.Unlock()
		return zero, s.closeErr
	}
	if err := s.waitForStreamLimit(ctx); err != nil {
		s.mx.Unlock()
		return zero, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	id := s.addStreamCtxCancel(cancel)
	s.mx.Unlock()

	str, streamID, err := s.openStreamSync(ctx)

	s.mx.Lock()
	defer s.mx.Unlock()
	delete(s.streamCtxs, id)

	if streamID != invalidStreamID && s.closeErr != nil {
		str.closeWithSession(s.closeErr)
		return zero, s.closeErr
	}
	if err != nil {
		if s.closeErr == nil {
			s.numStreams--
			s.maybeUnblockOpenSync()
		}
		if s.closeErr != nil {
			return zero, s.closeErr
		}
		return zero, err
	}
	s.m[streamID] = str
	return str, nil
}

func (s *outgoingStreamsMap[T]) waitForStreamLimit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(s.openQueue) == 0 && s.numStreams < s.maxStreams {
		s.numStreams++
		return nil
	}

	waitChan := make(chan struct{}, 1)
	s.openQueue = append(s.openQueue, waitChan)

	if s.numStreams >= s.maxStreams && !s.blockedSent {
		s.blockedSent = true
		maxStreams := s.maxStreams
		s.mx.Unlock()
		s.queueBlocked(maxStreams)
		s.mx.Lock()
		if s.closeErr != nil {
			return s.closeErr
		}
	}

	s.mx.Unlock()
	select {
	case <-ctx.Done():
		s.mx.Lock()
		s.openQueue = slices.DeleteFunc(s.openQueue, func(c chan struct{}) bool {
			return c == waitChan
		})
		// If we just received a WT_MAX_STREAMS capsule, this might have been the next stream
		// that could be opened. Make sure we unblock the next OpenStreamSync call.
		s.maybeUnblockOpenSync()
		return ctx.Err()
	case <-waitChan:
	}

	s.mx.Lock()
	if s.closeErr != nil {
		return s.closeErr
	}
	if err := ctx.Err(); err != nil {
		s.openQueue = slices.DeleteFunc(s.openQueue, func(c chan struct{}) bool {
			return c == waitChan
		})
		s.maybeUnblockOpenSync()
		return err
	}

	s.numStreams++
	s.openQueue = s.openQueue[1:]
	if len(s.openQueue) > 0 && s.numStreams >= s.maxStreams {
		if !s.blockedSent {
			s.blockedSent = true
			maxStreams := s.maxStreams
			s.mx.Unlock()
			s.queueBlocked(maxStreams)
			s.mx.Lock()
			if s.closeErr != nil {
				return s.closeErr
			}
		}
	} else {
		s.maybeUnblockOpenSync()
	}
	return nil
}

func (s *outgoingStreamsMap[T]) UpdateStreamLimit(limit uint64) error {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.closeErr != nil {
		return nil
	}
	if limit <= s.maxStreams {
		return fmt.Errorf("%w: current limit: %d, received limit: %d", errMaxStreamsNotIncreased, s.maxStreams, limit)
	}
	s.maxStreams = limit
	s.blockedSent = false
	s.maybeUnblockOpenSync()
	return nil
}

// unblockOpenSync unblocks the next OpenStreamSync go-routine to open a new stream.
func (s *outgoingStreamsMap[T]) maybeUnblockOpenSync() {
	if len(s.openQueue) == 0 {
		return
	}
	if s.numStreams >= s.maxStreams {
		return
	}
	// maybeUnblockOpenSync is called both from OpenStreamSync and from UpdateStreamLimit.
	// It's sufficient to only unblock OpenStreamSync once.
	select {
	case s.openQueue[0] <- struct{}{}:
	default:
	}
}

func (s *outgoingStreamsMap[T]) CloseSession(err error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.closeErr != nil {
		return
	}
	s.closeErr = err

	for _, cancel := range s.streamCtxs {
		cancel()
	}
	for _, c := range s.openQueue {
		close(c)
	}
	s.openQueue = nil

	for _, str := range s.m {
		str.closeWithSession(err)
	}
	s.m = nil
}
