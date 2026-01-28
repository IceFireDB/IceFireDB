package webtransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

type quicSendStream interface {
	io.WriteCloser
	CancelWrite(quic.StreamErrorCode)
	Context() context.Context
	SetWriteDeadline(time.Time) error
	SetReliableBoundary()
}

var (
	_ quicSendStream = &quic.SendStream{}
	_ quicSendStream = &quic.Stream{}
)

type quicReceiveStream interface {
	io.Reader
	CancelRead(quic.StreamErrorCode)
	SetReadDeadline(time.Time) error
}

var (
	_ quicReceiveStream = &quic.ReceiveStream{}
	_ quicReceiveStream = &quic.Stream{}
)

// A SendStream is a unidirectional WebTransport send stream.
type SendStream struct {
	str quicSendStream
	// WebTransport stream header.
	// Set by the constructor, set to nil once sent out.
	// Might be initialized to nil if this sendStream is part of an incoming bidirectional stream.
	streamHdr   []byte
	streamHdrMu sync.Mutex
	// Set to true when a goroutine is spawned to send the header asynchronously.
	// This only happens if the stream is closed / reset immediately after creation.
	sendingHdrAsync bool

	onClose func() // to remove the stream from the streamsMap

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error

	deadlineMu       sync.Mutex
	writeDeadline    time.Time
	deadlineNotifyCh chan struct{} // receives a value when deadline changes
}

func newSendStream(str quicSendStream, hdr []byte, onClose func()) *SendStream {
	return &SendStream{
		str:       str,
		closed:    make(chan struct{}),
		streamHdr: hdr,
		onClose:   onClose,
	}
}

// Write writes data to the stream.
// Write can be made to time out using [SendStream.SetWriteDeadline].
// If the stream was canceled, the error is a [StreamError].
func (s *SendStream) Write(b []byte) (int, error) {
	n, err := s.write(b)
	if err != nil && !isTimeoutError(err) {
		s.onClose()
	}
	var strErr *quic.StreamError
	if errors.As(err, &strErr) && strErr.ErrorCode == WTSessionGoneErrorCode {
		return n, s.handleSessionGoneError()
	}
	return n, maybeConvertStreamError(err)
}

// handleSessionGoneError waits for the session to be closed after receiving a WTSessionGoneErrorCode.
// If the peer is initiating the session close, we might need to wait for the CONNECT stream to be closed.
// While a malicious peer might withhold the session close, this is not an interesting attack vector:
// 1. a WebTransport stream consumes very little memory, and
// 2. the number of concurrent WebTransport sessions is limited.
func (s *SendStream) handleSessionGoneError() error {
	s.deadlineMu.Lock()
	if s.deadlineNotifyCh == nil {
		s.deadlineNotifyCh = make(chan struct{}, 1)
	}
	s.deadlineMu.Unlock()

	for {
		s.deadlineMu.Lock()
		deadline := s.writeDeadline
		s.deadlineMu.Unlock()

		var timerCh <-chan time.Time
		if !deadline.IsZero() {
			if d := time.Until(deadline); d > 0 {
				timerCh = time.After(d)
			} else {
				return os.ErrDeadlineExceeded
			}
		}
		select {
		case <-s.closed:
			return s.closeErr
		case <-timerCh:
			return os.ErrDeadlineExceeded
		case <-s.deadlineNotifyCh:
		}
	}
}

func (s *SendStream) write(b []byte) (int, error) {
	s.streamHdrMu.Lock()
	err := s.maybeSendStreamHeader()
	s.streamHdrMu.Unlock()
	if err != nil {
		return 0, err
	}
	return s.str.Write(b)
}

func (s *SendStream) maybeSendStreamHeader() error {
	if len(s.streamHdr) == 0 {
		return nil
	}
	n, err := s.str.Write(s.streamHdr)
	if n > 0 {
		s.streamHdr = s.streamHdr[n:]
	}
	s.str.SetReliableBoundary()
	if err != nil {
		return err
	}
	s.streamHdr = nil
	return nil
}

// CancelWrite aborts sending on this stream.
// Data already written, but not yet delivered to the peer is not guaranteed to be delivered reliably.
// Write will unblock immediately, and future calls to Write will fail.
// When called multiple times it is a no-op.
func (s *SendStream) CancelWrite(e StreamErrorCode) {
	// if a Goroutine is already sending the header, return immediately
	s.streamHdrMu.Lock()
	if s.sendingHdrAsync {
		s.streamHdrMu.Unlock()
		return
	}

	if len(s.streamHdr) > 0 {
		// Sending the stream header might block if we are blocked by flow control.
		// Send a stream header async so that CancelWrite can return immediately.
		s.sendingHdrAsync = true
		streamHdr := s.streamHdr
		s.streamHdr = nil
		s.streamHdrMu.Unlock()

		go func() {
			s.SetWriteDeadline(time.Time{})
			_, _ = s.str.Write(streamHdr)
			s.str.SetReliableBoundary()
			s.str.CancelWrite(webtransportCodeToHTTPCode(e))
			s.onClose()
		}()
		return
	}
	s.streamHdrMu.Unlock()

	s.str.CancelWrite(webtransportCodeToHTTPCode(e))
	s.onClose()
}

func (s *SendStream) closeWithSession(err error) {
	s.closeOnce.Do(func() {
		s.closeErr = err
		s.str.CancelWrite(WTSessionGoneErrorCode)
		close(s.closed)
	})
}

// Close closes the write-direction of the stream.
// Future calls to Write are not permitted after calling Close.
func (s *SendStream) Close() error {
	// if a Goroutine is already sending the header, return immediately
	s.streamHdrMu.Lock()
	if s.sendingHdrAsync {
		s.streamHdrMu.Unlock()
		return nil
	}

	if len(s.streamHdr) > 0 {
		// Sending the stream header might block if we are blocked by flow control.
		// Send a stream header async so that CancelWrite can return immediately.
		s.sendingHdrAsync = true
		streamHdr := s.streamHdr
		s.streamHdr = nil
		s.streamHdrMu.Unlock()

		go func() {
			s.SetWriteDeadline(time.Time{})
			_, _ = s.str.Write(streamHdr)
			s.str.SetReliableBoundary()
			_ = s.str.Close()
			s.onClose()
		}()
		return nil
	}
	s.streamHdrMu.Unlock()

	s.onClose()
	return maybeConvertStreamError(s.str.Close())
}

// The Context is canceled as soon as the write-side of the stream is closed.
// This happens when Close() or CancelWrite() is called, or when the peer
// cancels the read-side of their stream.
// The cancellation cause is set to the error that caused the stream to
// close, or `context.Canceled` in case the stream is closed without error.
func (s *SendStream) Context() context.Context {
	return s.str.Context()
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that
// some data was successfully written.
// A zero value for t means Write will not time out.
func (s *SendStream) SetWriteDeadline(t time.Time) error {
	s.deadlineMu.Lock()
	s.writeDeadline = t
	if s.deadlineNotifyCh != nil {
		select {
		case s.deadlineNotifyCh <- struct{}{}:
		default:
		}
	}
	s.deadlineMu.Unlock()

	return maybeConvertStreamError(s.str.SetWriteDeadline(t))
}

// A ReceiveStream is a unidirectional WebTransport receive stream.
type ReceiveStream struct {
	str quicReceiveStream

	onClose func() // to remove the stream from the streamsMap

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error

	deadlineMu       sync.Mutex
	readDeadline     time.Time
	deadlineNotifyCh chan struct{} // receives a value when deadline changes
}

func newReceiveStream(str quicReceiveStream, onClose func()) *ReceiveStream {
	return &ReceiveStream{
		str:     str,
		closed:  make(chan struct{}),
		onClose: onClose,
	}
}

// Read reads data from the stream.
// Read can be made to time out using [ReceiveStream.SetReadDeadline].
// If the stream was canceled, the error is a [StreamError].
func (s *ReceiveStream) Read(b []byte) (int, error) {
	n, err := s.str.Read(b)
	if err != nil && !isTimeoutError(err) {
		s.onClose()
	}
	var strErr *quic.StreamError
	if errors.As(err, &strErr) && strErr.ErrorCode == WTSessionGoneErrorCode {
		return n, s.handleSessionGoneError()
	}
	return n, maybeConvertStreamError(err)
}

// handleSessionGoneError waits for the session to be closed after receiving a WTSessionGoneErrorCode.
// If the peer is initiating the session close, we might need to wait for the CONNECT stream to be closed.
// While a malicious peer might withhold the session close, this is not an interesting attack vector:
// 1. a WebTransport stream consumes very little memory, and
// 2. the number of concurrent WebTransport sessions is limited.
func (s *ReceiveStream) handleSessionGoneError() error {
	s.deadlineMu.Lock()
	if s.deadlineNotifyCh == nil {
		s.deadlineNotifyCh = make(chan struct{}, 1)
	}
	s.deadlineMu.Unlock()

	for {
		s.deadlineMu.Lock()
		deadline := s.readDeadline
		s.deadlineMu.Unlock()

		var timerCh <-chan time.Time
		if !deadline.IsZero() {
			if d := time.Until(deadline); d > 0 {
				timerCh = time.After(d)
			} else {
				return os.ErrDeadlineExceeded
			}
		}
		select {
		case <-s.closed:
			return s.closeErr
		case <-timerCh:
			return os.ErrDeadlineExceeded
		case <-s.deadlineNotifyCh:
		}
	}
}

// CancelRead aborts receiving on this stream.
// It instructs the peer to stop transmitting stream data.
// Read will unblock immediately, and future Read calls will fail.
// When called multiple times it is a no-op.
func (s *ReceiveStream) CancelRead(e StreamErrorCode) {
	s.str.CancelRead(webtransportCodeToHTTPCode(e))
	s.onClose()
}

func (s *ReceiveStream) closeWithSession(err error) {
	s.closeOnce.Do(func() {
		s.closeErr = err
		s.str.CancelRead(WTSessionGoneErrorCode)
		close(s.closed)
	})
}

// SetReadDeadline sets the deadline for future Read calls and
// any currently-blocked Read call.
// A zero value for t means Read will not time out.
func (s *ReceiveStream) SetReadDeadline(t time.Time) error {
	s.deadlineMu.Lock()
	s.readDeadline = t
	if s.deadlineNotifyCh != nil {
		select {
		case s.deadlineNotifyCh <- struct{}{}:
		default:
		}
	}
	s.deadlineMu.Unlock()

	return maybeConvertStreamError(s.str.SetReadDeadline(t))
}

// Stream is a bidirectional WebTransport stream.
type Stream struct {
	sendStr *SendStream
	recvStr *ReceiveStream

	mx                             sync.Mutex
	sendSideClosed, recvSideClosed bool
	onClose                        func()
}

func newStream(str *quic.Stream, hdr []byte, onClose func()) *Stream {
	s := &Stream{onClose: onClose}
	s.sendStr = newSendStream(str, hdr, func() { s.registerClose(true) })
	s.recvStr = newReceiveStream(str, func() { s.registerClose(false) })
	return s
}

// Write writes data to the stream.
// Write can be made to time out using [Stream.SetWriteDeadline] or [Stream.SetDeadline].
// If the stream was canceled, the error is a [StreamError].
func (s *Stream) Write(b []byte) (int, error) {
	return s.sendStr.Write(b)
}

// Read reads data from the stream.
// Read can be made to time out using [Stream.SetReadDeadline] and [Stream.SetDeadline].
// If the stream was canceled, the error is a [StreamError].
func (s *Stream) Read(b []byte) (int, error) {
	return s.recvStr.Read(b)
}

// CancelWrite aborts sending on this stream.
// See [SendStream.CancelWrite] for more details.
func (s *Stream) CancelWrite(e StreamErrorCode) {
	s.sendStr.CancelWrite(e)
}

// CancelRead aborts receiving on this stream.
// See [ReceiveStream.CancelRead] for more details.
func (s *Stream) CancelRead(e StreamErrorCode) {
	s.recvStr.CancelRead(e)
}

// Close closes the send-direction of the stream.
// It does not close the receive-direction of the stream.
func (s *Stream) Close() error {
	return s.sendStr.Close()
}

func (s *Stream) registerClose(isSendSide bool) {
	s.mx.Lock()
	if isSendSide {
		s.sendSideClosed = true
	} else {
		s.recvSideClosed = true
	}
	isClosed := s.sendSideClosed && s.recvSideClosed
	s.mx.Unlock()

	if isClosed {
		s.onClose()
	}
}

func (s *Stream) closeWithSession(err error) {
	s.sendStr.closeWithSession(err)
	s.recvStr.closeWithSession(err)
}

// The Context is canceled as soon as the write-side of the stream is closed.
// See [SendStream.Context] for more details.
func (s *Stream) Context() context.Context {
	return s.sendStr.Context()
}

// SetWriteDeadline sets the deadline for future Write calls.
// See [SendStream.SetWriteDeadline] for more details.
func (s *Stream) SetWriteDeadline(t time.Time) error {
	return s.sendStr.SetWriteDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls.
// See [ReceiveStream.SetReadDeadline] for more details.
func (s *Stream) SetReadDeadline(t time.Time) error {
	return s.recvStr.SetReadDeadline(t)
}

// SetDeadline sets the read and write deadlines associated with the stream.
// It is equivalent to calling both SetReadDeadline and SetWriteDeadline.
func (s *Stream) SetDeadline(t time.Time) error {
	err1 := s.SetWriteDeadline(t)
	err2 := s.SetReadDeadline(t)
	return errors.Join(err1, err2)
}

func maybeConvertStreamError(err error) error {
	if err == nil {
		return nil
	}
	var streamErr *quic.StreamError
	if errors.As(err, &streamErr) {
		errorCode, cerr := httpCodeToWebtransportCode(streamErr.ErrorCode)
		if cerr != nil {
			return fmt.Errorf("stream reset, but failed to convert stream error %d: %w", streamErr.ErrorCode, cerr)
		}
		return &StreamError{
			ErrorCode: errorCode,
			Remote:    streamErr.Remote,
		}
	}
	return err
}

func isTimeoutError(err error) bool {
	nerr, ok := err.(net.Error)
	if !ok {
		return false
	}
	return nerr.Timeout()
}
