package webtransport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// Stream is a bidirectional WebTransport stream.
type Stream struct {
	sendStr *SendStream
	recvStr *ReceiveStream

	mx                             sync.Mutex
	sendSideClosed, recvSideClosed bool
	onClose                        func()
}

func newStream(
	str *quic.Stream,
	hdr []byte,
	sendFC *outgoingDataFlowController,
	receiveFC *incomingDataFlowController,
	queueCapsule func(capsule),
	onFlowControlError func(error),
	onClose func(),
	receiveHeaderLen int64,
) *Stream {
	s := &Stream{onClose: onClose}
	s.sendStr = newSendStream(str, hdr, sendFC, queueCapsule, func() { s.registerClose(true) })
	s.recvStr = newReceiveStream(str, receiveHeaderLen, func() { s.registerClose(false) }, receiveFC, onFlowControlError)
	return s
}

// StreamID returns the ID of the underlying QUIC stream.
func (s *Stream) StreamID() quic.StreamID {
	return s.recvStr.StreamID()
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
	if streamErr, ok := errors.AsType[*quic.StreamError](err); ok {
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
