package webtransport

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

type quicReceiveStream interface {
	io.Reader
	StreamID() quic.StreamID
	CancelRead(quic.StreamErrorCode)
	SetReceiveFinalSizeCallback(func(int64))
	SetReadDeadline(time.Time) error
}

var (
	_ quicReceiveStream = &quic.ReceiveStream{}
	_ quicReceiveStream = &quic.Stream{}
)

// A ReceiveStream is a unidirectional WebTransport receive stream.
type ReceiveStream struct {
	str quicReceiveStream
	fc  *incomingDataFlowController

	flowControlMx      sync.Mutex
	bytesRead          int64 // QUIC stream bytes consumed, including the WebTransport stream header
	finalSizeKnown     bool
	onFlowControlError func(error)

	onClose func() // to remove the stream from the streamsMap

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error

	deadlineMu       sync.Mutex
	readDeadline     time.Time
	deadlineNotifyCh chan struct{} // receives a value when deadline changes
}

func newReceiveStream(
	str quicReceiveStream,
	streamHeaderLen int64,
	onClose func(),
	fc *incomingDataFlowController,
	onFlowControlError func(error),
) *ReceiveStream {
	s := &ReceiveStream{
		str:                str,
		fc:                 fc,
		bytesRead:          streamHeaderLen,
		onFlowControlError: onFlowControlError,
		closed:             make(chan struct{}),
		onClose:            onClose,
	}
	if fc != nil {
		str.SetReceiveFinalSizeCallback(s.onReceiveFinalSize)
	}
	return s
}

// StreamID returns the ID of the underlying QUIC stream.
func (s *ReceiveStream) StreamID() quic.StreamID {
	return s.str.StreamID()
}

// Read reads data from the stream.
// Read can be made to time out using [ReceiveStream.SetReadDeadline].
// If the stream was canceled, the error is a [StreamError].
func (s *ReceiveStream) Read(b []byte) (int, error) {
	n, err := s.str.Read(b)
	if s.fc != nil {
		newlyRead := int64(n)
		s.flowControlMx.Lock()
		if !s.finalSizeKnown {
			s.bytesRead += newlyRead
		} else {
			newlyRead = 0
		}
		s.flowControlMx.Unlock()
		s.addBytesRead(newlyRead)
	}
	var strErr *quic.StreamError
	if errors.As(err, &strErr) && strErr.ErrorCode == WTSessionGoneErrorCode {
		err = s.handleSessionGoneError()
	}
	if err != nil && !isTimeoutError(err) {
		s.onClose()
	}
	return n, maybeConvertStreamError(err)
}

func (s *ReceiveStream) onReceiveFinalSize(size int64) {
	s.flowControlMx.Lock()
	n := size - s.bytesRead
	s.bytesRead = size
	s.finalSizeKnown = true
	s.flowControlMx.Unlock()

	s.addBytesRead(n)
}

func (s *ReceiveStream) addBytesRead(n int64) {
	if s.fc == nil || n == 0 {
		return
	}
	if err := s.fc.AddBytesRead(n); err != nil && s.onFlowControlError != nil {
		s.onFlowControlError(err)
	}
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
