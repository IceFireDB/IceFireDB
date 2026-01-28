package webtransport

import (
	"context"
	"encoding/binary"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

// sessionID is the WebTransport Session ID
type sessionID uint64

const closeSessionCapsuleType http3.CapsuleType = 0x2843

const maxCloseCapsuleErrorMsgLen = 1024

type acceptQueue[T any] struct {
	mx sync.Mutex
	// The channel is used to notify consumers (via Chan) about new incoming items.
	// Needs to be buffered to preserve the notification if an item is enqueued
	// between a call to Next and to Chan.
	c chan struct{}
	// Contains all the streams waiting to be accepted.
	// There's no explicit limit to the length of the queue, but it is implicitly
	// limited by the stream flow control provided by QUIC.
	queue []T
}

func newAcceptQueue[T any]() *acceptQueue[T] {
	return &acceptQueue[T]{c: make(chan struct{}, 1)}
}

func (q *acceptQueue[T]) Add(str T) {
	q.mx.Lock()
	q.queue = append(q.queue, str)
	q.mx.Unlock()

	select {
	case q.c <- struct{}{}:
	default:
	}
}

func (q *acceptQueue[T]) Next() T {
	q.mx.Lock()
	defer q.mx.Unlock()

	if len(q.queue) == 0 {
		return *new(T)
	}
	str := q.queue[0]
	q.queue = q.queue[1:]
	return str
}

func (q *acceptQueue[T]) Chan() <-chan struct{} { return q.c }

type http3Stream interface {
	io.ReadWriteCloser
	ReceiveDatagram(context.Context) ([]byte, error)
	SendDatagram([]byte) error
	CancelRead(quic.StreamErrorCode)
	CancelWrite(quic.StreamErrorCode)
	SetWriteDeadline(time.Time) error
}

var (
	_ http3Stream = &http3.Stream{}
	_ http3Stream = &http3.RequestStream{}
)

// SessionState contains the state of a WebTransport session
type SessionState struct {
	// ConnectionState contains the QUIC connection state, including TLS handshake information
	ConnectionState quic.ConnectionState

	// ApplicationProtocol contains the application protocol negotiated for the session
	ApplicationProtocol string
}

type Session struct {
	sessionID           sessionID
	conn                *quic.Conn
	str                 http3Stream
	applicationProtocol string

	streamHdr    []byte
	uniStreamHdr []byte

	ctx      context.Context
	closeMx  sync.Mutex
	closeErr error // not nil once the session is closed
	// streamCtxs holds all the context.CancelFuncs of calls to Open{Uni}StreamSync calls currently active.
	// When the session is closed, this allows us to cancel all these contexts and make those calls return.
	streamCtxs map[int]context.CancelFunc

	bidiAcceptQueue acceptQueue[*Stream]
	uniAcceptQueue  acceptQueue[*ReceiveStream]

	streams streamsMap
}

func newSession(ctx context.Context, sessionID sessionID, conn *quic.Conn, str http3Stream, applicationProtocol string) *Session {
	ctx, ctxCancel := context.WithCancel(ctx)
	c := &Session{
		sessionID:           sessionID,
		conn:                conn,
		str:                 str,
		applicationProtocol: applicationProtocol,
		ctx:                 ctx,
		streamCtxs:          make(map[int]context.CancelFunc),
		bidiAcceptQueue:     *newAcceptQueue[*Stream](),
		uniAcceptQueue:      *newAcceptQueue[*ReceiveStream](),
		streams:             *newStreamsMap(),
	}
	// precompute the headers for unidirectional streams
	c.uniStreamHdr = make([]byte, 0, 2+quicvarint.Len(uint64(c.sessionID)))
	c.uniStreamHdr = quicvarint.Append(c.uniStreamHdr, webTransportUniStreamType)
	c.uniStreamHdr = quicvarint.Append(c.uniStreamHdr, uint64(c.sessionID))
	// precompute the headers for bidirectional streams
	c.streamHdr = make([]byte, 0, 2+quicvarint.Len(uint64(c.sessionID)))
	c.streamHdr = quicvarint.Append(c.streamHdr, webTransportFrameType)
	c.streamHdr = quicvarint.Append(c.streamHdr, uint64(c.sessionID))

	go func() {
		defer ctxCancel()
		c.handleConn()
	}()
	return c
}

func (s *Session) handleConn() {
	err := s.parseNextCapsule()
	s.closeWithError(err)
}

// parseNextCapsule parses the next Capsule sent on the request stream.
// It returns a SessionError, if the capsule received is a WT_CLOSE_SESSION Capsule.
func (s *Session) parseNextCapsule() error {
	for {
		typ, r, err := http3.ParseCapsule(quicvarint.NewReader(s.str))
		if err != nil {
			return err
		}
		switch typ {
		case closeSessionCapsuleType:
			var b [4]byte
			if _, err := io.ReadFull(r, b[:]); err != nil {
				return err
			}
			appErrCode := binary.BigEndian.Uint32(b[:])
			// the length of the error message is limited to 1024 bytes
			appErrMsg, err := io.ReadAll(io.LimitReader(r, maxCloseCapsuleErrorMsgLen))
			if err != nil {
				return err
			}
			return &SessionError{
				Remote:    true,
				ErrorCode: SessionErrorCode(appErrCode),
				Message:   string(appErrMsg),
			}
		default:
			// unknown capsule, skip it
			if _, err := io.ReadAll(r); err != nil {
				return err
			}
		}
	}
}

func (s *Session) addStream(qstr *quic.Stream, addStreamHeader bool) *Stream {
	var hdr []byte
	if addStreamHeader {
		hdr = s.streamHdr
	}
	str := newStream(qstr, hdr, func() { s.streams.RemoveStream(qstr.StreamID()) })
	s.streams.AddStream(qstr.StreamID(), str.closeWithSession)
	return str
}

func (s *Session) addReceiveStream(qstr *quic.ReceiveStream) *ReceiveStream {
	str := newReceiveStream(qstr, func() { s.streams.RemoveStream(qstr.StreamID()) })
	s.streams.AddStream(qstr.StreamID(), str.closeWithSession)
	return str
}

func (s *Session) addSendStream(qstr *quic.SendStream) *SendStream {
	str := newSendStream(qstr, s.uniStreamHdr, func() { s.streams.RemoveStream(qstr.StreamID()) })
	s.streams.AddStream(qstr.StreamID(), str.closeWithSession)
	return str
}

// addIncomingStream adds a bidirectional stream that the remote peer opened
func (s *Session) addIncomingStream(qstr *quic.Stream) {
	s.closeMx.Lock()
	closeErr := s.closeErr
	if closeErr != nil {
		s.closeMx.Unlock()
		qstr.CancelRead(WTSessionGoneErrorCode)
		qstr.CancelWrite(WTSessionGoneErrorCode)
		return
	}
	str := s.addStream(qstr, false)
	s.closeMx.Unlock()

	s.bidiAcceptQueue.Add(str)
}

// addIncomingUniStream adds a unidirectional stream that the remote peer opened
func (s *Session) addIncomingUniStream(qstr *quic.ReceiveStream) {
	s.closeMx.Lock()
	closeErr := s.closeErr
	if closeErr != nil {
		s.closeMx.Unlock()
		qstr.CancelRead(WTSessionGoneErrorCode)
		return
	}
	str := s.addReceiveStream(qstr)
	s.closeMx.Unlock()

	s.uniAcceptQueue.Add(str)
}

// Context returns a context that is closed when the session is closed.
func (s *Session) Context() context.Context {
	return s.ctx
}

func (s *Session) AcceptStream(ctx context.Context) (*Stream, error) {
	s.closeMx.Lock()
	closeErr := s.closeErr
	s.closeMx.Unlock()
	if closeErr != nil {
		return nil, closeErr
	}

	for {
		// If there's a stream in the accept queue, return it immediately.
		if str := s.bidiAcceptQueue.Next(); str != nil {
			return str, nil
		}
		// No stream in the accept queue. Wait until we accept one.
		select {
		case <-s.ctx.Done():
			return nil, s.closeErr
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.bidiAcceptQueue.Chan():
		}
	}
}

func (s *Session) AcceptUniStream(ctx context.Context) (*ReceiveStream, error) {
	s.closeMx.Lock()
	closeErr := s.closeErr
	s.closeMx.Unlock()
	if closeErr != nil {
		return nil, s.closeErr
	}

	for {
		// If there's a stream in the accept queue, return it immediately.
		if str := s.uniAcceptQueue.Next(); str != nil {
			return str, nil
		}
		// No stream in the accept queue. Wait until we accept one.
		select {
		case <-s.ctx.Done():
			return nil, s.closeErr
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.uniAcceptQueue.Chan():
		}
	}
}

func (s *Session) OpenStream() (*Stream, error) {
	s.closeMx.Lock()
	defer s.closeMx.Unlock()

	if s.closeErr != nil {
		return nil, s.closeErr
	}

	qstr, err := s.conn.OpenStream()
	if err != nil {
		return nil, err
	}
	return s.addStream(qstr, true), nil
}

func (s *Session) addStreamCtxCancel(cancel context.CancelFunc) (id int) {
rand:
	id = rand.Int()
	if _, ok := s.streamCtxs[id]; ok {
		goto rand
	}
	s.streamCtxs[id] = cancel
	return id
}

func (s *Session) OpenStreamSync(ctx context.Context) (*Stream, error) {
	s.closeMx.Lock()
	if s.closeErr != nil {
		s.closeMx.Unlock()
		return nil, s.closeErr
	}
	ctx, cancel := context.WithCancel(ctx)
	id := s.addStreamCtxCancel(cancel)
	s.closeMx.Unlock()

	// open a new bidirectional stream without holding the mutex: this call might block
	qstr, err := s.conn.OpenStreamSync(ctx)

	s.closeMx.Lock()
	defer s.closeMx.Unlock()
	delete(s.streamCtxs, id)

	// the session might have been closed concurrently with OpenStreamSync returning
	if qstr != nil && s.closeErr != nil {
		qstr.CancelRead(WTSessionGoneErrorCode)
		qstr.CancelWrite(WTSessionGoneErrorCode)
		return nil, s.closeErr
	}
	if err != nil {
		if s.closeErr != nil {
			return nil, s.closeErr
		}
		return nil, err
	}
	return s.addStream(qstr, true), nil
}

func (s *Session) OpenUniStream() (*SendStream, error) {
	s.closeMx.Lock()
	defer s.closeMx.Unlock()

	if s.closeErr != nil {
		return nil, s.closeErr
	}
	qstr, err := s.conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	return s.addSendStream(qstr), nil
}

func (s *Session) OpenUniStreamSync(ctx context.Context) (str *SendStream, err error) {
	s.closeMx.Lock()
	if s.closeErr != nil {
		s.closeMx.Unlock()
		return nil, s.closeErr
	}
	ctx, cancel := context.WithCancel(ctx)
	id := s.addStreamCtxCancel(cancel)
	s.closeMx.Unlock()

	// open a new unidirectional stream without holding the mutex: this call might block
	qstr, err := s.conn.OpenUniStreamSync(ctx)

	s.closeMx.Lock()
	defer s.closeMx.Unlock()
	delete(s.streamCtxs, id)

	// the session might have been closed concurrently with OpenStreamSync returning
	if qstr != nil && s.closeErr != nil {
		qstr.CancelWrite(WTSessionGoneErrorCode)
		return nil, s.closeErr
	}
	if err != nil {
		if s.closeErr != nil {
			return nil, s.closeErr
		}
		return nil, err
	}
	return s.addSendStream(qstr), nil
}

func (s *Session) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

func (s *Session) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

func (s *Session) CloseWithError(code SessionErrorCode, msg string) error {
	first, err := s.closeWithError(&SessionError{ErrorCode: code, Message: msg})
	if err != nil || !first {
		return err
	}

	// truncate the message if it's too long
	if len(msg) > maxCloseCapsuleErrorMsgLen {
		msg = truncateUTF8(msg, maxCloseCapsuleErrorMsgLen)
	}

	b := make([]byte, 4, 4+len(msg))
	binary.BigEndian.PutUint32(b, uint32(code))
	b = append(b, []byte(msg)...)

	// Optimistically send the WT_CLOSE_SESSION Capsule:
	// If we're flow-control limited, we don't want to wait for the receiver to issue new flow control credits.
	// There's no idiomatic way to do a non-blocking write in Go, so we set a short deadline.
	s.str.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	if err := http3.WriteCapsule(quicvarint.NewWriter(s.str), closeSessionCapsuleType, b); err != nil {
		s.str.CancelWrite(WTSessionGoneErrorCode)
	}

	s.str.CancelRead(WTSessionGoneErrorCode)
	err = s.str.Close()
	<-s.ctx.Done()
	return err
}

func (s *Session) SendDatagram(b []byte) error {
	return s.str.SendDatagram(b)
}

func (s *Session) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return s.str.ReceiveDatagram(ctx)
}

func (s *Session) closeWithError(closeErr error) (bool /* first call to close session */, error) {
	s.closeMx.Lock()
	defer s.closeMx.Unlock()
	// Duplicate call, or the remote already closed this session.
	if s.closeErr != nil {
		return false, nil
	}
	s.closeErr = closeErr

	for _, cancel := range s.streamCtxs {
		cancel()
	}
	s.streams.CloseSession(closeErr)

	return true, nil
}

// SessionState returns the current state of the session
func (s *Session) SessionState() SessionState {
	return SessionState{
		ConnectionState:     s.conn.ConnectionState(),
		ApplicationProtocol: s.applicationProtocol,
	}
}

// truncateUTF8 cuts a string to max n bytes without breaking UTF-8 characters.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}

	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
