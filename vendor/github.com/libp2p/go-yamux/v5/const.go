package yamux

import (
	"encoding/binary"
	"fmt"
	"time"
)

type Error struct {
	msg                string
	timeout, temporary bool
}

func (ye *Error) Error() string {
	return ye.msg
}

func (ye *Error) Timeout() bool {
	return ye.timeout
}

func (ye *Error) Temporary() bool {
	return ye.temporary
}

type GoAwayError struct {
	ErrorCode uint32
	Remote    bool
}

func (e *GoAwayError) Error() string {
	if e.Remote {
		return fmt.Sprintf("remote sent go away, code: %d", e.ErrorCode)
	}
	return fmt.Sprintf("sent go away, code: %d", e.ErrorCode)
}

func (e *GoAwayError) Timeout() bool {
	return false
}

func (e *GoAwayError) Temporary() bool {
	return false
}

func (e *GoAwayError) Is(target error) bool {
	// to maintain compatibility with errors returned by previous versions
	if e.Remote && target == ErrRemoteGoAway {
		return true
	} else if !e.Remote && target == ErrSessionShutdown {
		return true
	} else if target == ErrStreamReset {
		// A GoAway on a connection also resets all the streams.
		return true
	}

	if err, ok := target.(*GoAwayError); ok {
		return *e == *err
	}
	return false
}

// A StreamError is used for errors returned from Read and Write calls after the stream is Reset
type StreamError struct {
	ErrorCode uint32
	Remote    bool
}

func (s *StreamError) Error() string {
	if s.Remote {
		return fmt.Sprintf("stream reset by remote, error code: %d", s.ErrorCode)
	}
	return fmt.Sprintf("stream reset, error code: %d", s.ErrorCode)
}

func (s *StreamError) Is(target error) bool {
	if target == ErrStreamReset {
		return true
	}
	e, ok := target.(*StreamError)
	return ok && *e == *s
}

var (
	// ErrInvalidVersion means we received a frame with an
	// invalid version
	ErrInvalidVersion = &Error{msg: "invalid protocol version"}

	// ErrInvalidMsgType means we received a frame with an
	// invalid message type
	ErrInvalidMsgType = &Error{msg: "invalid msg type"}

	// ErrSessionShutdown is used if there is a shutdown during
	// an operation
	ErrSessionShutdown = &GoAwayError{ErrorCode: goAwayNormal, Remote: false}

	// ErrStreamsExhausted is returned if we have no more
	// stream ids to issue
	ErrStreamsExhausted = &Error{msg: "streams exhausted"}

	// ErrDuplicateStream is used if a duplicate stream is
	// opened inbound
	ErrDuplicateStream = &Error{msg: "duplicate stream initiated"}

	// ErrReceiveWindowExceeded indicates the window was exceeded
	ErrRecvWindowExceeded = &Error{msg: "recv window exceeded"}

	// ErrTimeout is used when we reach an IO deadline
	ErrTimeout = &Error{msg: "i/o deadline reached", timeout: true, temporary: true}

	// ErrStreamClosed is returned when using a closed stream
	ErrStreamClosed = &Error{msg: "stream closed"}

	// ErrUnexpectedFlag is set when we get an unexpected flag
	ErrUnexpectedFlag = &Error{msg: "unexpected flag"}

	// ErrRemoteGoAway is used when we get a go away from the other side with error code
	// goAwayNormal(0).
	ErrRemoteGoAway = &GoAwayError{Remote: true, ErrorCode: goAwayNormal}

	// ErrStreamReset is sent if a stream is reset. This can happen
	// if the backlog is exceeded, or if there was a remote GoAway.
	ErrStreamReset = &Error{msg: "stream reset"}

	// ErrConnectionWriteTimeout indicates that we hit the "safety valve"
	// timeout writing to the underlying stream connection.
	ErrConnectionWriteTimeout = &Error{msg: "connection write timeout", timeout: true}

	// ErrKeepAliveTimeout is sent if a missed keepalive caused the stream close
	ErrKeepAliveTimeout = &Error{msg: "keepalive timeout", timeout: true}
)

const (
	// protoVersion is the only version we support
	protoVersion uint8 = 0
)

const (
	// Data is used for data frames. They are followed
	// by length bytes worth of payload.
	typeData uint8 = iota

	// WindowUpdate is used to change the window of
	// a given stream. The length indicates the delta
	// update to the window.
	typeWindowUpdate

	// Ping is sent as a keep-alive or to measure
	// the RTT. The StreamID and Length value are echoed
	// back in the response.
	typePing

	// GoAway is sent to terminate a session. The StreamID
	// should be 0 and the length is an error code.
	typeGoAway
)

const (
	// SYN is sent to signal a new stream. May
	// be sent with a data payload
	flagSYN uint16 = 1 << iota

	// ACK is sent to acknowledge a new stream. May
	// be sent with a data payload
	flagACK

	// FIN is sent to half-close the given stream.
	// May be sent with a data payload.
	flagFIN

	// RST is used to hard close a given stream.
	flagRST
)

const (
	// initialStreamWindow is the initial stream window size.
	// It's not an implementation choice, the value defined in the specification.
	initialStreamWindow = 256 * 1024
	maxStreamWindow     = 16 * 1024 * 1024
	goAwayWaitTime      = 100 * time.Millisecond
)

const (
	// goAwayNormal is sent on a normal termination
	goAwayNormal uint32 = iota

	// goAwayProtoErr sent on a protocol error
	goAwayProtoErr

	// goAwayInternalErr sent on an internal error
	goAwayInternalErr
)

const (
	sizeOfVersion  = 1
	sizeOfType     = 1
	sizeOfFlags    = 2
	sizeOfStreamID = 4
	sizeOfLength   = 4
	headerSize     = sizeOfVersion + sizeOfType + sizeOfFlags +
		sizeOfStreamID + sizeOfLength
)

type header [headerSize]byte

func (h header) Version() uint8 {
	return h[0]
}

func (h header) MsgType() uint8 {
	return h[1]
}

func (h header) Flags() uint16 {
	return binary.BigEndian.Uint16(h[2:4])
}

func (h header) StreamID() uint32 {
	return binary.BigEndian.Uint32(h[4:8])
}

func (h header) Length() uint32 {
	return binary.BigEndian.Uint32(h[8:12])
}

func (h header) String() string {
	return fmt.Sprintf("Vsn:%d Type:%d Flags:%d StreamID:%d Length:%d",
		h.Version(), h.MsgType(), h.Flags(), h.StreamID(), h.Length())
}

func encode(msgType uint8, flags uint16, streamID uint32, length uint32) header {
	var h header
	h[0] = protoVersion
	h[1] = msgType
	binary.BigEndian.PutUint16(h[2:4], flags)
	binary.BigEndian.PutUint32(h[4:8], streamID)
	binary.BigEndian.PutUint32(h[8:12], length)
	return h
}
