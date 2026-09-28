package webtransport

import (
	"encoding/binary"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

const (
	closeSessionCapsuleType       http3.CapsuleType = 0x2843
	maxDataCapsuleType            http3.CapsuleType = 0x190b4d3d
	maxStreamDataCapsuleType      http3.CapsuleType = 0x190b4d3e // only used on WebTransport over HTTP/2
	maxStreamsBidiCapsuleType     http3.CapsuleType = 0x190b4d3f
	maxStreamsUniCapsuleType      http3.CapsuleType = 0x190b4d40
	dataBlockedCapsuleType        http3.CapsuleType = 0x190b4d41
	streamDataBlockedCapsuleType  http3.CapsuleType = 0x190b4d42 // only used on WebTransport over HTTP/2
	streamsBlockedBidiCapsuleType http3.CapsuleType = 0x190b4d43
	streamsBlockedUniCapsuleType  http3.CapsuleType = 0x190b4d44
)

const maxCloseCapsuleErrorMsgLen = 1024

const maxStreamsLimit = 1 << 60

type capsule interface {
	Append([]byte) []byte
}

// parseNextCapsule parses Capsules sent on the request stream.
// It returns the next known Capsule, skipping unknown Capsules.
func parseNextCapsule(parser *http3.CapsuleParser) (capsule, error) {
	for {
		typ, capsuleReader, err := parser.Next()
		if err != nil {
			return nil, err
		}
		switch typ {
		case closeSessionCapsuleType:
			capsule, err := parseCloseSessionCapsule(capsuleReader)
			if err != nil {
				return nil, err
			}
			return capsule, nil
		case maxDataCapsuleType:
			maxData, err := parseMaxDataCapsule(capsuleReader)
			if err != nil {
				return nil, err
			}
			return maxDataCapsule{MaximumData: maxData}, nil
		case maxStreamDataCapsuleType:
			return nil, errors.New("WT_MAX_STREAM_DATA capsule received")
		case maxStreamsBidiCapsuleType:
			maxStreams, err := parseMaxStreamsCapsule(capsuleReader)
			if err != nil {
				return nil, err
			}
			return maxStreamsBidiCapsule{MaximumStreams: maxStreams}, nil
		case maxStreamsUniCapsuleType:
			maxStreams, err := parseMaxStreamsCapsule(capsuleReader)
			if err != nil {
				return nil, err
			}
			return maxStreamsUniCapsule{MaximumStreams: maxStreams}, nil
		case dataBlockedCapsuleType:
			maxData, err := parseDataBlockedCapsule(capsuleReader)
			if err != nil {
				return nil, err
			}
			return dataBlockedCapsule{MaximumData: maxData}, nil
		case streamDataBlockedCapsuleType:
			return nil, errors.New("WT_STREAM_DATA_BLOCKED capsule received")
		case streamsBlockedBidiCapsuleType:
			maxStreams, err := parseStreamsBlockedCapsule(capsuleReader)
			if err != nil {
				return nil, err
			}
			return streamsBlockedBidiCapsule{MaximumStreams: maxStreams}, nil
		case streamsBlockedUniCapsuleType:
			maxStreams, err := parseStreamsBlockedCapsule(capsuleReader)
			if err != nil {
				return nil, err
			}
			return streamsBlockedUniCapsule{MaximumStreams: maxStreams}, nil
		default:
			// unknown capsule, skip it
			if err := capsuleReader.Discard(); err != nil {
				return nil, err
			}
		}
	}
}

type closeSessionCapsule struct {
	ErrorCode SessionErrorCode
	Message   string
}

func parseCloseSessionCapsule(r http3.CapsuleReader) (closeSessionCapsule, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return closeSessionCapsule{}, err
	}
	msg, err := io.ReadAll(io.LimitReader(r, maxCloseCapsuleErrorMsgLen))
	if err != nil {
		return closeSessionCapsule{}, err
	}
	if err := r.Discard(); err != nil {
		return closeSessionCapsule{}, err
	}
	return closeSessionCapsule{
		ErrorCode: SessionErrorCode(binary.BigEndian.Uint32(b[:])),
		Message:   string(msg),
	}, nil
}

func (c closeSessionCapsule) Append(b []byte) []byte {
	msg := c.Message
	if len(msg) > maxCloseCapsuleErrorMsgLen {
		msg = truncateUTF8(msg, maxCloseCapsuleErrorMsgLen)
	}

	b = quicvarint.Append(b, uint64(closeSessionCapsuleType))
	b = quicvarint.Append(b, uint64(4+len(msg)))
	payloadStart := len(b)
	b = append(b, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[payloadStart:], uint32(c.ErrorCode))
	return append(b, msg...)
}

func (c closeSessionCapsule) ToSessionError() *SessionError {
	return &SessionError{
		Remote:    true,
		ErrorCode: c.ErrorCode,
		Message:   c.Message,
	}
}

type maxDataCapsule struct {
	MaximumData uint64
}

func (c maxDataCapsule) Append(b []byte) []byte {
	b = quicvarint.Append(b, uint64(maxDataCapsuleType))
	b = quicvarint.Append(b, uint64(quicvarint.Len(c.MaximumData)))
	return quicvarint.Append(b, c.MaximumData)
}

type maxStreamsBidiCapsule struct {
	MaximumStreams uint64
}

func (c maxStreamsBidiCapsule) Append(b []byte) []byte {
	b = quicvarint.Append(b, uint64(maxStreamsBidiCapsuleType))
	b = quicvarint.Append(b, uint64(quicvarint.Len(c.MaximumStreams)))
	return quicvarint.Append(b, c.MaximumStreams)
}

type maxStreamsUniCapsule struct {
	MaximumStreams uint64
}

func (c maxStreamsUniCapsule) Append(b []byte) []byte {
	b = quicvarint.Append(b, uint64(maxStreamsUniCapsuleType))
	b = quicvarint.Append(b, uint64(quicvarint.Len(c.MaximumStreams)))
	return quicvarint.Append(b, c.MaximumStreams)
}

type dataBlockedCapsule struct {
	MaximumData uint64
}

func (c dataBlockedCapsule) Append(b []byte) []byte {
	b = quicvarint.Append(b, uint64(dataBlockedCapsuleType))
	b = quicvarint.Append(b, uint64(quicvarint.Len(c.MaximumData)))
	return quicvarint.Append(b, c.MaximumData)
}

type streamsBlockedBidiCapsule struct {
	MaximumStreams uint64
}

func (c streamsBlockedBidiCapsule) Append(b []byte) []byte {
	b = quicvarint.Append(b, uint64(streamsBlockedBidiCapsuleType))
	b = quicvarint.Append(b, uint64(quicvarint.Len(c.MaximumStreams)))
	return quicvarint.Append(b, c.MaximumStreams)
}

type streamsBlockedUniCapsule struct {
	MaximumStreams uint64
}

func (c streamsBlockedUniCapsule) Append(b []byte) []byte {
	b = quicvarint.Append(b, uint64(streamsBlockedUniCapsuleType))
	b = quicvarint.Append(b, uint64(quicvarint.Len(c.MaximumStreams)))
	return quicvarint.Append(b, c.MaximumStreams)
}

func parseDataBlockedCapsule(r http3.CapsuleReader) (uint64, error) {
	maxData, err := quicvarint.Read(r)
	if err != nil {
		return 0, err
	}
	if r.Remaining() != 0 {
		return 0, errors.New("WT_DATA_BLOCKED capsule has trailing data")
	}
	return maxData, nil
}

func parseMaxDataCapsule(r http3.CapsuleReader) (uint64, error) {
	maxData, err := quicvarint.Read(r)
	if err != nil {
		return 0, err
	}
	if r.Remaining() != 0 {
		return 0, errors.New("WT_MAX_DATA capsule has trailing data")
	}
	return maxData, nil
}

func parseMaxStreamsCapsule(r http3.CapsuleReader) (uint64, error) {
	maxStreams, err := quicvarint.Read(r)
	if err != nil {
		return 0, err
	}
	if r.Remaining() != 0 {
		return 0, errors.New("WT_MAX_STREAMS capsule has trailing data")
	}
	if maxStreams > maxStreamsLimit {
		return 0, errors.New("WT_MAX_STREAMS value too large")
	}
	return maxStreams, nil
}

func parseStreamsBlockedCapsule(r http3.CapsuleReader) (uint64, error) {
	maxStreams, err := quicvarint.Read(r)
	if err != nil {
		return 0, err
	}
	if r.Remaining() != 0 {
		return 0, errors.New("WT_STREAMS_BLOCKED capsule has trailing data")
	}
	if maxStreams > maxStreamsLimit {
		return 0, errors.New("WT_STREAMS_BLOCKED value too large")
	}
	return maxStreams, nil
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
