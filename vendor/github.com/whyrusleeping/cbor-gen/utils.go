package typegen

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	cid "github.com/ipfs/go-cid"
)

const (
	maxCidLength  = 100
	maxHeaderSize = 9
)

const (
	MajUnsignedInt = 0
	MajNegativeInt = 1
	MajByteString  = 2
	MajTextString  = 3
	MajArray       = 4
	MajMap         = 5
	MajTag         = 6
	MajOther       = 7
)

var maxLengthError = fmt.Errorf("length beyond maximum allowed")

type CBORUnmarshaler interface {
	UnmarshalCBOR(io.Reader) error
}

type CBORMarshaler interface {
	MarshalCBOR(io.Writer) error
}

func readByte(r io.Reader) (byte, error) {
	// try to cast to a concrete type, it's much faster than casting to an
	// interface.
	switch r := r.(type) {
	case *bytes.Buffer:
		return r.ReadByte()
	case *bytes.Reader:
		return r.ReadByte()
	case *bufio.Reader:
		return r.ReadByte()
	case *peeker:
		return r.ReadByte()
	case *CborReader:
		return readByte(r.r)
	case io.ByteReader:
		return r.ReadByte()
	}
	var buf [1]byte
	_, err := io.ReadFull(r, buf[:1])
	return buf[0], err
}

func CborReadHeader(br io.Reader) (byte, uint64, error) {
	if cr, ok := br.(*CborReader); ok {
		return cr.ReadHeader()
	}

	first, err := readByte(br)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	maj := (first & 0xe0) >> 5
	low := first & 0x1f

	switch {
	case low < 24:
		return maj, uint64(low), nil
	case low == 24:
		next, err := readByte(br)
		if err != nil {
			return 0, 0, err
		}
		if next < 24 {
			return 0, 0, fmt.Errorf("cbor input was not canonical (lval 24 with value < 24)")
		}
		return maj, uint64(next), nil
	case low == 25:
		scratch := make([]byte, 2)
		if _, err := io.ReadAtLeast(br, scratch[:2], 2); err != nil {
			return 0, 0, err
		}
		val := uint64(binary.BigEndian.Uint16(scratch[:2]))
		if val <= math.MaxUint8 {
			return 0, 0, fmt.Errorf("cbor input was not canonical (lval 25 with value <= MaxUint8)")
		}
		return maj, val, nil
	case low == 26:
		scratch := make([]byte, 4)
		if _, err := io.ReadAtLeast(br, scratch[:4], 4); err != nil {
			return 0, 0, err
		}
		val := uint64(binary.BigEndian.Uint32(scratch[:4]))
		if val <= math.MaxUint16 {
			return 0, 0, fmt.Errorf("cbor input was not canonical (lval 26 with value <= MaxUint16)")
		}
		return maj, val, nil
	case low == 27:
		scratch := make([]byte, 8)
		if _, err := io.ReadAtLeast(br, scratch, 8); err != nil {
			return 0, 0, err
		}
		val := binary.BigEndian.Uint64(scratch)
		if val <= math.MaxUint32 {
			return 0, 0, fmt.Errorf("cbor input was not canonical (lval 27 with value <= MaxUint32)")
		}
		return maj, val, nil
	default:
		return 0, 0, fmt.Errorf("invalid header: (%x)", first)
	}
}

func readByteBuf(r io.Reader, scratch []byte) (byte, error) {
	// Reading a single byte from these buffers is much faster than copying
	// into a slice.
	switch r := r.(type) {
	case *bytes.Buffer:
		return r.ReadByte()
	case *bytes.Reader:
		return r.ReadByte()
	case *bufio.Reader:
		return r.ReadByte()
	case *peeker:
		return r.ReadByte()
	case *CborReader:
		return readByte(r.r)
	case io.ByteReader:
		return r.ReadByte()
	}
	_, err := io.ReadFull(r, scratch[:1])
	return scratch[0], err
}

// same as the above, just tries to allocate less by using a passed in scratch buffer
func CborReadHeaderBuf(br io.Reader, scratch []byte) (byte, uint64, error) {
	first, err := readByteBuf(br, scratch)
	if err != nil {
		return 0, 0, err
	}

	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	maj := (first & 0xe0) >> 5
	low := first & 0x1f

	switch {
	case low < 24:
		return maj, uint64(low), nil
	case low == 24:
		next, err := readByteBuf(br, scratch)
		if err != nil {
			return 0, 0, err
		}
		if next < 24 {
			return 0, 0, fmt.Errorf("cbor input was not canonical (lval 24 with value < 24)")
		}
		return maj, uint64(next), nil
	case low == 25:
		if _, err := io.ReadAtLeast(br, scratch[:2], 2); err != nil {
			return 0, 0, err
		}
		val := uint64(binary.BigEndian.Uint16(scratch[:2]))
		if val <= math.MaxUint8 {
			return 0, 0, fmt.Errorf("cbor input was not canonical (lval 25 with value <= MaxUint8)")
		}
		return maj, val, nil
	case low == 26:
		if _, err := io.ReadAtLeast(br, scratch[:4], 4); err != nil {
			return 0, 0, err
		}
		val := uint64(binary.BigEndian.Uint32(scratch[:4]))
		if val <= math.MaxUint16 {
			return 0, 0, fmt.Errorf("cbor input was not canonical (lval 26 with value <= MaxUint16)")
		}
		return maj, val, nil
	case low == 27:
		if _, err := io.ReadAtLeast(br, scratch[:8], 8); err != nil {
			return 0, 0, err
		}
		val := binary.BigEndian.Uint64(scratch[:8])
		if val <= math.MaxUint32 {
			return 0, 0, fmt.Errorf("cbor input was not canonical (lval 27 with value <= MaxUint32)")
		}
		return maj, val, nil
	default:
		return 0, 0, fmt.Errorf("invalid header: (%x)", first)
	}
}

func CborWriteHeader(w io.Writer, t byte, l uint64) error {
	return WriteMajorTypeHeader(w, t, l)
}

// TODO: No matter what I do, this function *still* allocates. Its super frustrating.
// See issue: https://github.com/golang/go/issues/33160
func WriteMajorTypeHeader(w io.Writer, t byte, l uint64) error {
	if w, ok := w.(*CborWriter); ok {
		return w.WriteMajorTypeHeader(t, l)
	}

	switch {
	case l < 24:
		_, err := w.Write([]byte{(t << 5) | byte(l)})
		return err
	case l < (1 << 8):
		_, err := w.Write([]byte{(t << 5) | 24, byte(l)})
		return err
	case l < (1 << 16):
		var b [3]byte
		b[0] = (t << 5) | 25
		binary.BigEndian.PutUint16(b[1:3], uint16(l))
		_, err := w.Write(b[:])
		return err
	case l < (1 << 32):
		var b [5]byte
		b[0] = (t << 5) | 26
		binary.BigEndian.PutUint32(b[1:5], uint32(l))
		_, err := w.Write(b[:])
		return err
	default:
		var b [9]byte
		b[0] = (t << 5) | 27
		binary.BigEndian.PutUint64(b[1:], uint64(l))
		_, err := w.Write(b[:])
		return err
	}
}

// Same as the above, but uses a passed in buffer to avoid allocations
func WriteMajorTypeHeaderBuf(buf []byte, w io.Writer, t byte, l uint64) error {
	switch {
	case l < 24:
		buf[0] = (t << 5) | byte(l)
		_, err := w.Write(buf[:1])
		return err
	case l < (1 << 8):
		buf[0] = (t << 5) | 24
		buf[1] = byte(l)
		_, err := w.Write(buf[:2])
		return err
	case l < (1 << 16):
		buf[0] = (t << 5) | 25
		binary.BigEndian.PutUint16(buf[1:3], uint16(l))
		_, err := w.Write(buf[:3])
		return err
	case l < (1 << 32):
		buf[0] = (t << 5) | 26
		binary.BigEndian.PutUint32(buf[1:5], uint32(l))
		_, err := w.Write(buf[:5])
		return err
	default:
		buf[0] = (t << 5) | 27
		binary.BigEndian.PutUint64(buf[1:9], uint64(l))
		_, err := w.Write(buf[:9])
		return err
	}
}

func CborEncodeMajorType(t byte, l uint64) []byte {
	switch {
	case l < 24:
		var b [1]byte
		b[0] = (t << 5) | byte(l)
		return b[:1]
	case l < (1 << 8):
		var b [2]byte
		b[0] = (t << 5) | 24
		b[1] = byte(l)
		return b[:2]
	case l < (1 << 16):
		var b [3]byte
		b[0] = (t << 5) | 25
		binary.BigEndian.PutUint16(b[1:3], uint16(l))
		return b[:3]
	case l < (1 << 32):
		var b [5]byte
		b[0] = (t << 5) | 26
		binary.BigEndian.PutUint32(b[1:5], uint32(l))
		return b[:5]
	default:
		var b [9]byte
		b[0] = (t << 5) | 27
		binary.BigEndian.PutUint64(b[1:], uint64(l))
		return b[:]
	}
}

func ReadTaggedByteArray(br io.Reader, exptag uint64, maxlen uint64) (bs []byte, err error) {
	maj, extra, err := CborReadHeader(br)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != MajTag {
		return nil, fmt.Errorf("expected cbor type 'tag' in input")
	}

	if extra != exptag {
		return nil, fmt.Errorf("expected tag %d", exptag)
	}

	return ReadByteArray(br, maxlen)
}

func ReadByteArray(br io.Reader, maxlen uint64) ([]byte, error) {
	maj, extra, err := CborReadHeader(br)
	if err != nil {
		return nil, err
	}

	if maj != MajByteString {
		return nil, fmt.Errorf("expected cbor type 'byte string' in input")
	}

	if extra > maxlen {
		return nil, fmt.Errorf("string in cbor input too long, maxlen: %d", maxlen)
	}

	buf := make([]byte, extra)
	if _, err := io.ReadAtLeast(br, buf, int(extra)); err != nil {
		return nil, err
	}

	return buf, nil
}

// WriteByteArray encodes a byte array as a cbor byte-string.
func WriteByteArray(bw io.Writer, bytes []byte) error {
	writer := NewCborWriter(bw)
	if err := writer.WriteMajorTypeHeader(MajByteString, uint64(len(bytes))); err != nil {
		return err
	}
	if _, err := writer.Write(bytes); err != nil {
		return err
	}
	return nil
}

var stringBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, MaxLength)
		return &b
	},
}

func ReadString(r io.Reader) (string, error) {
	return ReadStringWithMax(r, MaxLength)
}

func ReadStringWithMax(r io.Reader, maxLength uint64) (string, error) {
	maj, l, err := CborReadHeader(r)
	if err != nil {
		return "", err
	}

	if maj != MajTextString {
		return "", fmt.Errorf("got tag %d while reading string value (l = %d)", maj, l)
	}

	if l > maxLength {
		return "", fmt.Errorf("string in input was too long")
	}

	bufp := stringBufPool.Get().(*[]byte)
	if cap(*bufp) < int(l) {
		*bufp = append((*bufp)[:cap(*bufp)], make([]byte, int(l)-cap(*bufp))...)
	}
	buf := (*bufp)[:l] // shares same backing array as pooled slice
	defer func() {
		// optimizes to memclr
		for i := range buf {
			buf[i] = 0
		}
		stringBufPool.Put(bufp)
	}()
	_, err = io.ReadAtLeast(r, buf, int(l))
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

// Deprecated: use ReadString
func ReadStringBuf(r io.Reader, _ []byte) (string, error) {
	return ReadString(r)
}

func ReadCid(br io.Reader) (cid.Cid, error) {
	buf, err := ReadTaggedByteArray(br, 42, 512)
	if err != nil {
		return cid.Undef, err
	}

	return bufToCid(buf)
}

func bufToCid(buf []byte) (cid.Cid, error) {
	if len(buf) == 0 {
		return cid.Undef, fmt.Errorf("undefined cid")
	}

	if len(buf) < 2 {
		return cid.Undef, fmt.Errorf("cbor serialized CIDs must have at least two bytes")
	}

	if buf[0] != 0 {
		return cid.Undef, fmt.Errorf("cbor serialized CIDs must have binary multibase")
	}

	return cid.Cast(buf[1:])
}

var byteArrZero = []byte{0}

func WriteCid(w io.Writer, c cid.Cid) error {
	cw := NewCborWriter(w)
	if err := cw.WriteMajorTypeHeader(MajTag, 42); err != nil {
		return err
	}
	if c == cid.Undef {
		return fmt.Errorf("undefined cid")
		// return CborWriteHeader(w, MajByteString, 0)
	}

	if err := cw.WriteMajorTypeHeader(MajByteString, uint64(c.ByteLen()+1)); err != nil {
		return err
	}

	// that binary multibase prefix...
	if _, err := cw.Write(byteArrZero); err != nil {
		return err
	}

	if _, err := cw.WriteString(c.KeyString()); err != nil {
		return err
	}

	return nil
}

func WriteCidBuf(buf []byte, w io.Writer, c cid.Cid) error {
	if err := WriteMajorTypeHeaderBuf(buf, w, MajTag, 42); err != nil {
		return err
	}
	if c == cid.Undef {
		return fmt.Errorf("undefined cid")
		// return CborWriteHeader(w, MajByteString, 0)
	}

	if err := WriteMajorTypeHeaderBuf(buf, w, MajByteString, uint64(c.ByteLen()+1)); err != nil {
		return err
	}

	// that binary multibase prefix...
	if _, err := w.Write(byteArrZero); err != nil {
		return err
	}

	if _, err := c.WriteBytes(w); err != nil {
		return err
	}

	return nil
}
