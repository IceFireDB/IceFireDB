package sds

import (
	"bufio"
	"encoding/binary"
	"io"
	"math"
)

// Writer for write stuff
type Writer struct {
	err error
	bw  *bufio.Writer
}

// NewWriter returns a new Writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

// Flush the buffered bytes to the underlying writer.
func (w *Writer) Flush() error {
	if w.err == nil {
		w.err = w.bw.Flush()
	}
	return w.err
}

// Error returns any previous error.
func (w *Writer) Error() error {
	return w.err
}

// SetError allows for setting a custom user error, which will then be returned
// from all further Writer.Set*() calls.
// This operation is ignored if an error has already been assigned to Writer.
func (w *Writer) SetError(err error) {
	if w.err == nil {
		w.err = err
	}
}

// WriteUvarint writes a uvarint
func (w *Writer) WriteUvarint(x uint64) error {
	if w.err == nil {
		var buf [10]byte
		n := binary.PutUvarint(buf[:], x)
		_, w.err = w.bw.Write(buf[:n])
	}
	return w.err
}

// WriteVarint writes a varint
func (w *Writer) WriteVarint(x int64) error {
	if w.err == nil {
		var buf [10]byte
		n := binary.PutVarint(buf[:], x)
		_, w.err = w.bw.Write(buf[:n])
	}
	return w.err
}

// WriteUint32 writes a uint32
func (w *Writer) WriteUint32(x uint32) error {
	if w.err == nil {
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], x)
		_, w.err = w.bw.Write(buf[:])
	}
	return w.err
}

// WriteInt32 writes an int32
func (w *Writer) WriteInt32(x int32) error {
	return w.WriteUint32(uint32(x))
}

// WriteUint16 writes a uint16
func (w *Writer) WriteUint16(x uint16) error {
	if w.err == nil {
		var buf [2]byte
		binary.LittleEndian.PutUint16(buf[:], x)
		_, w.err = w.bw.Write(buf[:])
	}
	return w.err
}

// WriteInt16 writes an int16
func (w *Writer) WriteInt16(x int16) error {
	return w.WriteUint16(uint16(x))
}

// WriteUint8 writes a uint8
func (w *Writer) WriteUint8(x uint8) error {
	if w.err == nil {
		w.err = w.bw.WriteByte(x)
	}
	return w.err
}

// WriteInt8 writes an int8
func (w *Writer) WriteInt8(x int8) error {
	return w.WriteUint8(uint8(x))
}

// WriteByte writes a byte
func (w *Writer) WriteByte(x byte) error {
	return w.WriteUint8(uint8(x))
}

// WriteUint64 writes a uint64
func (w *Writer) WriteUint64(x uint64) error {
	if w.err == nil {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], x)
		_, w.err = w.bw.Write(buf[:])
	}
	return w.err
}

// WriteInt64 writes an int64
func (w *Writer) WriteInt64(x int64) error {
	return w.WriteUint64(uint64(x))
}

// WriteFloat64 writes a float64
func (w *Writer) WriteFloat64(f float64) error {
	return w.WriteUint64(math.Float64bits(f))
}

// WriteFloat32 writes a float32
func (w *Writer) WriteFloat32(f float32) error {
	return w.WriteUint32(math.Float32bits(f))
}

// WriteBool writes a bool
func (w *Writer) WriteBool(t bool) error {
	if w.err == nil {
		if t {
			w.err = w.bw.WriteByte(1)
		} else {
			w.err = w.bw.WriteByte(0)
		}
	}
	return w.err
}

// WriteString writes a string
func (w *Writer) WriteString(s string) error {
	if err := w.WriteUvarint(uint64(len(s))); err != nil {
		return err
	}
	if w.err == nil {
		_, w.err = w.bw.WriteString(s)
	}
	return w.err
}

// WriteBytes writes bytes
func (w *Writer) WriteBytes(b []byte) error {
	if err := w.WriteUvarint(uint64(len(b))); err != nil {
		return err
	}
	if w.err == nil {
		_, w.err = w.bw.Write(b)
	}
	return w.err
}

// Reader for reading snapbits
type Reader struct {
	err error
	br  *bufio.Reader
}

// NewReader returns a new Reader
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

// Error returns any previous error.
func (r *Reader) Error() error {
	return r.err
}

// SetError allows for setting a custom user error, which will then be returned
// from all further Reader.Set*() calls.
// This operation is ignored if an error has already been assigned to Reader.
func (r *Reader) SetError(err error) {
	if r.err == nil {
		r.err = err
	}
}

// ReadUvarint reads a uvarint
func (r *Reader) ReadUvarint() (uint64, error) {
	var x uint64
	if r.err == nil {
		x, r.err = binary.ReadUvarint(r.br)
	}
	return x, r.err
}

// ReadVarint reads a varint
func (r *Reader) ReadVarint() (int64, error) {
	var x int64
	if r.err == nil {
		x, r.err = binary.ReadVarint(r.br)
	}
	return x, r.err
}

// ReadUint64 reads a uint64
func (r *Reader) ReadUint64() (uint64, error) {
	var x uint64
	if r.err == nil {
		var buf [8]byte
		if _, r.err = io.ReadFull(r.br, buf[:]); r.err == nil {
			x = binary.LittleEndian.Uint64(buf[:])
		}
	}
	return x, r.err
}

// ReadInt64 reads an int64
func (r *Reader) ReadInt64() (int64, error) {
	x, err := r.ReadUint64()
	return int64(x), err
}

// ReadUint32 reads a uint32
func (r *Reader) ReadUint32() (uint32, error) {
	var x uint32
	if r.err == nil {
		var buf [4]byte
		if _, r.err = io.ReadFull(r.br, buf[:]); r.err == nil {
			x = binary.LittleEndian.Uint32(buf[:])
		}
	}
	return x, r.err
}

// ReadInt32 reads an int32
func (r *Reader) ReadInt32() (int32, error) {
	x, err := r.ReadUint32()
	return int32(x), err
}

// ReadUint16 reads a uint16
func (r *Reader) ReadUint16() (uint16, error) {
	var x uint16
	if r.err == nil {
		var buf [2]byte
		if _, r.err = io.ReadFull(r.br, buf[:]); r.err == nil {
			x = binary.LittleEndian.Uint16(buf[:])
		}
	}
	return x, r.err
}

// ReadInt16 reads an int16
func (r *Reader) ReadInt16() (int16, error) {
	x, err := r.ReadUint16()
	return int16(x), err
}

// ReadByte reads a byte
func (r *Reader) ReadByte() (byte, error) {
	var b byte
	if r.err == nil {
		b, r.err = r.br.ReadByte()
	}
	return b, r.err
}

// ReadUint8 reads a uint8
func (r *Reader) ReadUint8() (uint8, error) {
	x, err := r.ReadByte()
	return x, err
}

// ReadInt8 reads an int8
func (r *Reader) ReadInt8() (int8, error) {
	x, err := r.ReadUint8()
	return int8(x), err
}

// ReadFloat64 reads a float64
func (r *Reader) ReadFloat64() (float64, error) {
	x, err := r.ReadUint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(x), nil
}

// ReadFloat32 reads a float32
func (r *Reader) ReadFloat32() (float32, error) {
	x, err := r.ReadUint32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(x), nil
}

// ReadBool reads a bool
func (r *Reader) ReadBool() (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	return b != 0, err
}

// ReadBytes reads bytes
func (r *Reader) ReadBytes() ([]byte, error) {
	var n uint64
	n, r.err = r.ReadUvarint()
	if r.err == nil {
		b := make([]byte, n)
		_, r.err = io.ReadFull(r.br, b)
		if r.err == nil {
			return b, nil
		}
	}
	return nil, r.err
}

// ReadString reads a string
func (r *Reader) ReadString() (string, error) {
	var n uint64
	n, r.err = r.ReadUvarint()
	if r.err == nil {
		b := make([]byte, n)
		_, r.err = io.ReadFull(r.br, b)
		if r.err == nil {
			return string(b), nil
		}
	}
	return "", r.err
}
