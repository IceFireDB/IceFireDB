package io

import (
	"errors"
	"io"
)

var (
	_ io.ReaderAt   = (*offsetReadSeeker)(nil)
	_ io.ReadSeeker = (*offsetReadSeeker)(nil)
)

// offsetReadSeeker implements Read, and ReadAt on a section
// of an underlying io.ReaderAt.
// The main difference between io.SectionReader and offsetReadSeeker is that
// NewOffsetReadSeeker does not require the user to know the number of readable bytes.
//
// It also partially implements Seek, where the implementation panics if io.SeekEnd is passed.
// This is because, offsetReadSeeker does not know the end of the file therefore cannot seek relative
// to it.
type offsetReadSeeker struct {
	r    io.ReaderAt
	base int64
	off  int64
	b    [1]byte // avoid alloc in ReadByte
}

type ReadSeekerAt interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.ByteReader
}

// NewOffsetReadSeeker returns an ReadSeekerAt that reads from r
// starting offset offset off and stops with io.EOF when r reaches its end.
// The Seek function will panic if whence io.SeekEnd is passed.
func NewOffsetReadSeeker(r io.ReaderAt, off int64) (ReadSeekerAt, error) {
	if or, ok := r.(*offsetReadSeeker); ok {
		oldBase := or.base
		newBase := or.base + off
		if newBase < oldBase {
			return nil, errors.New("NewOffsetReadSeeker overflow int64")
		}
		return &offsetReadSeeker{
			r:    or.r,
			base: newBase,
			off:  newBase,
		}, nil
	}
	return &offsetReadSeeker{
		r:    r,
		base: off,
		off:  off,
	}, nil
}

func (o *offsetReadSeeker) Read(p []byte) (n int, err error) {
	n, err = o.r.ReadAt(p, o.off)
	oldOffset := o.off
	off := oldOffset + int64(n)
	if off < oldOffset {
		return 0, errors.New("ReadAt offset overflow")
	}
	o.off = off
	return
}

func (o *offsetReadSeeker) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, io.EOF
	}
	oldOffset := off
	off += o.base
	if off < oldOffset {
		return 0, errors.New("ReadAt offset overflow")
	}
	return o.r.ReadAt(p, off)
}

func (o *offsetReadSeeker) ReadByte() (byte, error) {
	_, err := o.Read(o.b[:])
	return o.b[0], err
}

func (o *offsetReadSeeker) Offset() int64 {
	return o.off
}

func (o *offsetReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		oldOffset := offset
		off := offset + o.base
		if off < oldOffset {
			return 0, errors.New("Seek offset overflow")
		}
		o.off = off
	case io.SeekCurrent:
		oldOffset := o.off
		if offset < 0 {
			if -offset > oldOffset {
				return 0, errors.New("Seek offset underflow")
			}
			o.off = oldOffset + offset
		} else {
			off := oldOffset + offset
			if off < oldOffset {
				return 0, errors.New("Seek offset overflow")
			}
			o.off = off
		}
	case io.SeekEnd:
		panic("unsupported whence: SeekEnd")
	}
	return o.Position(), nil
}

// Position returns the current position of this reader relative to the initial offset.
func (o *offsetReadSeeker) Position() int64 {
	return o.off - o.base
}
