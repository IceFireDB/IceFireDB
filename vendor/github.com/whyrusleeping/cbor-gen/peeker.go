package typegen

import (
	"bufio"
	"io"
)

// BytePeeker combines the Reader and ByteScanner interfaces.
type BytePeeker interface {
	io.Reader
	io.ByteScanner
}

func GetPeeker(r io.Reader) BytePeeker {
	if r, ok := r.(BytePeeker); ok {
		return r
	}
	return &peeker{reader: r}
}

// peeker is a non-buffering BytePeeker.
type peeker struct {
	reader    io.Reader
	peekState int
	lastByte  [1]byte
}

const (
	peekEmpty = iota
	peekSet
	peekUnread
)

func (p *peeker) Read(buf []byte) (n int, err error) {
	// Read "nothing". I.e., read an error, maybe.
	if len(buf) == 0 {
		// There's something pending in the
		if p.peekState == peekUnread {
			return 0, nil
		}
		return p.reader.Read(nil)
	}

	if p.peekState == peekUnread {
		buf[0] = p.lastByte[0]
		n, err = p.reader.Read(buf[1:])
		n += 1
	} else {
		n, err = p.reader.Read(buf)
	}
	if n > 0 {
		p.peekState = peekSet
		p.lastByte[0] = buf[n-1]
	}
	return n, err
}

func (p *peeker) ReadByte() (byte, error) {
	if p.peekState == peekUnread {
		p.peekState = peekSet
		return p.lastByte[0], nil
	}
	_, err := io.ReadFull(p.reader, p.lastByte[:])
	if err != nil {
		return 0, err
	}
	b := p.lastByte[0]
	p.peekState = peekSet
	return b, nil
}

func (p *peeker) UnreadByte() error {
	if p.peekState != peekSet {
		return bufio.ErrInvalidUnreadByte
	}
	p.peekState = peekUnread
	return nil
}
