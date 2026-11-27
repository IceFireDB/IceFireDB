package multihash

import (
	"errors"
	"io"
	"math"

	"github.com/multiformats/go-varint"
)

// Reader is an io.Reader wrapper that exposes a function
// to read a whole multihash, parse it, and return it.
type Reader interface {
	io.Reader

	ReadMultihash() (Multihash, error)
}

// Writer is an io.Writer wrapper that exposes a function
// to write a whole multihash.
type Writer interface {
	io.Writer

	WriteMultihash(Multihash) error
}

// NewReader wraps an io.Reader with a multihash.Reader
func NewReader(r io.Reader) Reader {
	return &mhReader{r}
}

// NewWriter wraps an io.Writer with a multihash.Writer
func NewWriter(w io.Writer) Writer {
	return &mhWriter{w}
}

type mhReader struct {
	r io.Reader
}

func (r *mhReader) Read(buf []byte) (n int, err error) {
	return r.r.Read(buf)
}

func (r *mhReader) ReadByte() (byte, error) {
	if br, ok := r.r.(io.ByteReader); ok {
		return br.ReadByte()
	}
	var b [1]byte
	n, err := r.r.Read(b[:])
	if n == 1 {
		return b[0], nil
	}
	if err == nil {
		if n != 0 {
			panic("reader returned an invalid length")
		}
		err = io.ErrNoProgress
	}
	return 0, err
}

func (r *mhReader) ReadMultihash() (Multihash, error) {
	code, err := varint.ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	length, err := varint.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if length > math.MaxInt32 {
		return nil, errors.New("digest too long, supporting only <= 2^31-1")
	}

	// length is untrusted: grow buf as digest bytes arrive, so allocation
	// tracks input received rather than the claimed length.
	prefixLen := varint.UvarintSize(code) + varint.UvarintSize(length)
	total := prefixLen + int(length)
	buf := make([]byte, prefixLen, min(total, prefixLen+digestPrealloc))
	n := varint.PutUvarint(buf, code)
	varint.PutUvarint(buf[n:], length)

	for len(buf) < total {
		if len(buf) == cap(buf) {
			buf = append(make([]byte, 0, min(2*cap(buf), total)), buf...)
		}
		m, err := io.ReadFull(r.r, buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+m]
		if err != nil {
			// As a single io.ReadFull would: EOF only if no digest bytes arrived.
			if err == io.EOF && len(buf) > prefixLen {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}

	return Cast(buf)
}

// digestPrealloc is the digest capacity allocated before any digest bytes
// arrive. Typical digests fit within it.
const digestPrealloc = 64 << 10

type mhWriter struct {
	w io.Writer
}

func (w *mhWriter) Write(buf []byte) (n int, err error) {
	return w.w.Write(buf)
}

func (w *mhWriter) WriteMultihash(m Multihash) error {
	_, err := w.w.Write([]byte(m))
	return err
}
