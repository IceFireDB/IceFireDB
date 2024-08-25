package typegen

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

var deferredBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(nil)
	},
}

type Deferred struct {
	Raw []byte
}

func (d *Deferred) MarshalCBOR(w io.Writer) error {
	if d == nil {
		_, err := w.Write(CborNull)
		return err
	}
	if d.Raw == nil {
		return errors.New("cannot marshal Deferred with nil value for Raw (will not unmarshal)")
	}
	_, err := w.Write(d.Raw)
	return err
}

func (d *Deferred) UnmarshalCBOR(br io.Reader) (err error) {
	buf := deferredBufferPool.Get().(*bytes.Buffer)

	defer func() {
		buf.Reset()
		deferredBufferPool.Put(buf)
	}()

	// Allocate some scratch space.
	scratch := make([]byte, maxHeaderSize)

	hasReadOnce := false
	defer func() {
		if err == io.EOF && hasReadOnce {
			err = io.ErrUnexpectedEOF
		}
	}()

	// Algorithm:
	//
	// 1. We start off expecting to read one element.
	// 2. If we see a tag, we expect to read one more element so we increment "remaining".
	// 3. If see an array, we expect to read "extra" elements so we add "extra" to "remaining".
	// 4. If see a map, we expect to read "2*extra" elements so we add "2*extra" to "remaining".
	// 5. While "remaining" is non-zero, read more elements.

	// define this once so we don't keep allocating it.
	limitedReader := io.LimitedReader{R: br}
	for remaining := uint64(1); remaining > 0; remaining-- {
		maj, extra, err := CborReadHeaderBuf(br, scratch)
		if err != nil {
			return err
		}
		hasReadOnce = true
		if err := WriteMajorTypeHeaderBuf(scratch, buf, maj, extra); err != nil {
			return err
		}

		switch maj {
		case MajUnsignedInt, MajNegativeInt, MajOther:
			// nothing fancy to do
		case MajByteString, MajTextString:
			if extra > ByteArrayMaxLen {
				return maxLengthError
			}
			// Copy the bytes
			limitedReader.N = int64(extra)
			buf.Grow(int(extra))
			if n, err := buf.ReadFrom(&limitedReader); err != nil {
				return err
			} else if n < int64(extra) {
				return io.ErrUnexpectedEOF
			}
		case MajTag:
			remaining++
		case MajArray:
			if extra > MaxLength {
				return maxLengthError
			}
			remaining += extra
		case MajMap:
			if extra > MaxLength {
				return maxLengthError
			}
			remaining += extra * 2
		default:
			return fmt.Errorf("unhandled deferred cbor type: %d", maj)
		}
	}
	// Reuse existing buffer. Also, copy to "give back" the allocation in the byte buffer (which
	// is likely significant).
	d.Raw = d.Raw[:0]
	d.Raw = append(d.Raw, buf.Bytes()...)
	return nil
}
