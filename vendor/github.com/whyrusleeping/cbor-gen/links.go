package typegen

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"

	cid "github.com/ipfs/go-cid"
)

func ScanForLinks(br io.Reader, cb func(cid.Cid)) (err error) {
	hasReadOnce := false
	defer func() {
		if err == io.EOF && hasReadOnce {
			err = io.ErrUnexpectedEOF
		}
	}()

	scratch := make([]byte, maxCidLength)
	for remaining := uint64(1); remaining > 0; remaining-- {
		maj, extra, err := CborReadHeaderBuf(br, scratch)
		if err != nil {
			return err
		}
		hasReadOnce = true

		switch maj {
		case MajUnsignedInt, MajNegativeInt, MajOther:
		case MajByteString, MajTextString:
			if extra > math.MaxInt32 {
				return fmt.Errorf("string in cbor input too long")
			}

			err := discard(br, int(extra))
			if err != nil {
				return err
			}
		case MajTag:
			if extra == 42 {
				maj, extra, err = CborReadHeaderBuf(br, scratch)
				if err != nil {
					return err
				}

				if maj != MajByteString {
					return fmt.Errorf("expected cbor type 'byte string' in input")
				}

				if extra > maxCidLength {
					return fmt.Errorf("string in cbor input too long")
				}

				if extra == 0 {
					return fmt.Errorf("string in cbor input is empty")
				}

				if _, err := io.ReadAtLeast(br, scratch[:extra], int(extra)); err != nil {
					return err
				}

				c, err := cid.Cast(scratch[1:extra])
				if err != nil {
					return err
				}
				cb(c)

			} else {
				remaining++
			}
		case MajArray:
			remaining += extra
		case MajMap:
			remaining += (extra * 2)
		default:
			return fmt.Errorf("unhandled cbor type: %d", maj)
		}
	}
	return nil
}

// discard is a helper function to discard data from a reader, special-casing
// the most common readers we encounter in this library for a significant
// performance boost.
func discard(br io.Reader, n int) error {
	// If we're expecting no bytes, don't even try to read. Otherwise, we may read an EOF.
	if n == 0 {
		return nil
	}

	switch r := br.(type) {
	case *bytes.Buffer:
		buf := r.Next(n)
		if len(buf) == 0 {
			return io.EOF
		} else if len(buf) < n {
			return io.ErrUnexpectedEOF
		}
		return nil
	case *bytes.Reader:
		if r.Len() == 0 {
			return io.EOF
		} else if r.Len() < n {
			_, _ = r.Seek(0, io.SeekEnd)
			return io.ErrUnexpectedEOF
		}
		_, err := r.Seek(int64(n), io.SeekCurrent)
		return err
	case *bufio.Reader:
		discarded, err := r.Discard(n)
		if discarded != 0 && discarded < n && err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	default:
		discarded, err := io.CopyN(io.Discard, br, int64(n))
		if discarded != 0 && discarded < int64(n) && err == io.EOF {
			return io.ErrUnexpectedEOF
		}

		return err
	}
}
