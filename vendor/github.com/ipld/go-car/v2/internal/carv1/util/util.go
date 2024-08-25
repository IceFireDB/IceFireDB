package util

import (
	"errors"
	"io"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/multiformats/go-varint"

	cid "github.com/ipfs/go-cid"
)

var ErrSectionTooLarge = errors.New("invalid section data, length of read beyond allowable maximum")
var ErrHeaderTooLarge = errors.New("invalid header data, length of read beyond allowable maximum")

type BytesReader interface {
	io.Reader
	io.ByteReader
}

func ReadNode(r io.Reader, zeroLenAsEOF bool, maxReadBytes uint64) (cid.Cid, []byte, error) {
	data, err := LdRead(r, zeroLenAsEOF, maxReadBytes)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	n, c, err := cid.CidFromBytes(data)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	return c, data[n:], nil
}

func LdWrite(w io.Writer, d ...[]byte) error {
	var sum uint64
	for _, s := range d {
		sum += uint64(len(s))
	}

	buf := make([]byte, 8)
	n := varint.PutUvarint(buf, sum)
	_, err := w.Write(buf[:n])
	if err != nil {
		return err
	}

	for _, s := range d {
		_, err = w.Write(s)
		if err != nil {
			return err
		}
	}

	return nil
}

func LdSize(d ...[]byte) uint64 {
	var sum uint64
	for _, s := range d {
		sum += uint64(len(s))
	}
	s := varint.UvarintSize(sum)
	return sum + uint64(s)
}

func LdReadSize(r io.Reader, zeroLenAsEOF bool, maxReadBytes uint64) (uint64, error) {
	l, err := varint.ReadUvarint(internalio.ToByteReader(r))
	if err != nil {
		// If the length of bytes read is non-zero when the error is EOF then signal an unclean EOF.
		if l > 0 && err == io.EOF {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	} else if l == 0 && zeroLenAsEOF {
		return 0, io.EOF
	}

	if l > maxReadBytes { // Don't OOM
		return 0, ErrSectionTooLarge
	}
	return l, nil
}

func LdRead(r io.Reader, zeroLenAsEOF bool, maxReadBytes uint64) ([]byte, error) {
	l, err := LdReadSize(r, zeroLenAsEOF, maxReadBytes)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}
