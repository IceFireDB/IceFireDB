package filestore

import (
	"io"
	"os"

	"golang.org/x/exp/mmap"
)

type FileReader interface {
	io.ReaderAt
	io.Closer
}

var _ FileReader = (*stdReader)(nil)

type stdReader struct {
	f *os.File
}

// ReadAt implements the FileReader interface.
func (r *stdReader) ReadAt(p []byte, off int64) (n int, err error) {
	return r.f.ReadAt(p, off)
}

// Close implements the FileReader interface.
func (r *stdReader) Close() error {
	return r.f.Close()
}

func newStdReader(path string) (FileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &stdReader{f: f}, nil
}

var _ FileReader = (*mmapReader)(nil)

type mmapReader struct {
	m *mmap.ReaderAt
}

// ReadAt implements the FileReader interface.
func (r *mmapReader) ReadAt(p []byte, off int64) (n int, err error) {
	return r.m.ReadAt(p, off)
}

// Close implements the FileReader interface.
func (r *mmapReader) Close() error {
	return r.m.Close()
}

func newMmapReader(path string) (FileReader, error) {
	m, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}
	return &mmapReader{m: m}, nil
}
