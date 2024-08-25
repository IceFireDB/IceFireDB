package files

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
)

// MultiFileReader reads from a `commands.Node` (which can be a directory of files
// or a regular file) as HTTP multipart encoded data.
type MultiFileReader struct {
	io.Reader

	// directory stack for NextFile
	files []DirIterator
	path  []string

	currentFile Node
	buf         bytes.Buffer
	mpWriter    *multipart.Writer
	closed      bool
	mutex       *sync.Mutex

	// if true, the content disposition will be "form-data"
	// if false, the content disposition will be "attachment"
	form bool

	// if true, 'abspath' header will be sent with raw (potentially binary) file
	// name. This must only be used for legacy purposes to talk with old servers.
	// if false, 'abspath-encoded' header will be sent with %-encoded filename
	rawAbsPath bool
}

// NewMultiFileReader constructs a MultiFileReader. `file` can be any `commands.Directory`.
// If `form` is set to true, the Content-Disposition will be "form-data".
// Otherwise, it will be "attachment". If `rawAbsPath` is set to true, the
// "abspath" header will be sent. Otherwise, the "abspath-encoded" header will be sent.
func NewMultiFileReader(file Directory, form, rawAbsPath bool) *MultiFileReader {
	it := file.Entries()

	mfr := &MultiFileReader{
		files:      []DirIterator{it},
		path:       []string{""},
		form:       form,
		rawAbsPath: rawAbsPath,
		mutex:      &sync.Mutex{},
	}
	mfr.mpWriter = multipart.NewWriter(&mfr.buf)

	return mfr
}

func (mfr *MultiFileReader) Read(buf []byte) (written int, err error) {
	mfr.mutex.Lock()
	defer mfr.mutex.Unlock()

	// if we are closed and the buffer is flushed, end reading
	if mfr.closed && mfr.buf.Len() == 0 {
		return 0, io.EOF
	}

	// if the current file isn't set, advance to the next file
	if mfr.currentFile == nil {
		var entry DirEntry

		for entry == nil {
			if len(mfr.files) == 0 {
				mfr.mpWriter.Close()
				mfr.closed = true
				return mfr.buf.Read(buf)
			}

			if !mfr.files[len(mfr.files)-1].Next() {
				if mfr.files[len(mfr.files)-1].Err() != nil {
					return 0, mfr.files[len(mfr.files)-1].Err()
				}
				mfr.files = mfr.files[:len(mfr.files)-1]
				mfr.path = mfr.path[:len(mfr.path)-1]
				continue
			}

			entry = mfr.files[len(mfr.files)-1]
		}

		// handle starting a new file part
		if !mfr.closed {
			mfr.currentFile = entry.Node()

			// write the boundary and headers
			header := make(textproto.MIMEHeader)
			filename := path.Join(path.Join(mfr.path...), entry.Name())
			mfr.addContentDisposition(header, filename)

			var contentType string

			switch f := entry.Node().(type) {
			case *Symlink:
				contentType = "application/symlink"
			case Directory:
				newIt := f.Entries()
				mfr.files = append(mfr.files, newIt)
				mfr.path = append(mfr.path, entry.Name())
				contentType = "application/x-directory"
			case File:
				// otherwise, use the file as a reader to read its contents
				contentType = "application/octet-stream"
			default:
				return 0, ErrNotSupported
			}

			header.Set(contentTypeHeader, contentType)
			if rf, ok := entry.Node().(FileInfo); ok {
				if mfr.rawAbsPath {
					// Legacy compatibility with old servers.
					header.Set("abspath", rf.AbsPath())
				} else {
					header.Set("abspath-encoded", url.QueryEscape(rf.AbsPath()))
				}
			}

			_, err := mfr.mpWriter.CreatePart(header)
			if err != nil {
				return 0, err
			}
		}
	}

	// if the buffer has something in it, read from it
	if mfr.buf.Len() > 0 {
		return mfr.buf.Read(buf)
	}

	// otherwise, read from file data
	if f, ok := mfr.currentFile.(File); ok {
		written, err = f.Read(buf)
		if err != io.EOF {
			return written, err
		}
	}

	if err := mfr.currentFile.Close(); err != nil {
		return written, err
	}

	mfr.currentFile = nil
	return written, nil
}

func (mfr *MultiFileReader) addContentDisposition(header textproto.MIMEHeader, filename string) {
	sb := &strings.Builder{}
	params := url.Values{}

	if mode := mfr.currentFile.Mode(); mode != 0 {
		params.Add("mode", "0"+strconv.FormatUint(uint64(mode), 8))
	}
	if mtime := mfr.currentFile.ModTime(); !mtime.IsZero() {
		params.Add("mtime", strconv.FormatInt(mtime.Unix(), 10))
		if n := mtime.Nanosecond(); n > 0 {
			params.Add("mtime-nsecs", strconv.FormatInt(int64(n), 10))
		}
	}

	sb.Grow(120)
	if mfr.form {
		sb.WriteString("form-data; name=\"file")
		if len(params) > 0 {
			fmt.Fprintf(sb, "?%s", params.Encode())
		}
		sb.WriteString("\"")
	} else {
		sb.WriteString("attachment")
	}

	fmt.Fprintf(sb, "; filename=\"%s\"", url.QueryEscape(filename))

	header.Set(contentDispositionHeader, sb.String())
}

// Boundary returns the boundary string to be used to separate files in the multipart data
func (mfr *MultiFileReader) Boundary() string {
	return mfr.mpWriter.Boundary()
}
