package cmds

import (
	"errors"
	"fmt"
	"io"
)

var (
	ErrClosedEmitter        = errors.New("cmds: emit on closed emitter")
	ErrClosingClosedEmitter = errors.New("cmds: closing closed emitter")
)

// Single can be used to signal to any ResponseEmitter that only one value will be emitted.
// This is important e.g. for the http.ResponseEmitter so it can set the HTTP headers appropriately.
type Single struct {
	Value any
}

func (s Single) String() string {
	return fmt.Sprintf("Single{%v}", s.Value)
}

func (s Single) GoString() string {
	return fmt.Sprintf("Single{%#v}", s.Value)
}

// EmitOnce is a helper that emits a value wrapped in Single, to signal that this will be the only value sent.
func EmitOnce(re ResponseEmitter, v any) error {
	return re.Emit(Single{v})
}

// ResponseEmitter encodes and sends the command code's output to the client.
// It is all a command can write to.
type ResponseEmitter interface {
	// Close closes the underlying transport.
	Close() error

	// CloseWithError closes the underlying transport and makes subsequent read
	// calls return the passed error.
	CloseWithError(error) error

	// SetLength sets the length of the output.
	// Must be called before Emit.
	SetLength(length uint64)

	// SetEncodingType overrides the encoding type for this response.
	// This only affects the Content-Type header in HTTP responses.
	//
	// Use this when the actual content encoding differs from what was
	// requested. For example, when returning gzip-compressed data,
	// call SetEncodingType(Gzip) so the Content-Type header is set to
	// "application/x-gzip" instead of the default.
	//
	// Must be called before Emit. Calling it after Emit has no effect
	// because HTTP headers are already sent.
	//
	// This is a no-op for non-HTTP emitters (CLI, chan, writer).
	SetEncodingType(encType EncodingType)

	// SetContentType overrides the Content-Type header for HTTP responses.
	// Use this with OctetStream encoding to specify a custom MIME type
	// for binary data.
	//
	// Must be called before Emit. Calling it after Emit has no effect
	// because HTTP headers are already sent.
	//
	// This is a no-op for non-HTTP emitters (CLI, chan, writer).
	SetContentType(contentType string)

	// Emit sends a value.
	// If value is io.Reader we just copy that to the connection
	// other values are marshalled.
	Emit(value any) error
}

// Copy sends all values received on res to re. If res is closed, it closes re.
func Copy(re ResponseEmitter, res Response) error {
	re.SetLength(res.Length())

	for {
		v, err := res.Next()
		if err != nil {
			if err == io.EOF {
				return re.Close()
			}

			return re.CloseWithError(err)
		}

		err = re.Emit(v)
		if err != nil {
			return err
		}
	}
}

// EmitChan reads a value from a channel and send that value to re.
func EmitChan(re ResponseEmitter, ch <-chan any) error {
	for v := range ch {
		err := re.Emit(v)
		if err != nil {
			return err
		}
	}

	return nil
}
