package ipldgit

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/ipld/go-ipld-prime"
)

// DecodeBlob fills a NodeAssembler (from `Type.Blob__Repr.NewBuilder()`) from a stream of bytes
func DecodeBlob(na ipld.NodeAssembler, rd *bufio.Reader) error {
	sizen, err := readNullTerminatedNumber(rd)
	if err != nil {
		return err
	}
	if sizen < 0 {
		return fmt.Errorf("invalid blob size: %d", sizen)
	}

	prefix := fmt.Sprintf("blob %d\x00", sizen)

	// The header's size is unverified until the body arrives, so grow the
	// buffer as it is read rather than reserving the declared size up front.
	var buf bytes.Buffer
	buf.WriteString(prefix)

	n, err := io.Copy(&buf, io.LimitReader(rd, int64(sizen)))
	if err != nil {
		return err
	}

	// Match io.ReadFull: EOF if the body was entirely absent, ErrUnexpectedEOF
	// if it was short.
	if n != int64(sizen) {
		if n == 0 {
			return io.EOF
		}
		return io.ErrUnexpectedEOF
	}

	return na.AssignBytes(buf.Bytes())
}

func encodeBlob(n ipld.Node, w io.Writer) error {
	b, err := n.AsBytes()
	if err != nil {
		return err
	}

	_, err = w.Write(b)
	return err
}
