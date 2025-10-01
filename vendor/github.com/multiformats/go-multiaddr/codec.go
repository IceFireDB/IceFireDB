package multiaddr

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/multiformats/go-varint"
)

func stringToBytes(s string) ([]byte, error) {
	// consume trailing slashes
	s = strings.TrimRight(s, "/")

	var b bytes.Buffer
	sp := strings.Split(s, "/")

	if sp[0] != "" {
		return nil, fmt.Errorf("failed to parse multiaddr %q: must begin with /", s)
	}

	// consume first empty elem
	sp = sp[1:]

	if len(sp) == 0 {
		return nil, fmt.Errorf("failed to parse multiaddr %q: empty multiaddr", s)
	}

	for len(sp) > 0 {
		name := sp[0]
		p := ProtocolWithName(name)
		if p.Code == 0 {
			return nil, fmt.Errorf("failed to parse multiaddr %q: unknown protocol %s", s, sp[0])
		}
		_, _ = b.Write(p.VCode)
		sp = sp[1:]

		if p.Size == 0 { // no length.
			continue
		}

		if len(sp) < 1 {
			return nil, fmt.Errorf("failed to parse multiaddr %q: unexpected end of multiaddr", s)
		}

		if p.Path {
			// it's a path protocol (terminal).
			// consume the rest of the address as the next component.
			sp = []string{"/" + strings.Join(sp, "/")}
		}

		a, err := p.Transcoder.StringToBytes(sp[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse multiaddr %q: invalid value %q for protocol %s: %s", s, sp[0], p.Name, err)
		}
		err = p.Transcoder.ValidateBytes(a)
		if err != nil {
			return nil, fmt.Errorf("failed to validate multiaddr %q: invalid value %q for protocol %s: %w", s, sp[0], p.Name, err)
		}
		if p.Size < 0 { // varint size.
			_, _ = b.Write(varint.ToUvarint(uint64(len(a))))
		}
		b.Write(a)
		sp = sp[1:]
	}

	return b.Bytes(), nil
}

func readComponent(b []byte) (int, *Component, error) {
	var offset int
	code, n, err := ReadVarintCode(b)
	if err != nil {
		return 0, nil, err
	}
	offset += n

	p := ProtocolWithCode(code)
	if p.Code == 0 {
		return 0, nil, fmt.Errorf("no protocol with code %d", code)
	}
	pPtr := protocolPtrByCode[code]
	if pPtr == nil {
		return 0, nil, fmt.Errorf("no protocol with code %d", code)
	}

	if p.Size == 0 {
		c := &Component{
			bytes:         string(b[:offset]),
			valueStartIdx: offset,
			protocol:      pPtr,
		}

		err := validateComponent(c)
		if err != nil {
			return 0, nil, err
		}

		return offset, c, nil
	}

	var size int
	if p.Size < 0 {
		// varint
		var n int
		size, n, err = ReadVarintCode(b[offset:])
		if err != nil {
			return 0, nil, err
		}
		offset += n
	} else {
		// Size is in bits, but we operate on bytes
		size = p.Size / 8
	}

	if len(b[offset:]) < size || size <= 0 {
		return 0, nil, fmt.Errorf("invalid value for size %d", len(b[offset:]))
	}

	c := &Component{
		bytes:         string(b[:offset+size]),
		protocol:      pPtr,
		valueStartIdx: offset,
	}
	err = validateComponent(c)
	if err != nil {
		return 0, nil, err
	}

	return offset + size, c, err
}

func readMultiaddr(b []byte) (int, Multiaddr, error) {
	if len(b) == 0 {
		return 0, nil, fmt.Errorf("empty multiaddr")
	}

	var res Multiaddr
	bytesRead := 0
	sawPathComponent := false
	for len(b) > 0 {
		n, c, err := readComponent(b)
		if err != nil {
			return 0, nil, err
		}
		b = b[n:]
		bytesRead += n

		if sawPathComponent {
			// It is an error to have another component after a path component.
			return bytesRead, nil, fmt.Errorf("unexpected component after path component")
		}
		sawPathComponent = c.protocol.Path
		res = append(res, *c)
	}
	return bytesRead, res, nil
}
