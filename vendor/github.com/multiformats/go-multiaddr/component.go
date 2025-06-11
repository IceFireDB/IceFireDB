package multiaddr

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/multiformats/go-varint"
)

// Component is a single multiaddr Component.
type Component struct {
	// bytes is the raw bytes of the component. It includes the protocol code as
	// varint, possibly the size of the value, and the value.
	bytes         string // string for immutability.
	protocol      *Protocol
	valueStartIdx int // Index of the first byte of the Component's value in the bytes array
}

func (c *Component) Multiaddr() Multiaddr {
	if c == nil {
		return nil
	}
	return []Component{*c}
}

func (c *Component) Encapsulate(o Multiaddrer) Multiaddr {
	return c.Multiaddr().Encapsulate(o)
}

func (c *Component) Decapsulate(o Multiaddrer) Multiaddr {
	return c.Multiaddr().Decapsulate(o)
}

func (c *Component) Bytes() []byte {
	if c == nil {
		return nil
	}
	return []byte(c.bytes)
}

func (c *Component) MarshalBinary() ([]byte, error) {
	if c == nil {
		return nil, errNilPtr
	}
	return c.Bytes(), nil
}

func (c *Component) UnmarshalBinary(data []byte) error {
	if c == nil {
		return errNilPtr
	}
	_, comp, err := readComponent(data)
	if err != nil {
		return err
	}
	*c = *comp
	return nil
}

func (c *Component) MarshalText() ([]byte, error) {
	if c == nil {
		return nil, errNilPtr
	}
	return []byte(c.String()), nil
}

func (c *Component) UnmarshalText(data []byte) error {
	if c == nil {
		return errNilPtr
	}

	bytes, err := stringToBytes(string(data))
	if err != nil {
		return err
	}
	_, comp, err := readComponent(bytes)
	if err != nil {
		return err
	}
	*c = *comp
	return nil
}

func (c *Component) MarshalJSON() ([]byte, error) {
	if c == nil {
		return nil, errNilPtr
	}
	txt, err := c.MarshalText()
	if err != nil {
		return nil, err
	}

	return json.Marshal(string(txt))
}

func (c *Component) UnmarshalJSON(data []byte) error {
	if c == nil {
		return errNilPtr
	}

	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	return c.UnmarshalText([]byte(v))
}

func (c *Component) Equal(o *Component) bool {
	if c == nil || o == nil {
		return c == o
	}
	return c.bytes == o.bytes
}

func (c *Component) Compare(o *Component) int {
	if c == nil && o == nil {
		return 0
	}
	if c == nil {
		return -1
	}
	if o == nil {
		return 1
	}
	return strings.Compare(c.bytes, o.bytes)
}

func (c *Component) Protocols() []Protocol {
	if c == nil {
		return nil
	}
	if c.protocol == nil {
		return nil
	}
	return []Protocol{*c.protocol}
}

func (c *Component) ValueForProtocol(code int) (string, error) {
	if c == nil {
		return "", fmt.Errorf("component is nil")
	}
	if c.protocol == nil {
		return "", fmt.Errorf("component has nil protocol")
	}
	if c.protocol.Code != code {
		return "", ErrProtocolNotFound
	}
	return c.Value(), nil
}

func (c *Component) Protocol() Protocol {
	if c == nil {
		return Protocol{}
	}
	if c.protocol == nil {
		return Protocol{}
	}
	return *c.protocol
}

func (c *Component) RawValue() []byte {
	if c == nil {
		return nil
	}
	return []byte(c.bytes[c.valueStartIdx:])
}

func (c *Component) Value() string {
	if c == nil {
		return ""
	}
	// This Component MUST have been checked by validateComponent when created
	value, _ := c.valueAndErr()
	return value
}

func (c *Component) valueAndErr() (string, error) {
	if c == nil {
		return "", errNilPtr
	}
	if c.protocol == nil {
		return "", fmt.Errorf("component has nil protocol")
	}
	if c.protocol.Transcoder == nil {
		return "", nil
	}
	value, err := c.protocol.Transcoder.BytesToString([]byte(c.bytes[c.valueStartIdx:]))
	if err != nil {
		return "", err
	}
	return value, nil
}

func (c *Component) String() string {
	if c == nil {
		return "<nil component>"
	}
	var b strings.Builder
	c.writeTo(&b)
	return b.String()
}

// writeTo is an efficient, private function for string-formatting a multiaddr.
// Trust me, we tend to allocate a lot when doing this.
func (c *Component) writeTo(b *strings.Builder) {
	if c == nil {
		return
	}
	if c.protocol == nil {
		return
	}
	b.WriteByte('/')
	b.WriteString(c.protocol.Name)
	value := c.Value()
	if len(value) == 0 {
		return
	}
	if !(c.protocol.Path && value[0] == '/') {
		b.WriteByte('/')
	}
	b.WriteString(value)
}

// NewComponent constructs a new multiaddr component
func NewComponent(protocol, value string) (*Component, error) {
	p := ProtocolWithName(protocol)
	if p.Code == 0 {
		return nil, fmt.Errorf("unsupported protocol: %s", protocol)
	}
	if p.Transcoder != nil {
		bts, err := p.Transcoder.StringToBytes(value)
		if err != nil {
			return nil, err
		}
		return newComponent(p, bts)
	} else if value != "" {
		return nil, fmt.Errorf("protocol %s doesn't take a value", p.Name)
	}
	return newComponent(p, nil)
}

func newComponent(protocol Protocol, bvalue []byte) (*Component, error) {
	protocolPtr := protocolPtrByCode[protocol.Code]
	if protocolPtr == nil {
		protocolPtr = &protocol
	}

	size := len(bvalue)
	size += len(protocol.VCode)
	if protocol.Size < 0 {
		size += varint.UvarintSize(uint64(len(bvalue)))
	}
	maddr := make([]byte, size)
	var offset int
	offset += copy(maddr[offset:], protocol.VCode)
	if protocol.Size < 0 {
		offset += binary.PutUvarint(maddr[offset:], uint64(len(bvalue)))
	}
	copy(maddr[offset:], bvalue)

	// Shouldn't happen
	if len(maddr) != offset+len(bvalue) {
		return nil, fmt.Errorf("component size mismatch: %d != %d", len(maddr), offset+len(bvalue))
	}

	c := &Component{
		bytes:         string(maddr),
		protocol:      protocolPtr,
		valueStartIdx: offset,
	}

	err := validateComponent(c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// validateComponent MUST be called after creating a non-zero Component.
// It ensures that we will be able to call all methods on Component without
// error.
func validateComponent(c *Component) error {
	if c == nil {
		return errNilPtr
	}
	if c.protocol == nil {
		return fmt.Errorf("component is missing its protocol")
	}
	if c.valueStartIdx > len(c.bytes) {
		return fmt.Errorf("component valueStartIdx is greater than the length of the component's bytes")
	}

	if len(c.protocol.VCode) == 0 {
		return fmt.Errorf("Component is missing its protocol's VCode field")
	}
	if len(c.bytes) < len(c.protocol.VCode) {
		return fmt.Errorf("component size mismatch: %d != %d", len(c.bytes), len(c.protocol.VCode))
	}
	if !bytes.Equal([]byte(c.bytes[:len(c.protocol.VCode)]), c.protocol.VCode) {
		return fmt.Errorf("component's VCode field is invalid: %v != %v", []byte(c.bytes[:len(c.protocol.VCode)]), c.protocol.VCode)
	}
	if c.protocol.Size < 0 {
		size, n, err := ReadVarintCode([]byte(c.bytes[len(c.protocol.VCode):]))
		if err != nil {
			return err
		}
		if size != len(c.bytes[c.valueStartIdx:]) {
			return fmt.Errorf("component value size mismatch: %d != %d", size, len(c.bytes[c.valueStartIdx:]))
		}

		if len(c.protocol.VCode)+n+size != len(c.bytes) {
			return fmt.Errorf("component size mismatch: %d != %d", len(c.protocol.VCode)+n+size, len(c.bytes))
		}
	} else {
		// Fixed size value
		size := c.protocol.Size / 8
		if size != len(c.bytes[c.valueStartIdx:]) {
			return fmt.Errorf("component value size mismatch: %d != %d", size, len(c.bytes[c.valueStartIdx:]))
		}

		if len(c.protocol.VCode)+size != len(c.bytes) {
			return fmt.Errorf("component size mismatch: %d != %d", len(c.protocol.VCode)+size, len(c.bytes))
		}
	}

	_, err := c.valueAndErr()
	if err != nil {
		return err

	}
	if c.protocol.Transcoder != nil {
		err = c.protocol.Transcoder.ValidateBytes([]byte(c.bytes[c.valueStartIdx:]))
		if err != nil {
			return err
		}
	}
	return nil
}
