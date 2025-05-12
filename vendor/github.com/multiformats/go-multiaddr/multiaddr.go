package multiaddr

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"golang.org/x/exp/slices"
)

var errNilPtr = errors.New("nil ptr")

// Multiaddr is the data structure representing a Multiaddr
type Multiaddr []Component

func (m Multiaddr) copy() Multiaddr {
	if m == nil {
		return nil
	}
	out := make(Multiaddr, len(m))
	copy(out, m)
	return out
}

// NewMultiaddr parses and validates an input string, returning a *Multiaddr
func NewMultiaddr(s string) (a Multiaddr, err error) {
	defer func() {
		if e := recover(); e != nil {
			log.Printf("Panic in NewMultiaddr on input %q: %s", s, e)
			err = fmt.Errorf("%v", e)
		}
	}()
	b, err := stringToBytes(s)
	if err != nil {
		return nil, err
	}
	return NewMultiaddrBytes(b)
}

// NewMultiaddrBytes initializes a Multiaddr from a byte representation.
// It validates it as an input string.
func NewMultiaddrBytes(b []byte) (a Multiaddr, err error) {
	defer func() {
		if e := recover(); e != nil {
			log.Printf("Panic in NewMultiaddrBytes on input %q: %s", b, e)
			err = fmt.Errorf("%v", e)
		}
	}()
	bytesRead, m, err := readMultiaddr(b)
	if err != nil {
		return nil, err
	}
	if bytesRead != len(b) {
		return nil, fmt.Errorf("unexpected extra data. %v bytes leftover", len(b)-bytesRead)
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

// Equal tests whether two multiaddrs are equal
func (m Multiaddr) Equal(m2 Multiaddr) bool {
	if len(m) != len(m2) {
		return false
	}
	for i, c := range m {
		if !c.Equal(&m2[i]) {
			return false
		}
	}
	return true
}

func (m Multiaddr) Compare(o Multiaddr) int {
	for i := 0; i < len(m) && i < len(o); i++ {
		if cmp := m[i].Compare(&o[i]); cmp != 0 {
			return cmp
		}
	}
	return cmp.Compare(len(m), len(o))
}

// Bytes returns the []byte representation of this Multiaddr
func (m Multiaddr) Bytes() []byte {
	size := 0
	for _, c := range m {
		size += len(c.bytes)
	}

	out := make([]byte, 0, size)
	for _, c := range m {
		out = append(out, c.bytes...)
	}

	return out
}

// String returns the string representation of a Multiaddr
func (m Multiaddr) String() string {
	var buf strings.Builder

	for _, c := range m {
		c.writeTo(&buf)
	}
	return buf.String()
}

func (m Multiaddr) MarshalBinary() ([]byte, error) {
	return m.Bytes(), nil
}

func (m *Multiaddr) UnmarshalBinary(data []byte) error {
	if m == nil {
		return errNilPtr
	}
	new, err := NewMultiaddrBytes(data)
	if err != nil {
		return err
	}
	*m = new
	return nil
}

func (m Multiaddr) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func (m *Multiaddr) UnmarshalText(data []byte) error {
	if m == nil {
		return errNilPtr
	}

	new, err := NewMultiaddr(string(data))
	if err != nil {
		return err
	}
	*m = new
	return nil
}

func (m Multiaddr) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

func (m *Multiaddr) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errNilPtr
	}
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	new, err := NewMultiaddr(v)
	*m = new
	return err
}

// Protocols returns the list of protocols this Multiaddr has.
// will panic in case we access bytes incorrectly.
func (m Multiaddr) Protocols() []Protocol {
	out := make([]Protocol, 0, len(m))
	for _, c := range m {
		out = append(out, c.Protocol())
	}
	return out
}

type Multiaddrer interface {
	// Multiaddr returns the Multiaddr representation
	Multiaddr() Multiaddr
}

func (m Multiaddr) Multiaddr() Multiaddr {
	return m
}

// AppendComponent is the same as using `append(m, *c)`, but with a safety check
// for a nil Component.
func (m Multiaddr) AppendComponent(cs ...*Component) Multiaddr {
	for _, c := range cs {
		if c == nil {
			continue
		}
		m = append(m, *c)
	}
	return m
}

// Encapsulate wraps a given Multiaddr, returning the resulting joined Multiaddr
func (m Multiaddr) Encapsulate(other Multiaddrer) Multiaddr {
	return Join(m, other)
}

// Decapsulate unwraps Multiaddr up until the given Multiaddr is found.
func (m Multiaddr) Decapsulate(rightPartsAny Multiaddrer) Multiaddr {
	if rightPartsAny == nil {
		return m
	}
	rightParts := rightPartsAny.Multiaddr()
	leftParts := m

	lastIndex := -1
	for i := range leftParts {
		foundMatch := false
		for j, rightC := range rightParts {
			if len(leftParts) <= i+j {
				foundMatch = false
				break
			}

			foundMatch = rightC.Equal(&leftParts[i+j])
			if !foundMatch {
				break
			}
		}

		if foundMatch {
			lastIndex = i
		}
	}

	if lastIndex == 0 {
		return nil
	}

	if lastIndex < 0 {
		return m
	}
	return leftParts[:lastIndex]
}

var ErrProtocolNotFound = fmt.Errorf("protocol not found in multiaddr")

func (m Multiaddr) ValueForProtocol(code int) (value string, err error) {
	for _, c := range m {
		if c.Protocol().Code == code {
			return c.Value(), nil
		}
	}
	return "", ErrProtocolNotFound
}

// FilterAddrs is a filter that removes certain addresses, according to the given filters.
// If all filters return true, the address is kept.
func FilterAddrs(a []Multiaddr, filters ...func(Multiaddr) bool) []Multiaddr {
	b := make([]Multiaddr, 0, len(a))
addrloop:
	for _, addr := range a {
		for _, filter := range filters {
			if !filter(addr) {
				continue addrloop
			}
		}
		b = append(b, addr)
	}
	return b
}

// Contains reports whether addr is contained in addrs.
func Contains(addrs []Multiaddr, addr Multiaddr) bool {
	for _, a := range addrs {
		if addr.Equal(a) {
			return true
		}
	}
	return false
}

// Unique deduplicates addresses in place, leave only unique addresses.
// It doesn't allocate.
func Unique(addrs []Multiaddr) []Multiaddr {
	if len(addrs) == 0 {
		return addrs
	}
	// Use the new slices package here, as the sort function doesn't allocate (sort.Slice does).
	slices.SortFunc(addrs, func(a, b Multiaddr) int { return a.Compare(b) })
	idx := 1
	for i := 1; i < len(addrs); i++ {
		if !addrs[i-1].Equal(addrs[i]) {
			addrs[idx] = addrs[i]
			idx++
		}
	}
	for i := idx; i < len(addrs); i++ {
		addrs[i] = nil
	}
	return addrs[:idx]
}
