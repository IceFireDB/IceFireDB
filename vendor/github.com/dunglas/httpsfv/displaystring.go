package httpsfv

import (
	"encoding/hex"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

type DisplayString string

var ErrInvalidDisplayString = errors.New("invalid display string type")

var notVcharOrSp = &unicode.RangeTable{
	R16: []unicode.Range16{
		{0x0000, 0x001f, 1},
		{0x007f, 0x00ff, 1},
	},
	LatinOffset: 2,
}

// marshalSFV serializes as defined in
// https://httpwg.org/specs/rfc9651.html#ser-string.
func (s DisplayString) marshalSFV(b *strings.Builder) error {
	if _, err := b.WriteString(`%"`); err != nil {
		return err
	}

	for i := 0; i < len(s); i++ {
		if s[i] == '%' || s[i] == '"' || unicode.Is(notVcharOrSp, rune(s[i])) {
			b.WriteRune('%')
			b.WriteString(hex.EncodeToString([]byte{s[i]}))

			continue
		}

		b.WriteByte(s[i])
	}

	b.WriteByte('"')

	return nil
}

// parseDisplayString parses as defined in
// https://httpwg.org/specs/rfc9651.html#parse-display.
func parseDisplayString(s *scanner) (DisplayString, error) {
	if s.eof() || len(s.data[s.off:]) < 2 || s.data[s.off:2] != `%"` {
		return "", &UnmarshalError{s.off, ErrInvalidDisplayString}
	}
	s.off += 2

	var b strings.Builder
	for !s.eof() {
		c := s.data[s.off]
		s.off++

		switch c {
		case '%':
			if len(s.data[s.off:]) < 2 {
				return "", &UnmarshalError{s.off, ErrInvalidDisplayString}
			}
			c0 := unhex(s.data[s.off])
			if c0 == 0 {
				return "", &UnmarshalError{s.off, ErrInvalidDisplayString}
			}

			c1 := unhex(s.data[s.off+1])
			if c1 == 0 {
				return "", &UnmarshalError{s.off, ErrInvalidDisplayString}
			}

			b.WriteByte(c0<<4 | c1)
			s.off += 2
		case '"':
			r := b.String()
			if !utf8.ValidString(r) {
				return "", ErrInvalidDisplayString
			}

			return DisplayString(r), nil

		default:
			if unicode.Is(notVcharOrSp, rune(c)) {
				return "", &UnmarshalError{s.off, ErrInvalidDisplayString}
			}

			b.WriteByte(c)
		}
	}

	return "", &UnmarshalError{s.off, ErrInvalidDisplayString}
}

func unhex(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	default:
		return 0
	}
}
