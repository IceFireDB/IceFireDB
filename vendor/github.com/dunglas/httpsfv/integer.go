package httpsfv

import (
	"errors"
	"io"
	"strconv"
)

const maxDigit = 12

// ErrNotDigit is returned when a character should be a digit but isn't.
var ErrNotDigit = errors.New("character is not a digit")

// ErrNumberOutOfRange is returned when the number is too large according to the specification.
var ErrNumberOutOfRange = errors.New("integer or decimal out of range")

// ErrInvalidDecimalFormat is returned when the decimal format is invalid.
var ErrInvalidDecimalFormat = errors.New("invalid decimal format")

const (
	typeInteger = iota
	typeDecimal
)

// marshalInteger serializes as defined in
// https://httpwg.org/specs/rfc9651.html#integer.
func marshalInteger(b io.StringWriter, i int64) error {
	if i < -999999999999999 || i > 999999999999999 {
		return ErrNumberOutOfRange
	}

	_, err := b.WriteString(strconv.FormatInt(i, 10))

	return err
}

// parseNumber parses as defined in
// https://httpwg.org/specs/rfc9651.html#parse-number.
func parseNumber(s *scanner) (interface{}, error) {
	neg := isNeg(s)
	if neg && s.eof() {
		return 0, &UnmarshalError{s.off, ErrUnexpectedEndOfString}
	}

	if !isDigit(s.data[s.off]) {
		return 0, &UnmarshalError{s.off, ErrNotDigit}
	}

	start := s.off
	s.off++

	var (
		decSepOff int
		t         = typeInteger
	)

	for s.off < len(s.data) {
		size := s.off - start
		if (t == typeInteger && (size >= 15)) || size >= 16 {
			return 0, &UnmarshalError{s.off, ErrNumberOutOfRange}
		}

		c := s.data[s.off]
		if isDigit(c) {
			s.off++

			continue
		}

		if t == typeInteger && c == '.' {
			if size > maxDigit {
				return 0, &UnmarshalError{s.off, ErrNumberOutOfRange}
			}

			t = typeDecimal
			decSepOff = s.off
			s.off++

			continue
		}

		break
	}

	str := s.data[start:s.off]

	if t == typeInteger {
		return parseInteger(str, neg, s.off)
	}

	return parseDecimal(s, decSepOff, str, neg)
}

func isNeg(s *scanner) bool {
	if s.data[s.off] == '-' {
		s.off++

		return true
	}

	return false
}

func parseInteger(str string, neg bool, off int) (int64, error) {
	i, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		// Should never happen
		return 0, &UnmarshalError{off, err}
	}

	if neg {
		i = -i
	}

	if i < -999999999999999 || i > 999999999999999 {
		return 0, &UnmarshalError{off, ErrNumberOutOfRange}
	}

	return i, err
}
