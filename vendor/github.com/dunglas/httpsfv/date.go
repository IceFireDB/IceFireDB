package httpsfv

import (
	"errors"
	"io"
	"time"
)

var ErrInvalidDateFormat = errors.New("invalid date format")

// marshalDate serializes as defined in
// https://httpwg.org/specs/rfc9651.html#ser-date.
func marshalDate(b io.StringWriter, i time.Time) error {
	_, err := b.WriteString("@")
	if err != nil {
		return err
	}

	return marshalInteger(b, i.Unix())
}

// parseDate parses as defined in
// https://httpwg.org/specs/rfc9651.html#parse-date.
func parseDate(s *scanner) (time.Time, error) {
	if s.eof() || s.data[s.off] != '@' {
		return time.Time{}, &UnmarshalError{s.off, ErrInvalidDateFormat}
	}
	s.off++

	n, err := parseNumber(s)
	if err != nil {
		return time.Time{}, &UnmarshalError{s.off, ErrInvalidDateFormat}
	}

	i, ok := n.(int64)
	if !ok {
		return time.Time{}, &UnmarshalError{s.off, ErrInvalidDateFormat}
	}

	return time.Unix(i, 0), nil
}
