package httpsfv

import (
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
)

const maxDecDigit = 3

// ErrInvalidDecimal is returned when a decimal is invalid.
var ErrInvalidDecimal = errors.New("the integer portion is larger than 12 digits: invalid decimal")

// marshalDecimal serializes as defined in
// https://httpwg.org/specs/rfc9651.html#ser-decimal.
//
// TODO(dunglas): add support for decimal float type when one will be available
// (https://github.com/golang/go/issues/19787)
func marshalDecimal(b io.StringWriter, d float64) error {
	const TH = 0.001

	rounded := math.RoundToEven(d/TH) * TH
	i, frac := math.Modf(rounded)

	if i < -999999999999 || i > 999999999999 {
		return ErrInvalidDecimal
	}

	if _, err := b.WriteString(strings.TrimRight(strconv.FormatFloat(rounded, 'f', 3, 64), "0")); err != nil {
		return err
	}

	if frac == 0 {
		_, err := b.WriteString("0")

		return err
	}

	return nil
}

func parseDecimal(s *scanner, decSepOff int, str string, neg bool) (float64, error) {
	if decSepOff == s.off-1 {
		return 0, &UnmarshalError{s.off, ErrInvalidDecimalFormat}
	}

	if len(s.data[decSepOff+1:s.off]) > maxDecDigit {
		return 0, &UnmarshalError{s.off, ErrNumberOutOfRange}
	}

	i, err := strconv.ParseFloat(str, 64)
	if err != nil {
		// Should never happen
		return 0, &UnmarshalError{s.off, err}
	}

	if neg {
		i = -i
	}

	return i, nil
}
