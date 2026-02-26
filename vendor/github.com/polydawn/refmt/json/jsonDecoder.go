package json

import (
	"fmt"
	"io"

	"github.com/polydawn/refmt/shared"
	. "github.com/polydawn/refmt/tok"
)

type stackFrame struct {
	step decoderStep
	some bool // Set to true after first value in any context; use to decide if a comma must precede the next value.
}

type Decoder struct {
	r shared.SlickReader

	stack []stackFrame // When empty, and step returns done, all done.
	frame stackFrame   // Shortcut to end of stack.
}

func NewDecoder(r io.Reader) (d *Decoder) {
	d = &Decoder{
		r:     shared.NewReader(r),
		stack: make([]stackFrame, 0, 10),
	}
	d.frame = stackFrame{d.step_acceptValue, false}
	return
}

func (d *Decoder) Reset() {
	d.stack = d.stack[0:0]
	d.frame = stackFrame{d.step_acceptValue, false}
}

type decoderStep func(tokenSlot *Token) (done bool, err error)

func (d *Decoder) Step(tokenSlot *Token) (done bool, err error) {
	done, err = d.frame.step(tokenSlot)
	// If the step errored: out, entirely.
	if err != nil {
		return true, err
	}
	// If the step wasn't done, return same status.
	if !done {
		return false, nil
	}
	// If it WAS done, and stack empty, we're entirely done.
	nSteps := len(d.stack) - 1
	if nSteps <= 0 {
		return true, nil // that's all folks
	}
	// Pop the stack.  Reset "some" to true.
	d.frame = d.stack[nSteps]
	d.stack = d.stack[0:nSteps]
	return false, nil
}

func (d *Decoder) pushPhase(newPhase decoderStep) {
	d.stack = append(d.stack, d.frame)
	d.frame = stackFrame{newPhase, false}
}

func readn1skippingWhitespace(r shared.SlickReader) (majorByte byte, err error) {
	for {
		majorByte, err = r.Readn1()
		switch majorByte {
		case ' ', '\t', '\r', '\n': // continue
		default:
			return
		}
	}
}

// The original step, where any value is accepted, and no terminators for composites are valid.
// ONLY used in the original step; all other steps handle leaf nodes internally.
func (d *Decoder) step_acceptValue(tokenSlot *Token) (done bool, err error) {
	majorByte, err := readn1skippingWhitespace(d.r)
	if err != nil {
		return true, err
	}
	return d.stepHelper_acceptValue(majorByte, tokenSlot)
}

// Step in midst of decoding an array.
func (d *Decoder) step_acceptArrValueOrBreak(tokenSlot *Token) (done bool, err error) {
	majorByte, err := readn1skippingWhitespace(d.r)
	if err != nil {
		return true, err
	}
	if d.frame.some {
		switch majorByte {
		case ']':
			tokenSlot.Type = TArrClose
			return true, nil
		case ',':
			majorByte, err = readn1skippingWhitespace(d.r)
			if err != nil {
				return true, err
			}
			// and now fall through to the next switch
		default:
			return true, fmt.Errorf("expected comma or array close after array value; got %s", byteToString(majorByte))
		}
	}
	switch majorByte {
	case ']':
		tokenSlot.Type = TArrClose
		return true, nil
	default:
		d.frame = stackFrame{d.frame.step, true}
		_, err := d.stepHelper_acceptValue(majorByte, tokenSlot)
		return false, err
	}
}

// Step in midst of decoding a map, key expected up next, or end.
func (d *Decoder) step_acceptMapKeyOrBreak(tokenSlot *Token) (done bool, err error) {
	majorByte, err := readn1skippingWhitespace(d.r)
	if err != nil {
		return true, err
	}
	if d.frame.some {
		switch majorByte {
		case '}':
			tokenSlot.Type = TMapClose
			return true, nil
		case ',':
			majorByte, err = readn1skippingWhitespace(d.r)
			if err != nil {
				return true, err
			}
			// and now fall through to the next switch
		default:
			return true, fmt.Errorf("expected comma or map close after map value; got %s", byteToString(majorByte))
		}
	}
	switch majorByte {
	case '}':
		tokenSlot.Type = TMapClose
		return true, nil
	default:
		d.frame.some = true
		// Consume a string for key.
		_, err := d.stepHelper_acceptKey(majorByte, tokenSlot) // FIXME surely not *any* value?  not composites, at least?
		if err != nil {
			return true, err
		}
		// Now scan up to consume the colon as well, which is required next.
		majorByte, err = readn1skippingWhitespace(d.r)
		if err != nil {
			return true, err
		}
		if majorByte != ':' {
			return true, fmt.Errorf("expected colon after map key; got %s", byteToString(majorByte))
		}
		// Next up: expect a value.
		d.frame = stackFrame{d.step_acceptMapValue, false}
		return false, err
	}
}

// Step in midst of decoding a map, value expected up next.
func (d *Decoder) step_acceptMapValue(tokenSlot *Token) (done bool, err error) {
	majorByte, err := readn1skippingWhitespace(d.r)
	if err != nil {
		return true, err
	}
	d.frame = stackFrame{d.step_acceptMapKeyOrBreak, true}
	_, err = d.stepHelper_acceptValue(majorByte, tokenSlot)
	return false, err
}

func (d *Decoder) stepHelper_acceptValue(majorByte byte, tokenSlot *Token) (done bool, err error) {
	return d.stepHelper_acceptKV("value", majorByte, tokenSlot)
}

func (d *Decoder) stepHelper_acceptKey(majorByte byte, tokenSlot *Token) (done bool, err error) {
	return d.stepHelper_acceptKV("key", majorByte, tokenSlot)
}

func (d *Decoder) stepHelper_acceptKV(t string, majorByte byte, tokenSlot *Token) (done bool, err error) {
	switch majorByte {
	case '{':
		tokenSlot.Type = TMapOpen
		tokenSlot.Length = -1
		d.pushPhase(d.step_acceptMapKeyOrBreak)
		return false, nil
	case '[':
		tokenSlot.Type = TArrOpen
		tokenSlot.Length = -1
		d.pushPhase(d.step_acceptArrValueOrBreak)
		return false, nil
	case 'n':
		d.r.Readnzc(3) // FIXME must check these equal "ull"!
		tokenSlot.Type = TNull
		return true, nil
	case '"':
		tokenSlot.Type = TString
		tokenSlot.Str, err = d.decodeString()
		return true, err
	case 'f':
		d.r.Readnzc(4) // FIXME must check these equal "alse"!
		tokenSlot.Type = TBool
		tokenSlot.Bool = false
		return true, nil
	case 't':
		d.r.Readnzc(3) // FIXME must check these equal "rue"!
		tokenSlot.Type = TBool
		tokenSlot.Bool = true
		return true, nil
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// Some kind of numeric... but in json, we *can't tell* if it's float or int.
		// JSON in general doesn't differentiate.  But we usually try to anyway.
		// (If this results in us yielding an int, and an obj.Unmarshaller is filling a float,
		// it's the Unmarshaller responsibility to decide to cast that.)
		tokenSlot.Type, tokenSlot.Int, tokenSlot.Float64, err = d.decodeNumber(majorByte)
		return true, err
	default:
		return true, fmt.Errorf("invalid char while expecting start of %s: %s", t, byteToString(majorByte))
	}
}

var byteToStringMap = map[byte]string{
	',': "comma",
	':': "colon",
	'{': "map open",
	'}': "map close",
	'[': "array open",
	']': "array close",
	'"': "quote",
}

func byteToString(b byte) string {
	if s, ok := byteToStringMap[b]; ok {
		return s
	}
	return fmt.Sprintf("0x%x", b)
}
