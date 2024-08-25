package typegen

import (
	"fmt"
	"io"
	"time"
)

var (
	CborBoolFalse = []byte{0xf4}
	CborBoolTrue  = []byte{0xf5}
	CborNull      = []byte{0xf6}
)

func EncodeBool(b bool) []byte {
	if b {
		return CborBoolTrue
	}
	return CborBoolFalse
}

func WriteBool(w io.Writer, b bool) error {
	_, err := w.Write(EncodeBool(b))
	return err
}

type CborBool bool

func (cb CborBool) MarshalCBOR(w io.Writer) error {
	return WriteBool(w, bool(cb))
}

func (cb *CborBool) UnmarshalCBOR(r io.Reader) error {
	t, val, err := CborReadHeader(r)
	if err != nil {
		return err
	}

	if t != MajOther {
		return fmt.Errorf("booleans should be major type 7")
	}

	switch val {
	case 20:
		*cb = false
	case 21:
		*cb = true
	default:
		return fmt.Errorf("booleans are either major type 7, value 20 or 21 (got %d)", val)
	}
	return nil
}

type CborInt int64

func (ci CborInt) MarshalCBOR(w io.Writer) error {
	v := int64(ci)
	if v >= 0 {
		if err := WriteMajorTypeHeader(w, MajUnsignedInt, uint64(v)); err != nil {
			return err
		}
	} else {
		if err := WriteMajorTypeHeader(w, MajNegativeInt, uint64(-v)-1); err != nil {
			return err
		}
	}
	return nil
}

func (ci *CborInt) UnmarshalCBOR(r io.Reader) error {
	maj, extra, err := CborReadHeader(r)
	if err != nil {
		return err
	}
	var extraI int64
	switch maj {
	case MajUnsignedInt:
		extraI = int64(extra)
		if extraI < 0 {
			return fmt.Errorf("int64 positive overflow")
		}
	case MajNegativeInt:
		extraI = int64(extra)
		if extraI < 0 {
			return fmt.Errorf("int64 negative overflow")
		}
		extraI = -1 - extraI
	default:
		return fmt.Errorf("wrong type for int64 field: %d", maj)
	}

	*ci = CborInt(extraI)
	return nil
}

type CborTime time.Time

func (ct CborTime) MarshalCBOR(w io.Writer) error {
	nsecs := ct.Time().UnixNano()

	cbi := CborInt(nsecs)

	return cbi.MarshalCBOR(w)
}

func (ct *CborTime) UnmarshalCBOR(r io.Reader) error {
	var cbi CborInt
	if err := cbi.UnmarshalCBOR(r); err != nil {
		return err
	}

	t := time.Unix(0, int64(cbi))

	*ct = (CborTime)(t)
	return nil
}

func (ct CborTime) Time() time.Time {
	return (time.Time)(ct)
}

func (ct CborTime) MarshalJSON() ([]byte, error) {
	return ct.Time().MarshalJSON()
}

func (ct *CborTime) UnmarshalJSON(b []byte) error {
	var t time.Time
	if err := t.UnmarshalJSON(b); err != nil {
		return err
	}
	*(*time.Time)(ct) = t
	return nil
}
