package typegen

import (
	"bytes"
	"fmt"
	"go/format"
	"os"

	"golang.org/x/xerrors"
)

// WriteTupleFileEncodersToFile is a convenience wrapper around Gen.WriteTupleEncodersToFile using
// default options.
func WriteTupleEncodersToFile(fname, pkg string, types ...interface{}) error {
	return Gen{}.WriteTupleEncodersToFile(fname, pkg, types...)
}

// WriteTupleFileEncodersToFile generates array backed MarshalCBOR and UnmarshalCBOR implementations for the
// given types in the specified file, with the specified package name.
//
// The MarshalCBOR and UnmarshalCBOR implementations will marshal/unmarshal each type's fields as a
// fixed-length CBOR array of field values.
func (g Gen) WriteTupleEncodersToFile(fname, pkg string, types ...interface{}) error {
	buf := new(bytes.Buffer)

	typeInfos := make([]*GenTypeInfo, len(types))
	for i, t := range types {
		gti, err := ParseTypeInfo(t)
		if err != nil {
			return xerrors.Errorf("failed to parse type info: %w", err)
		}
		typeInfos[i] = gti
	}

	if err := g.PrintHeaderAndUtilityMethods(buf, pkg, typeInfos); err != nil {
		return xerrors.Errorf("failed to write header: %w", err)
	}

	for _, t := range typeInfos {
		if err := g.GenTupleEncodersForType(t, buf); err != nil {
			return xerrors.Errorf("failed to generate encoders: %w", err)
		}
	}

	data, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to format: %w", err)
	}

	fi, err := os.Create(fname)
	if err != nil {
		return xerrors.Errorf("failed to open file: %w", err)
	}

	_, err = fi.Write(data)
	if err != nil {
		_ = fi.Close()
		return err
	}
	_ = fi.Close()

	return nil
}

// WriteMapFileEncodersToFile is a convenience wrapper around Gen.WriteMapEncodersToFile using
// default options.
func WriteMapEncodersToFile(fname, pkg string, types ...interface{}) error {
	return Gen{}.WriteMapEncodersToFile(fname, pkg, types...)
}

// WriteMapFileEncodersToFile generates map backed MarshalCBOR and UnmarshalCBOR implementations for
// the given types in the specified file, with the specified package name.
//
// The MarshalCBOR and UnmarshalCBOR implementations will marshal/unmarshal each type's fields as a
// map of field names to field values.
func (g Gen) WriteMapEncodersToFile(fname, pkg string, types ...interface{}) error {
	buf := new(bytes.Buffer)

	typeInfos := make([]*GenTypeInfo, len(types))
	for i, t := range types {
		gti, err := ParseTypeInfo(t)
		if err != nil {
			return xerrors.Errorf("failed to parse type info: %w", err)
		}
		typeInfos[i] = gti
	}

	if err := g.PrintHeaderAndUtilityMethods(buf, pkg, typeInfos); err != nil {
		return xerrors.Errorf("failed to write header: %w", err)
	}

	for _, t := range typeInfos {
		if err := g.GenMapEncodersForType(t, buf); err != nil {
			return xerrors.Errorf("failed to generate encoders: %w", err)
		}
	}

	data, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}

	fi, err := os.Create(fname)
	if err != nil {
		return xerrors.Errorf("failed to open file: %w", err)
	}

	_, err = fi.Write(data)
	if err != nil {
		_ = fi.Close()
		return err
	}
	_ = fi.Close()

	return nil
}
