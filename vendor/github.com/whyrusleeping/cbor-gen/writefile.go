package typegen

import (
	"bytes"
	"go/format"
	"os"

	"golang.org/x/xerrors"
)

func WriteTupleEncodersToFile(fname, pkg string, types ...interface{}) error {
	buf := new(bytes.Buffer)

	typeInfos := make([]*GenTypeInfo, len(types))
	for i, t := range types {
		gti, err := ParseTypeInfo(t)
		if err != nil {
			return xerrors.Errorf("failed to parse type info: %w", err)
		}
		typeInfos[i] = gti
	}

	if err := PrintHeaderAndUtilityMethods(buf, pkg, typeInfos); err != nil {
		return xerrors.Errorf("failed to write header: %w", err)
	}

	for _, t := range typeInfos {
		if err := GenTupleEncodersForType(t, buf); err != nil {
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

func WriteMapEncodersToFile(fname, pkg string, types ...interface{}) error {
	buf := new(bytes.Buffer)

	typeInfos := make([]*GenTypeInfo, len(types))
	for i, t := range types {
		gti, err := ParseTypeInfo(t)
		if err != nil {
			return xerrors.Errorf("failed to parse type info: %w", err)
		}
		typeInfos[i] = gti
	}

	if err := PrintHeaderAndUtilityMethods(buf, pkg, typeInfos); err != nil {
		return xerrors.Errorf("failed to write header: %w", err)
	}

	for _, t := range typeInfos {
		if err := GenMapEncodersForType(t, buf); err != nil {
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
