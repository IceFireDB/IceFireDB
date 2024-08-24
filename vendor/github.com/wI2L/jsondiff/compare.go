package jsondiff

import "encoding/json"

// Compare compares the JSON representations of the
// given values and returns the differences relative
// to the former as a list of JSON Patch operations.
func Compare(source, target interface{}) (Patch, error) {
	var d Differ
	return compare(&d, source, target)
}

// CompareOpts is similar to Compare, but also accepts
// a list of options to configure the behavior.
func CompareOpts(source, target interface{}, opts ...Option) (Patch, error) {
	var d Differ
	d.applyOpts(opts...)

	return compare(&d, source, target)
}

// CompareJSON compares the given JSON documents and
// returns the differences relative to the former as
// a list of JSON Patch operations.
func CompareJSON(source, target []byte) (Patch, error) {
	var d Differ
	return compareJSON(&d, source, target)
}

// CompareJSONOpts is similar to CompareJSON, but also
// accepts a list of options to configure the behavior.
func CompareJSONOpts(source, target []byte, opts ...Option) (Patch, error) {
	var d Differ
	d.applyOpts(opts...)

	return compareJSON(&d, source, target)
}

func compare(d *Differ, src, tgt interface{}) (Patch, error) {
	si, _, err := marshalUnmarshal(src)
	if err != nil {
		return nil, err
	}
	ti, tb, err := marshalUnmarshal(tgt)
	if err != nil {
		return nil, err
	}
	d.targetBytes = tb
	d.Compare(si, ti)

	return d.patch, nil
}

func compareJSON(d *Differ, src, tgt []byte) (Patch, error) {
	var si, ti interface{}
	if err := json.Unmarshal(src, &si); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(tgt, &ti); err != nil {
		return nil, err
	}
	d.targetBytes = tgt
	d.Compare(si, ti)

	return d.patch, nil
}

// marshalUnmarshal returns the result of unmarshaling
// the JSON representation of the given interface value.
func marshalUnmarshal(i interface{}) (interface{}, []byte, error) {
	b, err := json.Marshal(i)
	if err != nil {
		return nil, nil, err
	}
	var val interface{}
	if err := json.Unmarshal(b, &val); err != nil {
		return nil, nil, err
	}
	return val, b, nil
}
