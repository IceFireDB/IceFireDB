package drjson

import (
	"bytes"
	"encoding/json"
)

func marshalJSON(val any) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(val)
	return buf, err
}

// MarshalJSONBytes is needed to avoid changes
// on the original bytes due to HTML escapes.
func MarshalJSONBytes(val any) ([]byte, error) {
	buf, err := marshalJSON(val)
	if err != nil {
		return nil, err
	}

	// remove last \n added by Encode
	return buf.Bytes()[:buf.Len()-1], nil
}
