package jsondiff

import (
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
)

// JSON Patch operation types.
// These are defined in RFC 6902 section 4.
// https://datatracker.ietf.org/doc/html/rfc6902#section-4
const (
	OperationAdd     = "add"
	OperationReplace = "replace"
	OperationRemove  = "remove"
	OperationMove    = "move"
	OperationCopy    = "copy"
	OperationTest    = "test"
)

const (
	fromFieldLen  = 10 // ,"from":""
	valueFieldLen = 9  // ,"value":
	opBaseLen     = 19 // {"op":"","path":""}
)

// Patch represents a series of JSON Patch operations.
type Patch []Operation

// Operation represents a single RFC6902 JSON Patch operation.
type Operation struct {
	Type     string      `json:"op"`
	From     pointer     `json:"from,omitempty"`
	Path     pointer     `json:"path"`
	OldValue interface{} `json:"-"`
	Value    interface{} `json:"value,omitempty"`
}

// String implements the fmt.Stringer interface.
func (o Operation) String() string {
	b, err := json.Marshal(o)
	if err != nil {
		return "<invalid operation>"
	}
	return string(b)
}

// MarshalJSON implements the json.Marshaler interface.
func (o Operation) MarshalJSON() ([]byte, error) {
	type op Operation
	if !o.hasValue() {
		o.Value = nil
	}
	if !o.hasFrom() {
		o.From = emptyPtr
	}
	return json.Marshal(op(o))
}

// jsonLength returns the length in bytes that the
// operation would occupy when marshaled as JSON.
func (o Operation) jsonLength(targetBytes []byte) int {
	l := opBaseLen + len(o.Type) + len(o.Path)

	if o.hasValue() {
		valueLen := len(targetBytes)
		if !o.Path.isRoot() {
			r := gjson.GetBytes(targetBytes, o.Path.toJSONPath())
			valueLen = len(r.Raw)
		}
		l += valueFieldLen + valueLen
	}
	if o.hasFrom() {
		l += fromFieldLen + len(o.From)
	}
	return l
}

func (o Operation) hasFrom() bool {
	switch o.Type {
	case OperationAdd, OperationReplace, OperationTest:
		return false
	default:
		return true
	}
}

func (o Operation) hasValue() bool {
	switch o.Type {
	case OperationCopy, OperationMove:
		return false
	default:
		return true
	}
}

// String implements the fmt.Stringer interface.
func (p Patch) String() string {
	sb := strings.Builder{}

	for i, op := range p {
		if i != 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(op.String())
	}
	return sb.String()
}

func (p *Patch) remove(idx int) Patch {
	return (*p)[:idx+copy((*p)[idx:], (*p)[idx+1:])]
}

func (p *Patch) append(typ string, from, path pointer, src, tgt interface{}) Patch {
	return append(*p, Operation{
		Type:     typ,
		From:     from,
		Path:     path,
		OldValue: src,
		Value:    tgt,
	})
}

func (p Patch) jsonLength(targetBytes []byte) int {
	length := 0
	for _, op := range p {
		length += op.jsonLength(targetBytes)
	}
	// Count comma-separators if the patch
	// has more than one operation.
	if len(p) > 1 {
		length += len(p) - 1
	}
	return length
}
