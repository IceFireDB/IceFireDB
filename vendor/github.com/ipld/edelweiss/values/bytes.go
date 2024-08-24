package values

import (
	"github.com/ipld/edelweiss/defs"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type Bytes []byte

func NewBytes(v []byte) *Bytes {
	x := Bytes(v)
	return &x
}

func (Bytes) Def() defs.Def {
	return defs.Bytes{}
}

func (v *Bytes) Parse(n datamodel.Node) error {
	if n.Kind() != ipld.Kind_Bytes {
		return ErrNA
	} else {
		*(*[]byte)(v), _ = n.AsBytes()
		return nil
	}
}

func (v Bytes) Node() datamodel.Node {
	return v
}

// datamodel.Node implementation

func (Bytes) Kind() datamodel.Kind {
	return datamodel.Kind_Bytes
}

func (Bytes) LookupByString(string) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Bytes) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Bytes) LookupByIndex(idx int64) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Bytes) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Bytes) MapIterator() datamodel.MapIterator {
	return nil
}

func (Bytes) ListIterator() datamodel.ListIterator {
	return nil
}

func (Bytes) Length() int64 {
	return -1
}

func (Bytes) IsAbsent() bool {
	return false
}

func (Bytes) IsNull() bool {
	return false
}

func (Bytes) AsBool() (bool, error) {
	return false, ErrNA
}

func (Bytes) AsInt() (int64, error) {
	return 0, ErrNA
}

func (Bytes) AsFloat() (float64, error) {
	return 0, ErrNA
}

func (Bytes) AsString() (string, error) {
	return "", ErrNA
}

func (v Bytes) AsBytes() ([]byte, error) {
	return v, nil
}

func (Bytes) AsLink() (datamodel.Link, error) {
	return nil, ErrNA
}

func (Bytes) Prototype() datamodel.NodePrototype {
	return nil // not needed
}

func TryParseBytes(n datamodel.Node) (Bytes, error) {
	var b Bytes
	return b, b.Parse(n)
}

// datamodel.NodeAssembler implementation

func (x *Bytes) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	return nil, ErrNA
}

func (x *Bytes) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	return nil, ErrNA
}

func (x *Bytes) AssignNull() error {
	return ErrNA
}

func (x *Bytes) AssignBool(bool) error {
	return ErrNA
}

func (x *Bytes) AssignInt(int64) error {
	return ErrNA
}

func (x *Bytes) AssignFloat(float64) error {
	return ErrNA
}

func (x *Bytes) AssignString(string) error {
	return ErrNA
}

func (x *Bytes) AssignBytes(v []byte) error {
	*(*[]byte)(x) = v
	return nil
}

func (x *Bytes) AssignLink(datamodel.Link) error {
	return ErrNA
}

func (x *Bytes) AssignNode(n datamodel.Node) error {
	if v, err := n.AsBytes(); err != nil {
		return ErrNA
	} else {
		return x.AssignBytes(v)
	}
}
