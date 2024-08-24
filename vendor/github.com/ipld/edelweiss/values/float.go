package values

import (
	"github.com/ipld/edelweiss/defs"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type Float float64

func NewFloat(v float64) *Float {
	x := Float(v)
	return &x
}

func (Float) Def() defs.Def {
	return defs.Int{}
}

func (v *Float) Parse(n datamodel.Node) error {
	if n.Kind() != ipld.Kind_Float {
		return ErrNA
	} else {
		*(*float64)(v), _ = n.AsFloat()
		return nil
	}
}

func (v Float) Node() datamodel.Node {
	return v
}

// datamodel.Node implementation

func (Float) Kind() datamodel.Kind {
	return datamodel.Kind_Float
}

func (Float) LookupByString(string) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Float) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Float) LookupByIndex(idx int64) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Float) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Float) MapIterator() datamodel.MapIterator {
	return nil
}

func (Float) ListIterator() datamodel.ListIterator {
	return nil
}

func (Float) Length() int64 {
	return -1
}

func (Float) IsAbsent() bool {
	return false
}

func (Float) IsNull() bool {
	return false
}

func (Float) AsBool() (bool, error) {
	return false, ErrNA
}

func (Float) AsInt() (int64, error) {
	return 0, ErrNA
}

func (v Float) AsFloat() (float64, error) {
	return float64(v), nil
}

func (Float) AsString() (string, error) {
	return "", ErrNA
}

func (Float) AsBytes() ([]byte, error) {
	return nil, ErrNA
}

func (Float) AsLink() (datamodel.Link, error) {
	return nil, ErrNA
}

func (Float) Prototype() datamodel.NodePrototype {
	return nil // not needed
}

func TryParseFloat(n datamodel.Node) (Float, error) {
	var x Float
	return x, x.Parse(n)
}

// datamodel.NodeAssembler implementation

func (x *Float) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	return nil, ErrNA
}

func (x *Float) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	return nil, ErrNA
}

func (x *Float) AssignNull() error {
	return ErrNA
}

func (x *Float) AssignBool(bool) error {
	return ErrNA
}

func (x *Float) AssignInt(int64) error {
	return ErrNA
}

func (x *Float) AssignFloat(v float64) error {
	*(*float64)(x) = v
	return nil
}

func (x *Float) AssignString(string) error {
	return ErrNA
}

func (x *Float) AssignBytes([]byte) error {
	return ErrNA
}

func (x *Float) AssignLink(datamodel.Link) error {
	return ErrNA
}

func (x *Float) AssignNode(n datamodel.Node) error {
	if v, err := n.AsFloat(); err != nil {
		return ErrNA
	} else {
		return x.AssignFloat(v)
	}
}
