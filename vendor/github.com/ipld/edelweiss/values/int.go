package values

import (
	"github.com/ipld/edelweiss/defs"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type Int int64

func NewInt(v int64) *Int {
	x := Int(v)
	return &x
}

func (Int) Def() defs.Def {
	return defs.Int{}
}

func (v *Int) Parse(n datamodel.Node) error {
	if n.Kind() != ipld.Kind_Int {
		return ErrNA
	} else {
		*(*int64)(v), _ = n.AsInt()
		return nil
	}
}

func (v Int) Node() datamodel.Node {
	return v
}

// datamodel.Node implementation

func (Int) Kind() datamodel.Kind {
	return datamodel.Kind_Int
}

func (Int) LookupByString(string) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Int) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Int) LookupByIndex(idx int64) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Int) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Int) MapIterator() datamodel.MapIterator {
	return nil
}

func (Int) ListIterator() datamodel.ListIterator {
	return nil
}

func (Int) Length() int64 {
	return -1
}

func (Int) IsAbsent() bool {
	return false
}

func (Int) IsNull() bool {
	return false
}

func (Int) AsBool() (bool, error) {
	return false, ErrNA
}

func (v Int) AsInt() (int64, error) {
	return int64(v), nil
}

func (Int) AsFloat() (float64, error) {
	return 0, ErrNA
}

func (Int) AsString() (string, error) {
	return "", ErrNA
}

func (Int) AsBytes() ([]byte, error) {
	return nil, ErrNA
}

func (Int) AsLink() (datamodel.Link, error) {
	return nil, ErrNA
}

func (Int) Prototype() datamodel.NodePrototype {
	return nil // not needed
}

func TryParseInt(n datamodel.Node) (Int, error) {
	var x Int
	return x, x.Parse(n)
}

// datamodel.NodeAssembler implementation

func (x *Int) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	return nil, ErrNA
}

func (x *Int) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	return nil, ErrNA
}

func (x *Int) AssignNull() error {
	return ErrNA
}

func (x *Int) AssignBool(bool) error {
	return ErrNA
}

func (x *Int) AssignInt(v int64) error {
	*(*int64)(x) = v
	return nil
}

func (x *Int) AssignFloat(float64) error {
	return ErrNA
}

func (x *Int) AssignString(string) error {
	return ErrNA
}

func (x *Int) AssignBytes([]byte) error {
	return ErrNA
}

func (x *Int) AssignLink(datamodel.Link) error {
	return ErrNA
}

func (x *Int) AssignNode(n datamodel.Node) error {
	if v, err := n.AsInt(); err != nil {
		return ErrNA
	} else {
		return x.AssignInt(v)
	}
}
