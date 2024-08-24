package values

import (
	"github.com/ipld/edelweiss/defs"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type Char rune

func NewChar(v rune) *Char {
	x := Char(v)
	return &x
}

func (Char) Def() defs.Def {
	return defs.Char{}
}

func (v *Char) Parse(n datamodel.Node) error {
	if n.Kind() != ipld.Kind_Int {
		return ErrNA
	} else {
		if i, _ := n.AsInt(); int64(rune(i)) != i {
			return ErrNA
		} else {
			*(*rune)(v) = rune(i)
			return nil
		}
	}
}

func (v Char) Node() datamodel.Node {
	return v
}

// datamodel.Node implementation

func (Char) Kind() datamodel.Kind {
	return datamodel.Kind_Int
}

func (Char) LookupByString(string) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Char) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Char) LookupByIndex(idx int64) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Char) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Char) MapIterator() datamodel.MapIterator {
	return nil
}

func (Char) ListIterator() datamodel.ListIterator {
	return nil
}

func (Char) Length() int64 {
	return -1
}

func (Char) IsAbsent() bool {
	return false
}

func (Char) IsNull() bool {
	return false
}

func (Char) AsBool() (bool, error) {
	return false, ErrNA
}

func (v Char) AsInt() (int64, error) {
	return int64(v), nil
}

func (Char) AsFloat() (float64, error) {
	return 0, ErrNA
}

func (Char) AsString() (string, error) {
	return "", ErrNA
}

func (Char) AsBytes() ([]byte, error) {
	return nil, ErrNA
}

func (Char) AsLink() (datamodel.Link, error) {
	return nil, ErrNA
}

func (Char) Prototype() datamodel.NodePrototype {
	return nil // not needed
}

func TryParseChar(n datamodel.Node) (Char, error) {
	var x Char
	return x, x.Parse(n)
}

// datamodel.NodeAssembler implementation

func (x *Char) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	return nil, ErrNA
}

func (x *Char) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	return nil, ErrNA
}

func (x *Char) AssignNull() error {
	return ErrNA
}

func (x *Char) AssignBool(bool) error {
	return ErrNA
}

func (x *Char) AssignInt(v int64) error {
	if int64(rune(v)) != v {
		return ErrBounds
	} else {
		*(*rune)(x) = rune(v)
		return nil
	}
}

func (x *Char) AssignFloat(float64) error {
	return ErrNA
}

func (x *Char) AssignString(string) error {
	return ErrNA
}

func (x *Char) AssignBytes([]byte) error {
	return ErrNA
}

func (x *Char) AssignLink(datamodel.Link) error {
	return ErrNA
}

func (x *Char) AssignNode(n datamodel.Node) error {
	if v, err := n.AsInt(); err != nil {
		return ErrNA
	} else {
		return x.AssignInt(v)
	}
}
