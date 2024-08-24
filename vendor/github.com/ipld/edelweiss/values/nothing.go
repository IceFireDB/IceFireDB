package values

import (
	"github.com/ipld/edelweiss/defs"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type Nothing struct{}

func (Nothing) Def() defs.Def {
	return defs.Nothing{}
}

func (v *Nothing) Parse(n datamodel.Node) error {
	if n.Kind() != ipld.Kind_Null {
		return ErrNA
	} else {
		return nil
	}
}

func (v Nothing) Node() datamodel.Node {
	return v
}

// datamodel.Node implementation

func (Nothing) Kind() datamodel.Kind {
	return datamodel.Kind_Null
}

func (Nothing) LookupByString(string) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Nothing) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Nothing) LookupByIndex(idx int64) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Nothing) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Nothing) MapIterator() datamodel.MapIterator {
	return nil
}

func (Nothing) ListIterator() datamodel.ListIterator {
	return nil
}

func (Nothing) Length() int64 {
	return -1
}

func (Nothing) IsAbsent() bool {
	return false
}

func (Nothing) IsNull() bool {
	return true
}

func (v Nothing) AsBool() (bool, error) {
	return false, ErrNA
}

func (Nothing) AsInt() (int64, error) {
	return 0, ErrNA
}

func (Nothing) AsFloat() (float64, error) {
	return 0, ErrNA
}

func (Nothing) AsString() (string, error) {
	return "", ErrNA
}

func (Nothing) AsBytes() ([]byte, error) {
	return nil, ErrNA
}

func (Nothing) AsLink() (datamodel.Link, error) {
	return nil, ErrNA
}

func (Nothing) Prototype() datamodel.NodePrototype {
	return nil // not needed
}

func TryParseNothing(n datamodel.Node) (Nothing, error) {
	var nth Nothing
	return nth, nth.Parse(n)
}

// datamodel.NodeAssembler implementation

func (x *Nothing) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	return nil, ErrNA
}

func (x *Nothing) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	return nil, ErrNA
}

func (x *Nothing) AssignNull() error {
	return nil
}

func (x *Nothing) AssignBool(bool) error {
	return ErrNA
}

func (x *Nothing) AssignInt(v int64) error {
	return ErrNA
}

func (x *Nothing) AssignFloat(float64) error {
	return ErrNA
}

func (x *Nothing) AssignString(string) error {
	return ErrNA
}

func (x *Nothing) AssignBytes([]byte) error {
	return ErrNA
}

func (x *Nothing) AssignLink(datamodel.Link) error {
	return ErrNA
}

func (x *Nothing) AssignNode(n datamodel.Node) error {
	if n.IsNull() {
		return nil
	} else {
		return ErrNA
	}
}
