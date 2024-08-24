package values

import (
	"github.com/ipld/edelweiss/defs"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type List []Any

func (List) Def() defs.Def {
	return defs.List{Element: defs.Any{}}
}

func listEqual(x, y List) bool {
	if len(x) != len(y) {
		return false
	} else {
		for i := range x {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}
}

func (v List) Node() datamodel.Node {
	return v
}

func (v *List) Parse(n datamodel.Node) error {
	if n.Kind() == ipld.Kind_Null {
		*v = nil
		return nil
	}
	if n.Kind() != ipld.Kind_List {
		return ErrNA
	} else {
		*v = make(List, n.Length())
		iter := n.ListIterator()
		for !iter.Done() {
			if i, n, err := iter.Next(); err != nil {
				return ErrUnexpected
			} else if err = (*v)[i].Parse(n); err != nil {
				return err
			}
		}
		return nil
	}
}

// datamodel.Node implementation

func (List) Kind() datamodel.Kind {
	return datamodel.Kind_List
}

func (List) LookupByString(string) (datamodel.Node, error) {
	return nil, ErrNA
}

func (List) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	return nil, ErrNA
}

func (v List) LookupByIndex(i int64) (datamodel.Node, error) {
	if i < 0 || i >= v.Length() {
		return nil, ErrBounds
	} else {
		return v[i].Node(), nil
	}
}

func (v List) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	if i, err := seg.Index(); err != nil {
		return nil, ErrNA
	} else {
		return v.LookupByIndex(i)
	}
}

func (List) MapIterator() datamodel.MapIterator {
	return nil
}

func (v List) ListIterator() datamodel.ListIterator {
	return &listIterator{v, 0}
}

func (v List) Length() int64 {
	return int64(len(v))
}

func (List) IsAbsent() bool {
	return false
}

func (List) IsNull() bool {
	return false
}

func (v List) AsBool() (bool, error) {
	return false, ErrNA
}

func (List) AsInt() (int64, error) {
	return 0, ErrNA
}

func (List) AsFloat() (float64, error) {
	return 0, ErrNA
}

func (List) AsString() (string, error) {
	return "", ErrNA
}

func (List) AsBytes() ([]byte, error) {
	return nil, ErrNA
}

func (List) AsLink() (datamodel.Link, error) {
	return nil, ErrNA
}

func (List) Prototype() datamodel.NodePrototype {
	return nil // not needed
}

type listIterator struct {
	list List
	at   int64
}

func (iter *listIterator) Next() (int64, datamodel.Node, error) {
	if iter.Done() {
		return -1, nil, ErrBounds
	}
	v := iter.list[iter.at]
	i := int64(iter.at)
	iter.at++
	return i, v.Node(), nil
}

func (iter *listIterator) Done() bool {
	return iter.at >= iter.list.Length()
}

func TryParseList(n datamodel.Node) (List, error) {
	var l List
	return l, l.Parse(n)
}
