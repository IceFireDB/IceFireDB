package values

import (
	"github.com/ipld/edelweiss/defs"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type Map []KeyValue

type KeyValue struct {
	Key   Any
	Value Any
}

func (v Map) Def() defs.Def {
	return defs.Map{Key: defs.Any{}, Value: defs.Any{}}
}

func (v Map) Node() datamodel.Node {
	return v
}

func (v *Map) Parse(n datamodel.Node) error {
	if n.Kind() != ipld.Kind_Map {
		return ErrNA
	} else {
		iter := n.MapIterator()
		for !iter.Done() {
			kn, vn, _ := iter.Next()
			var kv KeyValue
			if err := kv.Key.Parse(kn); err != nil {
				return err
			}
			if err := kv.Value.Parse(vn); err != nil {
				return err
			}
			*v = append(*v, kv)
		}
		return nil
	}
}

// datamodel.Node implementation

func (Map) Kind() datamodel.Kind {
	return datamodel.Kind_Map
}

func (v Map) LookupByString(s string) (datamodel.Node, error) {
	return v.LookupByNode(String(s))
}

func (v Map) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	for _, kv := range v {
		if ipld.DeepEqual(kv.Key.Node(), key) {
			return kv.Value.Node(), nil
		}
	}
	return nil, ErrNotFound
}

func (v Map) LookupByIndex(i int64) (datamodel.Node, error) {
	return v.LookupByNode(Int(i))
}

func (v Map) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	if idx, err := seg.Index(); err != nil {
		return v.LookupByString(seg.String())
	} else {
		return v.LookupByIndex(idx)
	}
}

func (v Map) MapIterator() datamodel.MapIterator {
	return &mapIterator{v, 0}
}

func (v Map) ListIterator() datamodel.ListIterator {
	return nil
}

func (v Map) Length() int64 {
	return int64(len(v))
}

func (Map) IsAbsent() bool {
	return false
}

func (Map) IsNull() bool {
	return false
}

func (v Map) AsBool() (bool, error) {
	return false, ErrNA
}

func (Map) AsInt() (int64, error) {
	return 0, ErrNA
}

func (Map) AsFloat() (float64, error) {
	return 0, ErrNA
}

func (Map) AsString() (string, error) {
	return "", ErrNA
}

func (Map) AsBytes() ([]byte, error) {
	return nil, ErrNA
}

func (Map) AsLink() (datamodel.Link, error) {
	return nil, ErrNA
}

func (Map) Prototype() datamodel.NodePrototype {
	return nil // not needed
}

type mapIterator struct {
	m  Map
	at int64
}

func (iter *mapIterator) Next() (datamodel.Node, datamodel.Node, error) {
	if iter.Done() {
		return nil, nil, ErrBounds
	}
	v := iter.m[iter.at]
	iter.at++
	return v.Key.Node(), v.Value.Node(), nil
}

func (iter *mapIterator) Done() bool {
	return iter.at >= int64(len(iter.m))
}

func TryParseMap(n datamodel.Node) (Map, error) {
	var s Map
	return s, s.Parse(n)
}
