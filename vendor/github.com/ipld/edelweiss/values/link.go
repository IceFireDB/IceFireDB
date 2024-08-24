package values

import (
	"fmt"

	cid "github.com/ipfs/go-cid"
	"github.com/ipld/edelweiss/defs"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	ipldcid "github.com/ipld/go-ipld-prime/linking/cid"
)

// Link models a link to any type.
type Link cid.Cid

func (Link) Def() defs.Def {
	return defs.Link{To: defs.Any{}}
}

func (v *Link) Parse(n datamodel.Node) error {
	if n.Kind() != ipld.Kind_Link {
		return ErrNA
	} else {
		ipldLink, _ := n.AsLink()
		// TODO: Is there a more general way to convert ipld.Link interface into a concrete user object?
		cidLink, ok := ipldLink.(ipldcid.Link)
		if !ok {
			return fmt.Errorf("only cid links are supported")
		} else {
			*v = Link(cidLink.Cid)
			return nil
		}
	}
}

func (v Link) Node() datamodel.Node {
	return v
}

// datamodel.Node implementation

func (Link) Kind() datamodel.Kind {
	return datamodel.Kind_Link
}

func (Link) LookupByString(string) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Link) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Link) LookupByIndex(idx int64) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Link) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return nil, ErrNA
}

func (Link) MapIterator() datamodel.MapIterator {
	return nil
}

func (Link) ListIterator() datamodel.ListIterator {
	return nil
}

func (Link) Length() int64 {
	return -1
}

func (Link) IsAbsent() bool {
	return false
}

func (Link) IsNull() bool {
	return false
}

func (Link) AsBool() (bool, error) {
	return false, ErrNA
}

func (v Link) AsInt() (int64, error) {
	return 0, ErrNA
}

func (Link) AsFloat() (float64, error) {
	return 0, ErrNA
}

func (Link) AsString() (string, error) {
	return "", ErrNA
}

func (Link) AsBytes() ([]byte, error) {
	return nil, ErrNA
}

func (v Link) AsLink() (datamodel.Link, error) {
	return ipldcid.Link{Cid: cid.Cid(v)}, nil
}

func (Link) Prototype() datamodel.NodePrototype {
	return nil // not needed
}

func TryParseLink(n datamodel.Node) (Link, error) {
	var x Link
	return x, x.Parse(n)
}
