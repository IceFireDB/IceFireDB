package iter

import (
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

type IterLink struct {
	Substrate dagpb.PBLink
}

func (il *IterLink) AsBool() (bool, error) {
	return false, datamodel.ErrWrongKind{}
}
func (il *IterLink) AsBytes() ([]byte, error) {
	return nil, datamodel.ErrWrongKind{}
}
func (il *IterLink) AsFloat() (float64, error) {
	return 0.0, datamodel.ErrWrongKind{}
}
func (il *IterLink) AsInt() (int64, error) {
	return 0, datamodel.ErrWrongKind{}
}
func (il *IterLink) AsLink() (datamodel.Link, error) {
	return il.Substrate.FieldHash().AsLink()
}
func (il *IterLink) AsString() (string, error) {
	return "", datamodel.ErrWrongKind{}
}

func (il *IterLink) IsAbsent() bool {
	return il.Substrate.IsAbsent()
}
func (il *IterLink) IsNull() bool {
	return il.Substrate.IsNull()
}
func (il *IterLink) Kind() datamodel.Kind {
	return datamodel.Kind_Link
}
func (il *IterLink) Length() int64 {
	return 0
}
func (il *IterLink) ListIterator() datamodel.ListIterator {
	return nil
}
func (il *IterLink) LookupByIndex(idx int64) (datamodel.Node, error) {
	return nil, datamodel.ErrWrongKind{}
}
func (il *IterLink) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	return nil, datamodel.ErrWrongKind{}
}
func (il *IterLink) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return nil, datamodel.ErrWrongKind{}
}
func (il *IterLink) LookupByString(key string) (datamodel.Node, error) {
	return nil, datamodel.ErrWrongKind{}
}
func (il *IterLink) MapIterator() datamodel.MapIterator {
	return nil
}
func (il *IterLink) Prototype() datamodel.NodePrototype {
	return basicnode.Prototype__Link{}
}
func (il *IterLink) Representation() datamodel.Node {
	return il
}
