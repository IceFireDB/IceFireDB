package file

import (
	"context"
	"io"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
)

func newDeferredFileNode(ctx context.Context, lsys *ipld.LinkSystem, root ipld.Link) LargeBytesNode {
	dfn := deferredFileNode{
		LargeBytesNode: nil,
		root:           root,
		lsys:           lsys,
		ctx:            ctx,
	}
	dfn.LargeBytesNode = &deferred{&dfn}
	return &dfn
}

type deferredFileNode struct {
	LargeBytesNode

	root ipld.Link
	lsys *ipld.LinkSystem
	ctx  context.Context
}

func (d *deferredFileNode) resolve() error {
	if d.lsys == nil {
		return nil
	}
	target, err := d.lsys.Load(ipld.LinkContext{Ctx: d.ctx}, d.root, protoFor(d.root))
	if err != nil {
		return err
	}

	asFSNode, err := NewUnixFSFile(d.ctx, target, d.lsys)
	if err != nil {
		return err
	}
	d.LargeBytesNode = asFSNode
	d.root = nil
	d.lsys = nil
	d.ctx = nil
	return nil
}

type deferred struct {
	*deferredFileNode
}

type deferredReader struct {
	io.ReadSeeker
	*deferredFileNode
}

func (d *deferred) AsLargeBytes() (io.ReadSeeker, error) {
	return &deferredReader{nil, d.deferredFileNode}, nil
}

func (d *deferredReader) Read(p []byte) (int, error) {
	if d.ReadSeeker == nil {
		if err := d.deferredFileNode.resolve(); err != nil {
			return 0, err
		}
		rs, err := d.deferredFileNode.AsLargeBytes()
		if err != nil {
			return 0, err
		}
		d.ReadSeeker = rs
	}
	return d.ReadSeeker.Read(p)
}

func (d *deferredReader) Seek(offset int64, whence int) (int64, error) {
	if d.ReadSeeker == nil {
		if err := d.deferredFileNode.resolve(); err != nil {
			return 0, err
		}
		rs, err := d.deferredFileNode.AsLargeBytes()
		if err != nil {
			return 0, err
		}
		d.ReadSeeker = rs
	}
	return d.ReadSeeker.Seek(offset, whence)
}

func (d *deferred) Kind() ipld.Kind {
	return ipld.Kind_Bytes
}

func (d *deferred) AsBytes() ([]byte, error) {
	if err := d.deferredFileNode.resolve(); err != nil {
		return []byte{}, err
	}

	return d.deferredFileNode.AsBytes()
}

func (d *deferred) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "bool", MethodName: "AsBool", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d *deferred) AsInt() (int64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "int", MethodName: "AsInt", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d *deferred) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "float", MethodName: "AsFloat", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d *deferred) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "string", MethodName: "AsString", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d *deferred) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "link", MethodName: "AsLink", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d *deferred) AsNode() (ipld.Node, error) {
	return nil, nil
}

func (d *deferred) Size() int {
	return 0
}

func (d *deferred) IsAbsent() bool {
	return false
}

func (d *deferred) IsNull() bool {
	if err := d.deferredFileNode.resolve(); err != nil {
		return true
	}
	return d.deferredFileNode.IsNull()
}

func (d *deferred) Length() int64 {
	return 0
}

func (d *deferred) ListIterator() ipld.ListIterator {
	return nil
}

func (d *deferred) MapIterator() ipld.MapIterator {
	return nil
}

func (d *deferred) LookupByIndex(idx int64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (d *deferred) LookupByString(key string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (d *deferred) LookupByNode(key ipld.Node) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (d *deferred) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

// shardded files / nodes look like dagpb nodes.
func (d *deferred) Prototype() ipld.NodePrototype {
	return dagpb.Type.PBNode
}
