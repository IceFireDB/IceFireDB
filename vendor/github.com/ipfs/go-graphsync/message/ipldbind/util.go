package ipldbind

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

func SafeUnwrap(node datamodel.Node) (_ interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			if rerr, ok := r.(error); ok {
				err = rerr
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	ptr := bindnode.Unwrap(node)
	return ptr, err
}

func SafeWrap(ptr interface{}, typ schema.Type) (_ schema.TypedNode, err error) {
	defer func() {
		if r := recover(); r != nil {
			if rerr, ok := r.(error); ok {
				err = rerr
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	node := bindnode.Wrap(ptr, typ)
	return node, err
}
