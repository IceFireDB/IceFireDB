package ipldbind

import (
	_ "embed"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

//go:embed schema.ipldsch
var embedSchema []byte

var Prototype struct {
	Message schema.TypedPrototype
}

func init() {
	ts, err := ipld.LoadSchemaBytes(embedSchema)
	if err != nil {
		panic(err)
	}

	Prototype.Message = bindnode.Prototype((*GraphSyncMessageRoot)(nil), ts.TypeByName("GraphSyncMessageRoot"))
}
