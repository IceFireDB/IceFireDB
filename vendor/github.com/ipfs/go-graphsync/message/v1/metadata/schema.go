package metadata

import (
	_ "embed"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

//go:embed schema.ipldsch
var embedSchema []byte

var Prototype struct {
	Metadata schema.TypedPrototype
}

func init() {
	ts, err := ipld.LoadSchemaBytes(embedSchema)
	if err != nil {
		panic(err)
	}

	Prototype.Metadata = bindnode.Prototype((*Metadata)(nil), ts.TypeByName("Metadata"))
}
