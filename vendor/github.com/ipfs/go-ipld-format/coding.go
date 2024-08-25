package format

import (
	"fmt"

	blocks "github.com/ipfs/go-block-format"
)

// DecodeBlockFunc functions decode blocks into nodes.
type DecodeBlockFunc func(block blocks.Block) (Node, error)

// Registry is a structure for storing mappings of multicodec IPLD codec numbers to DecodeBlockFunc functions.
//
// Registry includes no mutexing. If using Registry in a concurrent context, you must handle synchronization yourself.
// (Typically, it is recommended to do initialization earlier in a program, before fanning out goroutines;
// this avoids the need for mutexing overhead.)
//
// Multicodec indicator numbers are specified in
// https://github.com/multiformats/multicodec/blob/master/table.csv .
// You should not use indicator numbers which are not specified in that table
// (however, there is nothing in this implementation that will attempt to stop you, either).
type Registry struct {
	decoders map[uint64]DecodeBlockFunc
}

func (r *Registry) ensureInit() {
	if r.decoders != nil {
		return
	}
	r.decoders = make(map[uint64]DecodeBlockFunc)
}

// Register registers decoder for all blocks with the passed codec.
//
// This will silently replace any existing registered block decoders.
func (r *Registry) Register(codec uint64, decoder DecodeBlockFunc) {
	r.ensureInit()
	if decoder == nil {
		panic("not sensible to attempt to register a nil function")
	}
	r.decoders[codec] = decoder
}

func (r *Registry) Decode(block blocks.Block) (Node, error) {
	// Short-circuit by cast if we already have a Node.
	if node, ok := block.(Node); ok {
		return node, nil
	}

	ty := block.Cid().Type()
	r.ensureInit()
	decoder, ok := r.decoders[ty]

	if ok {
		return decoder(block)
	} else {
		// TODO: get the *long* name for this format
		return nil, fmt.Errorf("unrecognized object type: %d", ty)
	}
}

// Decode decodes the given block using passed DecodeBlockFunc.
// Note: this is just a helper function, consider using the DecodeBlockFunc itself rather than this helper
func Decode(block blocks.Block, decoder DecodeBlockFunc) (Node, error) {
	// Short-circuit by cast if we already have a Node.
	if node, ok := block.(Node); ok {
		return node, nil
	}

	return decoder(block)
}
