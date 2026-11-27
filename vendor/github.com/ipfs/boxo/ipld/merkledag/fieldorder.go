package merkledag

import (
	"slices"

	"google.golang.org/protobuf/encoding/protowire"
)

// PBNodeFieldOrder selects the order of the top-level PBNode fields in the
// serialized dag-pb form. Both orders decode to the same logical node, but
// produce different bytes and therefore different CIDs.
type PBNodeFieldOrder int

const (
	// PBNodeLinksFirst writes the repeated Links field (field number 2)
	// before the Data field (field number 1). This is the order the DAG-PB
	// spec requires encoders to produce [1], used by all UnixFS profiles
	// through unixfs-v1-2025.
	//
	// [1]: https://ipld.io/specs/codecs/dag-pb/spec/#protobuf-strictness
	PBNodeLinksFirst PBNodeFieldOrder = iota

	// PBNodeDataFirst writes the Data field (field number 1) before the
	// repeated Links field (field number 2), so streaming readers can
	// process Data (e.g. HAMT parameters) before reading links. The DAG-PB
	// spec says decoders should accept either order [1]; IPIP-550
	// (https://github.com/ipfs/specs/pull/550) defines this one as a
	// low-level opt-in for writers that need it. No named profile selects
	// it, and enabling it changes CIDs.
	//
	// [1]: https://ipld.io/specs/codecs/dag-pb/spec/#protobuf-strictness
	PBNodeDataFirst
)

// DefaultPBNodeFieldOrder is the field order used when encoding a ProtoNode.
// PBNodeDataFirst changes the bytes, and so the CID, of every encoded node
// that has both Data and Links: directories, HAMT shards, and the root and
// intermediate nodes of files larger than one chunk. A node with only one of
// the two fields encodes the same under both orders.
//
// Like the other UnixFS import globals that io.UnixFSProfile.ApplyGlobals
// writes, this is a process-wide setting, not a per-node option.
// Per-node plumbing would touch every producer and consumer of ProtoNode, so
// the global is the accepted compromise. What follows from it:
//
//   - Set it once at startup, before the first encode, and never change it
//     while the process runs. It is read on every encode without
//     synchronization, and a node that was already encoded keeps its cached
//     bytes and CID until it is mutated or re-encoded with
//     EncodeProtobuf(true).
//   - It applies to every ProtoNode, not only UnixFS ones.
//   - A node decoded from storage keeps its wire bytes as its encode cache,
//     so storing it back unchanged keeps its CID. Copy and every mutation
//     drop that cache, and the next encode uses the current order. Switching
//     the order on an existing repository therefore changes the CIDs of
//     nodes whose content did not change, for example MFS directories,
//     which are copied when loaded (io.NewDirectoryFromNode).
var DefaultPBNodeFieldOrder = PBNodeLinksFirst

// moveDataFirst rewrites a links-first dag-pb encoding produced by
// dagpb.AppendEncode into the PBNodeDataFirst order. AppendEncode writes the
// Data field last, so the field occupies the trailing tag+length+bytes span
// of enc and moving that span to the front is a rotation. Reusing the
// reference encoder keeps one source of truth for link sorting and field
// presence. dataLen is the length of the Data field that was encoded; the
// field must be present.
func moveDataFirst(enc []byte, dataLen int) []byte {
	span := protowire.SizeTag(1) + protowire.SizeBytes(dataLen)
	split := len(enc) - span
	return slices.Concat(enc[split:], enc[:split])
}
