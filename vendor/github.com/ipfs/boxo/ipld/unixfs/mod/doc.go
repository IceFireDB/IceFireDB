// Package mod provides DAG modification utilities for UnixFS files.
//
// This package is designed for MFS (Mutable File System) operations where
// files need to be modified in place: writing at offsets, appending data,
// truncating, and seeking within files.
//
// # Metadata Handling (Mode and ModTime)
//
// UnixFS supports optional Mode (unix permissions) and ModTime (modification
// time) metadata. Most use cases do not set these fields, and files without
// metadata work normally.
//
// When metadata IS present, DagModifier follows Unix filesystem semantics:
//
//   - ModTime is automatically updated to time.Now() when content is modified
//     or truncated, matching how Unix filesystems behave.
//   - Mode is preserved during modifications.
//
// If you need a specific mtime after modification (e.g., for archival or
// reproducible builds), explicitly set it on the FSNode after the operation.
//
// # Identity CID Handling
//
// This package automatically handles identity CIDs (multihash code 0x00) which
// inline data directly in the CID. When modifying nodes with identity CIDs,
// the package ensures the [verifcid.DefaultMaxIdentityDigestSize] limit is
// respected by automatically switching to a cryptographic hash function when
// the encoded data would exceed this limit. The replacement hash function is
// chosen from (in order): the configured Prefix if non-identity, or
// [util.DefaultIpfsHash] as a fallback.
//
// # RawNode Growth
//
// When appending data to a RawNode that would require multiple blocks, the
// node is automatically converted to a UnixFS file structure. This is necessary
// because RawNodes cannot have child nodes. The original raw data remains
// accessible via its original CID, while the new structure provides full
// UnixFS capabilities.
//
// # Raw Leaf Collapsing
//
// When RawLeaves is enabled and a file fits in a single block with no metadata
// (no Mode or ModTime), GetNode returns a RawNode directly rather than a
// ProtoNode wrapper. This ensures CID compatibility with `ipfs add --raw-leaves`.
// Files with metadata always return ProtoNode to preserve the metadata.
package mod
