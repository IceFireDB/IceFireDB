// Package mfs implements an in-memory model of a mutable IPFS filesystem.
//
// The filesystem is rooted at a [Root] which contains a tree of [Directory]
// and [File] nodes. Changes to files and directories are accumulated in
// memory and flushed to the underlying DAG service.
//
// # Structure
//
//   - [Root]: Top-level entry point, created via [NewRoot], with optional
//     republishing of the root CID on changes
//   - [Directory]: A mutable directory that maps names to child nodes
//   - [File]: A mutable file backed by a UnixFS DAG
//
// # Filesystem Operations
//
// Top-level functions [Mv], [Lookup], and [FlushPath] operate on paths within
// the MFS tree. Directories support adding, removing, and listing entries.
// Files support reading and writing through [FileDescriptor].
package mfs
