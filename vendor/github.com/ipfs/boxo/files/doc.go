// Package files provides abstractions for working with files, directories,
// and other file-like objects.
//
// # Core Interfaces
//
// The package defines a hierarchy of interfaces:
//
//   - [Node]: Base interface for all file-like objects (mode, modification time, size)
//   - [File]: A regular file with [io.Reader] and [io.Seeker]
//   - [Directory]: Contains entries traversable via [DirIterator]
//   - [FileInfo]: Extends [Node] with local filesystem information
//
// # Implementations
//
//   - [ReaderFile]: Wraps an [io.Reader] as a [File]
//   - [NewSerialFile]: Reads from a local filesystem path
//   - [WebFile]: Reads from an HTTP URL
//   - [NewSliceDirectory]: In-memory directory from a slice of entries
//   - [MultiFileReader]: Multipart HTTP encoding for file trees
package files
