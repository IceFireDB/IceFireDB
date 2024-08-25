// Package storage provides a CAR abstraction for the
// github.com/ipld/go-ipld-prime/storage interfaces in the form of a StorageCar.
//
// THIS PACKAGE IS EXPERIMENTAL. Breaking changes may be introduced in
// semver-minor releases before this package stabilizes. Use with caution and
// prefer the blockstore API if stability is required.
//
// StorageCar as ReadableStorage provides basic Get and Has operations. It also
// implements StreamingReadableStorage for the more efficient GetStreaming
// operation which is easily supported by the CAR format.
//
// StorageCar as WritableStorage provides the Put operation. It does not
// implement StreamingWritableStorage because the CAR format requires CIDs to
// be written before the blocks themselves, which is not possible with
// StreamingWritableStorage without buffering. Therefore, the PutStream function
// in github.com/ipld/go-ipld-prime/storage will provide equivalent
// functionality if it were to be implemented here.
//
// StorageCar can be used with an IPLD LinkSystem, defined by
// github.com/ipld/go-ipld-prime/linking, with the
// linking.SetReadStorage and linking.SetWriteStorage functions, to provide
// read and/or write to and/or from a CAR format as required.
//
// The focus of the StorageCar interfaces is to use the minimal possible IO
// interface for the operation(s) being performed.
//
// • OpenReadable requires an io.ReaderAt as seeking is required for
// random-access reads as a ReadableStore.
//
// • NewWritable requires an io.Writer when used to write a CARv1 as this format
// can be written in a continuous stream as blocks are written through a
// WritableStore (i.e. when the WriteAsCarV1 option is turned on). When used to
// write a CARv2, the default mode, a random-access io.WriterAt is required as
// the CARv2 header must be written after the payload is finalized and index
// written in order to indicate payload location in the output. The plain
// Writable store may be used to stream CARv1 contents without buffering;
// only storing CIDs in memory for de-duplication (where required) and to still
// allow Has operations.
//
// • NewReadableWritable requires an io.ReaderAt and an io.Writer as it combines
// the functionality of a NewWritable with OpenReadable, being able to random-
// access read any written blocks.
//
// • OpenReadableWritable requires an io.ReaderAt, an io.Writer and an
// io.WriterAt as it extends the NewReadableWritable functionality with the
// ability to resume an existing CAR. In addition, if the CAR being resumed is
// a CARv2, the IO object being provided must have a Truncate() method (e.g.
// an io.File) in order to properly manage CAR lifecycle and avoid writing a
// corrupt CAR.
//
// The following options are available to customize the behavior of the
// StorageCar:
//
// • WriteAsCarV1
//
// • StoreIdentityCIDs
//
// • AllowDuplicatePuts
//
// • UseWholeCIDs
//
// • ZeroLengthSectionAsEOF
//
// • UseIndexCodec
//
// • UseDataPadding
//
// • UseIndexPadding
//
// • MaxIndexCidSize
//
// • MaxAllowedHeaderSize
//
// • MaxAllowedSectionSize
package storage
