package io

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"time"

	"github.com/alecthomas/units"
	"github.com/ipfs/boxo/files"
	mdag "github.com/ipfs/boxo/ipld/merkledag"
	format "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	"github.com/ipfs/boxo/ipld/unixfs/private/linksize"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("unixfs")

// ErrInvalidHAMTFanout is returned when an invalid HAMT fanout value is provided.
// Valid values must be a positive power of 2 and a multiple of 8 (e.g., 8, 16, 32, 64, 128, 256).
var ErrInvalidHAMTFanout = errors.New("HAMT fanout must be a positive power of 2 and multiple of 8")

// HAMTShardingSize is a global option that allows switching to a HAMTDirectory
// when the BasicDirectory grows above the size (in bytes) signalled by this
// flag. The default size of 0 disables the option.
//
// Size estimation depends on HAMTSizeEstimation mode:
//   - SizeEstimationLinks (default): estimates using link name + CID byte lengths,
//     ignoring Tsize, protobuf overhead, and UnixFS Data field (mode/mtime).
//   - SizeEstimationBlock: computes exact serialized dag-pb block size arithmetically,
//     including the UnixFS Data field and all protobuf encoding overhead.
//   - SizeEstimationDisabled: ignores size entirely, uses only MaxLinks.
//
// Threshold behavior: directory converts to HAMT when estimatedSize > HAMTShardingSize.
// A directory exactly at the threshold stays basic (threshold value is NOT included).
//
// Thread safety: this global is not safe for concurrent modification.
// Set it once during program initialization, before starting any imports.
var HAMTShardingSize = int(256 * units.KiB)

// DefaultShardWidth is the default value used for hamt sharding width.
// Needs to be a power of two (shard entry size) and multiple of 8 (bitfield size).
//
// Thread safety: this global is not safe for concurrent modification.
// Set it once during program initialization, before starting any imports.
var DefaultShardWidth = 256

// SizeEstimationMode defines how directory size is estimated for HAMT sharding decisions.
// If unsure which mode to use, prefer SizeEstimationBlock for accurate estimation.
type SizeEstimationMode int

const (
	// SizeEstimationLinks estimates size using link names + CID byte lengths.
	// This is the legacy behavior: sum(len(link.Name) + len(link.Cid.Bytes()))
	// This mode ignores Tsize, protobuf overhead, and optional metadata fields
	// (mode, mtime), which may lead to underestimation and delayed HAMT conversion.
	// Use only when compatibility with legacy DAGs and software is required.
	SizeEstimationLinks SizeEstimationMode = iota

	// SizeEstimationBlock estimates size using full serialized dag-pb block size.
	// This correctly accounts for all fields including Tsize, protobuf varints,
	// and optional metadata (mode, mtime). Use this mode for accurate HAMT
	// threshold decisions and cross-implementation CID determinism.
	SizeEstimationBlock

	// SizeEstimationDisabled disables size-based HAMT threshold entirely.
	// When set, the decision to convert between BasicDirectory and HAMTDirectory
	// is based solely on the number of links, controlled by MaxLinks option
	// (set via WithMaxLinks). HAMTShardingSize is ignored. Use this mode when
	// you want explicit control over directory sharding based on entry count
	// rather than serialized size.
	SizeEstimationDisabled
)

// HAMTSizeEstimation controls which method is used to estimate directory size
// for HAMT sharding decisions. Default is SizeEstimationLinks for backward compatibility.
// Modern software should set this to SizeEstimationBlock for accurate estimation.
//
// Thread safety: this global is not safe for concurrent modification.
// Set it once during program initialization, before starting any imports.
var HAMTSizeEstimation = SizeEstimationLinks

// varintLen returns the encoded size of a protobuf varint.
func varintLen(v uint64) int {
	// Protobuf varints use 7 bits per byte (MSB is continuation flag), so a value
	// requiring N bits needs ceil(N/7) bytes. This is equivalent to:
	//   if v == 0 { return 1 }
	//   return (bits.Len64(v) + 6) / 7
	// but avoids branching: (9*bitLen + 64) / 64 maps bitLen=0 to 1 and computes
	// ceil(bitLen/7) for bitLen>0 (since 9/64 approximates 1/7).
	return int(9*uint32(bits.Len64(v))+64) / 64
}

// linkSerializedSize returns the exact number of bytes a link adds to a PBNode
// protobuf encoding. This is used for IPIP-499's "block-bytes" HAMT threshold
// estimation mode (SizeEstimationBlock), which provides accurate size tracking
// by accounting for Tsize and protobuf varint overhead that simpler estimation
// methods ignore.
func linkSerializedSize(name string, c cid.Cid, tsize uint64) int {
	cidLen := len(c.Bytes())
	nameLen := len(name)

	// PBLink encoding (all field tags are 1 byte since field numbers < 16):
	// - Hash (field 1, bytes): tag(1) + len_varint + cid_bytes
	// - Name (field 2, string): tag(1) + len_varint + name_bytes
	// - Tsize (field 3, varint): tag(1) + varint
	linkLen := 1 + varintLen(uint64(cidLen)) + cidLen +
		1 + varintLen(uint64(nameLen)) + nameLen +
		1 + varintLen(tsize)

	// Wrapper in PBNode.Links (field 2): tag(1) + len_varint + message
	return 1 + varintLen(uint64(linkLen)) + linkLen
}

// dataFieldSerializedSize returns the exact number of bytes the UnixFS Data field
// adds to a PBNode protobuf encoding for a BasicDirectory. This computes the size
// arithmetically without serialization.
//
// UnixFS Data fields (https://specs.ipfs.tech/unixfs/#data):
//   - Type     (field 1, varint):  USED - always Directory (1) for BasicDirectory
//   - Data     (field 2, bytes):   NOT USED - only for File/Raw content
//   - filesize (field 3, varint):  NOT USED - only for File nodes
//   - blocksizes (field 4, repeated varint): NOT USED - only for File nodes
//   - hashType (field 5, varint):  NOT USED - only for HAMTShard
//   - fanout   (field 6, varint):  NOT USED - only for HAMTShard
//   - mode     (field 7, varint):  USED - optional unix permissions
//   - mtime    (field 8, message): USED - optional modification time
//
// If new fields are added to the UnixFS spec for directories, this function
// must be updated. The test TestDataFieldSerializedSizeMatchesActual verifies
// this calculation against actual protobuf serialization.
func dataFieldSerializedSize(mode os.FileMode, mtime time.Time) int {
	// Inner UnixFS Data message
	innerSize := 0

	// Type field (field 1, varint): Directory = 1
	// Protobuf tag = field_number * 8 + wire_type = 1 * 8 + 0 = 8 (1 byte)
	// Value 1 encodes as 1 byte, so total = 2 bytes
	innerSize += 2

	// Mode field (field 7, optional varint)
	if mode != 0 {
		modeVal := uint64(files.ModePermsToUnixPerms(mode))
		// Protobuf tag = field_number * 8 + wire_type = 7 * 8 + 0 = 56 (1 byte)
		innerSize += 1 + varintLen(modeVal)
	}

	// Mtime field (field 8, optional embedded message)
	if !mtime.IsZero() {
		mtimeSize := 0
		// seconds (field 1, int64 varint)
		secs := mtime.Unix()
		if secs >= 0 {
			mtimeSize += 1 + varintLen(uint64(secs))
		} else {
			// Protobuf encodes negative int64 as 10-byte varint (sign-extended to
			// fill all 64 bits, requiring the maximum varint encoding length).
			mtimeSize += 1 + 10
		}

		// nanos (field 2, optional fixed32)
		if mtime.Nanosecond() > 0 {
			mtimeSize += 1 + 4 // tag(1) + fixed32(4)
		}

		// Mtime wrapper: tag(1) + len_varint + message
		innerSize += 1 + varintLen(uint64(mtimeSize)) + mtimeSize
	}

	// PBNode.Data wrapper: tag(1) + len_varint + innerSize
	return 1 + varintLen(uint64(innerSize)) + innerSize
}

// Directory defines a UnixFS directory. It is used for creating, reading and
// editing directories. It allows to work with different directory schemes,
// like the basic or the HAMT implementation.
//
// It just allows to perform explicit edits on a single directory, working with
// directory trees is out of its scope, they are managed by the MFS layer
// (which is the main consumer of this interface).
type Directory interface {
	// SetCidBuilder sets the CID Builder of the root node.
	SetCidBuilder(cid.Builder)

	// AddChild adds a (name, key) pair to the root node.
	AddChild(context.Context, string, ipld.Node) error

	// ForEachLink applies the given function to Links in the directory.
	ForEachLink(context.Context, func(*ipld.Link) error) error

	// EnumLinksAsync returns a channel which will receive Links in the directory
	// as they are enumerated, where order is not guaranteed
	EnumLinksAsync(context.Context) <-chan format.LinkResult

	// Links returns the all the links in the directory node.
	Links(context.Context) ([]*ipld.Link, error)

	// Find returns the root node of the file named 'name' within this directory.
	// In the case of HAMT-directories, it will traverse the tree.
	//
	// Returns os.ErrNotExist if the child does not exist.
	Find(context.Context, string) (ipld.Node, error)

	// RemoveChild removes the child with the given name.
	//
	// Returns os.ErrNotExist if the child doesn't exist.
	RemoveChild(context.Context, string) error

	// GetNode returns the root of this directory.
	GetNode() (ipld.Node, error)

	// GetCidBuilder returns the CID Builder used.
	GetCidBuilder() cid.Builder

	// GetMaxLinks returns the configured value for MaxLinks.
	GetMaxLinks() int

	// SetMaxLinks sets the number of links for the directory. Used when converting
	// between Basic and HAMT.
	SetMaxLinks(n int)

	// GetMaxHAMTFanout returns the configured value for MaxHAMTFanout.
	GetMaxHAMTFanout() int

	// SetMaxHAMTFanout sets the max width of shards when using a HAMT.
	// It must be a power of 2 and multiple of 8. Used when converting
	// between Basic and HAMT.
	SetMaxHAMTFanout(n int)

	// SetStat sets the stat information for the directory. Used when
	// converting between Basic and HAMT.
	SetStat(os.FileMode, time.Time)

	// GetSizeEstimationMode returns the method used to estimate serialized dag-pb block size
	// for directory type conversion decisions.
	// Returns the instance-specific mode if set, otherwise the global HAMTSizeEstimation.
	GetSizeEstimationMode() SizeEstimationMode

	// SetSizeEstimationMode sets the method used to estimate serialized dag-pb block size.
	// Used when inheriting settings from a parent directory after loading from disk.
	SetSizeEstimationMode(SizeEstimationMode)

	// GetHAMTShardingSize returns the per-directory threshold for HAMT sharding.
	// If not set (0), the global HAMTShardingSize is used.
	GetHAMTShardingSize() int

	// SetHAMTShardingSize sets the per-directory threshold for HAMT sharding.
	// Used when inheriting settings from a parent directory after loading from disk.
	SetHAMTShardingSize(size int)
}

// A DirectoryOption can be used to initialize directories.
type DirectoryOption func(d Directory)

// WithMaxLinks stablishes the max number of links allowed for a Basic
// directory or a Dynamic directory with an underlying Basic directory:
//
// - On Dynamic directories using a BasicDirectory, it can trigger conversion
// to HAMT when set and exceeded. The subsequent HAMT nodes will use
// MaxHAMTFanout as ShardWidth when set, or DefaultShardWidth
// otherwise. Conversion can be triggered too based on HAMTShardingSize.
//
// - On Dynamic directories using a HAMTDirectory, it can trigger conversion
// to BasicDirectory when the number of directory entries is below MaxLinks (and
// HAMTShardingSize allows).
//
// - On pure Basic directories, it causes an error when adding more than
// MaxLinks children.
func WithMaxLinks(n int) DirectoryOption {
	return func(d Directory) {
		d.SetMaxLinks(n)
	}
}

// WithMaxHAMTFanout establishes the maximum fanout factor (number of links) for
// a HAMT directory or a Dynamic directory with an underlying HAMT directory:
//
//   - On Dynamic directories, it specifies the HAMT fanout when a HAMT is
//     used. When unset, DefaultShardWidth applies.
//   - On pure HAMT directories, it sets the ShardWidth, otherwise DefaultShardWidth
//     is used.
//
// Valid values must be a positive power of 2 and a multiple of 8
// (e.g., 8, 16, 32, 64, 128, 256, 512, 1024).
//
// If an invalid value is provided, NewBasicDirectory and NewHAMTDirectory will
// return ErrInvalidHAMTFanout. Use 0 or omit this option to use DefaultShardWidth.
func WithMaxHAMTFanout(n int) DirectoryOption {
	return func(d Directory) {
		d.SetMaxHAMTFanout(n)
	}
}

// WithCidBuilder sets the CidBuilder for new Directories.
func WithCidBuilder(cb cid.Builder) DirectoryOption {
	return func(d Directory) {
		d.SetCidBuilder(cb)
	}
}

// WithStat can be used to set the empty directory node permissions.
func WithStat(mode os.FileMode, mtime time.Time) DirectoryOption {
	return func(d Directory) {
		d.SetStat(mode, mtime)
	}
}

// WithSizeEstimationMode sets the method used to estimate serialized dag-pb block size
// when deciding whether to convert between BasicDirectory and HAMTDirectory.
// This must be set at directory creation time; mode cannot be changed afterwards.
func WithSizeEstimationMode(mode SizeEstimationMode) DirectoryOption {
	return func(d Directory) {
		switch dir := d.(type) {
		case *BasicDirectory:
			dir.sizeEstimation = &mode
		case *HAMTDirectory:
			dir.sizeEstimation = &mode
		}
	}
}

// TODO: Evaluate removing `dserv` from this layer and providing it in MFS.
// (The functions should in that case add a `DAGService` argument.)

// Link size estimation function. For production it's usually the one here
// but during test we may mock it to get fixed sizes.
func productionLinkSize(linkName string, linkCid cid.Cid) int {
	return len(linkName) + linkCid.ByteLen()
}

func init() {
	linksize.LinkSizeFunction = productionLinkSize
}

var (
	_ Directory = (*DynamicDirectory)(nil)
	_ Directory = (*BasicDirectory)(nil)
	_ Directory = (*HAMTDirectory)(nil)
)

// BasicDirectory is the basic implementation of `Directory`. All the entries
// are stored in a single node.
type BasicDirectory struct {
	node  *mdag.ProtoNode
	dserv ipld.DAGService

	// Internal variable used to cache the size of the basic directory.
	// The meaning depends on the size estimation mode (set at creation time):
	//   - SizeEstimationLinks: sum of link name + CID byte lengths (legacy estimate)
	//   - SizeEstimationBlock: exact serialized dag-pb block size (computed arithmetically)
	//   - SizeEstimationDisabled: 0 (size tracking disabled)
	// DO NOT CHANGE THIS as it will affect the HAMT transition behavior in HAMTShardingSize.
	estimatedSize int

	totalLinks int

	// opts
	// maxNumberOfLinks. If set, can trigger conversion to HAMT directory.
	maxLinks      int
	maxHAMTFanout int
	cidBuilder    cid.Builder
	mode          os.FileMode
	mtime         time.Time

	// Size estimation mode. If nil, falls back to global HAMTSizeEstimation.
	// Set at creation time via WithSizeEstimationMode option.
	sizeEstimation *SizeEstimationMode

	// Per-directory HAMT sharding size threshold. If 0, falls back to global HAMTShardingSize.
	hamtShardingSize int
}

// HAMTDirectory is the HAMT implementation of `Directory`.
// (See package `hamt` for more information.)
type HAMTDirectory struct {
	shard *hamt.Shard
	dserv ipld.DAGService

	// opts
	maxLinks      int
	maxHAMTFanout int
	cidBuilder    cid.Builder
	mode          os.FileMode
	mtime         time.Time

	// Track the changes in size by the AddChild and RemoveChild calls
	// for the HAMTShardingSize option.
	sizeChange int
	totalLinks int

	// Size estimation mode. If nil, falls back to global HAMTSizeEstimation.
	sizeEstimation *SizeEstimationMode

	// Per-directory HAMT sharding size threshold. If 0, falls back to global HAMTShardingSize.
	hamtShardingSize int
}

// NewBasicDirectory creates an empty basic directory with the given options.
func NewBasicDirectory(dserv ipld.DAGService, opts ...DirectoryOption) (*BasicDirectory, error) {
	basicDir := &BasicDirectory{
		dserv:         dserv,
		maxHAMTFanout: DefaultShardWidth,
	}

	for _, o := range opts {
		o(basicDir)
	}

	// Validate maxHAMTFanout: 0 means use default, positive must be valid
	if basicDir.maxHAMTFanout == 0 {
		basicDir.maxHAMTFanout = DefaultShardWidth
	} else if !validShardWidth(basicDir.maxHAMTFanout) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidHAMTFanout, basicDir.maxHAMTFanout)
	}

	var node *mdag.ProtoNode
	if basicDir.mode > 0 || !basicDir.mtime.IsZero() {
		node = format.EmptyDirNodeWithStat(basicDir.mode, basicDir.mtime)
	} else {
		node = format.EmptyDirNode()
	}
	basicDir.node = node
	err := basicDir.node.SetCidBuilder(basicDir.cidBuilder)
	if err != nil {
		return nil, err
	}

	// Initialize all size tracking fields from the node.
	// This must be done after node is created and options applied.
	basicDir.computeEstimatedSizeAndTotalLinks()

	return basicDir, nil
}

// NewBasicDirectoryFromNode creates a basic directory wrapping the given
// ProtoNode.
func NewBasicDirectoryFromNode(dserv ipld.DAGService, node *mdag.ProtoNode) *BasicDirectory {
	basicDir := new(BasicDirectory)
	basicDir.node = node
	basicDir.dserv = dserv

	// Extract mode/mtime from node's UnixFS data for size estimation.
	// This allows dataFieldSerializedSize to compute the Data field size
	// without re-parsing or re-serializing the node.
	if data := node.Data(); len(data) > 0 {
		if fsNode, err := format.FSNodeFromBytes(data); err == nil {
			basicDir.mode = fsNode.Mode()
			basicDir.mtime = fsNode.ModTime()
		}
	}

	// Initialize all size tracking fields from the node.
	basicDir.computeEstimatedSizeAndTotalLinks()

	return basicDir
}

// NewHAMTDirectory creates an empty HAMT directory with the given options.
func NewHAMTDirectory(dserv ipld.DAGService, sizeChange int, opts ...DirectoryOption) (*HAMTDirectory, error) {
	dir := &HAMTDirectory{
		dserv:         dserv,
		sizeChange:    sizeChange,
		maxHAMTFanout: DefaultShardWidth,
	}

	for _, opt := range opts {
		opt(dir)
	}

	// Validate maxHAMTFanout: 0 means use default, positive must be valid
	if dir.maxHAMTFanout == 0 {
		dir.maxHAMTFanout = DefaultShardWidth
	} else if !validShardWidth(dir.maxHAMTFanout) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidHAMTFanout, dir.maxHAMTFanout)
	}

	shard, err := hamt.NewShard(dir.dserv, dir.maxHAMTFanout)
	if err != nil {
		return nil, err
	}

	shard.SetCidBuilder(dir.cidBuilder)
	// Propagate mode/mtime to the shard for inclusion in UnixFS data
	shard.SetStat(dir.mode, dir.mtime)
	dir.shard = shard

	return dir, nil
}

// NewHAMTDirectoryFromNode creates a HAMT directory from the given node,
// which must correspond to an existing HAMT.
func NewHAMTDirectoryFromNode(dserv ipld.DAGService, node ipld.Node) (*HAMTDirectory, error) {
	dir := &HAMTDirectory{
		dserv: dserv,
	}

	shard, err := hamt.NewHamtFromDag(dserv, node)
	if err != nil {
		return nil, err
	}
	dir.shard = shard
	dir.totalLinks = len(node.Links())

	return dir, nil
}

// NewDirectory returns a Directory implemented by DynamicDirectory containing
// a BasicDirectory that automatically converts to a from a HAMTDirectory
// based on HAMTShardingSize, MaxLinks and MaxHAMTFanout (when set).
func NewDirectory(dserv ipld.DAGService, opts ...DirectoryOption) (Directory, error) {
	bd, err := NewBasicDirectory(dserv, opts...)
	if err != nil {
		return nil, err
	}
	return &DynamicDirectory{bd}, nil
}

// ErrNotADir implies that the given node was not a unixfs directory
var ErrNotADir = errors.New("merkledag node was not a directory or shard")

// NewDirectoryFromNode loads a unixfs directory from the given IPLD node and
// DAGService.
func NewDirectoryFromNode(dserv ipld.DAGService, node ipld.Node) (Directory, error) {
	protoBufNode, ok := node.(*mdag.ProtoNode)
	if !ok {
		return nil, ErrNotADir
	}

	fsNode, err := format.FSNodeFromBytes(protoBufNode.Data())
	if err != nil {
		return nil, err
	}

	switch fsNode.Type() {
	case format.TDirectory:
		return &DynamicDirectory{NewBasicDirectoryFromNode(dserv, protoBufNode.Copy().(*mdag.ProtoNode))}, nil
	case format.THAMTShard:
		hamtDir, err := NewHAMTDirectoryFromNode(dserv, node)
		if err != nil {
			return nil, err
		}
		// Restore mode/mtime from the HAMT shard's UnixFS metadata.
		// These are needed for HAMT->Basic conversion to preserve metadata.
		hamtDir.SetStat(fsNode.Mode(), fsNode.ModTime())
		return &DynamicDirectory{hamtDir}, nil
	}

	return nil, ErrNotADir
}

// GetMaxLinks returns the configured MaxLinks.
func (d *BasicDirectory) GetMaxLinks() int {
	return d.maxLinks
}

// SetMaxLinks sets the maximum number of links for the Directory, but has no
// side effects until new children are added (in which case the new limit
// applies).
func (d *BasicDirectory) SetMaxLinks(n int) {
	d.maxLinks = n
}

// GetMaxLinks returns the configured HAMTFanout.
func (d *BasicDirectory) GetMaxHAMTFanout() int {
	return d.maxHAMTFanout
}

// SetMaxHAMTFanout sets the HAMT fanout for use during Basic->HAMT conversion.
// The value is validated when the directory is created via NewBasicDirectory.
// Valid values: positive power of 2 and multiple of 8. Use 0 for default.
func (d *BasicDirectory) SetMaxHAMTFanout(n int) {
	d.maxHAMTFanout = n
}

// SetStat stores mode and/or mtime values for use during Basic<->HAMT conversions.
// Pass zero for mode or zero time for mtime to leave that field unchanged.
// Note: clearing previously set values is not supported; to clear mode/mtime,
// create a new directory without them.
//
// This method does NOT modify the underlying node's Data field or cachedBlockSize.
// The stored values are only applied when creating a new directory during conversion
// (via WithStat option). If you need to update an existing node's mode/mtime,
// modify the node directly and replace the directory, or use MFS which handles
// this for you.
func (d *BasicDirectory) SetStat(mode os.FileMode, mtime time.Time) {
	if mode > 0 {
		d.mode = mode
	}
	if !mtime.IsZero() {
		d.mtime = mtime
	}
}

// GetSizeEstimationMode returns the method used to estimate serialized dag-pb block size
// for BasicDirectory to HAMTDirectory conversion decisions.
// Returns the instance-specific mode if set, otherwise the global HAMTSizeEstimation.
func (d *BasicDirectory) GetSizeEstimationMode() SizeEstimationMode {
	if d.sizeEstimation != nil {
		return *d.sizeEstimation
	}
	return HAMTSizeEstimation // fall back to global
}

// SetSizeEstimationMode sets the method used to estimate serialized dag-pb block size.
// Used when inheriting settings from a parent directory after loading from disk.
// Note: This only recomputes estimatedSize, not totalLinks, since link count
// is independent of the estimation mode.
func (d *BasicDirectory) SetSizeEstimationMode(mode SizeEstimationMode) {
	oldMode := d.GetSizeEstimationMode()
	d.sizeEstimation = &mode

	// Only recompute estimatedSize if mode actually changed
	if mode == oldMode {
		return
	}

	// Recompute estimatedSize with new mode, but preserve totalLinks
	savedTotalLinks := d.totalLinks
	d.computeEstimatedSizeAndTotalLinks()
	// Restore totalLinks since it shouldn't change based on mode
	d.totalLinks = savedTotalLinks
}

// GetHAMTShardingSize returns the per-directory threshold for HAMT sharding.
// If not set (0), the global HAMTShardingSize is used.
func (d *BasicDirectory) GetHAMTShardingSize() int {
	return d.hamtShardingSize
}

// SetHAMTShardingSize sets the per-directory threshold for HAMT sharding.
// Used when inheriting settings from a parent directory after loading from disk.
func (d *BasicDirectory) SetHAMTShardingSize(size int) {
	d.hamtShardingSize = size
}

// computeEstimatedSizeAndTotalLinks initializes size tracking fields from the current node.
// The estimatedSize is computed based on the current size estimation mode:
// - SizeEstimationLinks: sum of link name + CID byte lengths
// - SizeEstimationBlock: full serialized dag-pb block size (computed arithmetically)
// - SizeEstimationDisabled: 0
func (d *BasicDirectory) computeEstimatedSizeAndTotalLinks() {
	d.estimatedSize = 0
	d.totalLinks = 0

	mode := d.GetSizeEstimationMode()
	if mode == SizeEstimationBlock && d.node != nil {
		// Compute data field size from stored metadata (no serialization needed).
		// The mode and mtime fields are extracted in NewBasicDirectoryFromNode
		// or set via WithStat option during creation.
		d.estimatedSize = dataFieldSerializedSize(d.mode, d.mtime)

		// Add link sizes using linkSerializedSize function
		for _, l := range d.node.Links() {
			d.estimatedSize += linkSerializedSize(l.Name, l.Cid, l.Size)
			d.totalLinks++
		}
		return
	}

	// For Links mode, accumulate link sizes; for Disabled mode, just count
	for _, l := range d.node.Links() {
		if mode == SizeEstimationLinks {
			d.estimatedSize += linksize.LinkSizeFunction(l.Name, l.Cid)
		}
		d.totalLinks++
	}
}

// updateEstimatedSize adjusts estimatedSize for link changes.
// Pass nil for oldLink when adding, nil for newLink when removing.
// The name parameter is used for size calculation since link.Name may not be set.
func (d *BasicDirectory) updateEstimatedSize(name string, oldLink, newLink *ipld.Link) {
	switch d.GetSizeEstimationMode() {
	case SizeEstimationBlock:
		if oldLink != nil {
			d.estimatedSize -= linkSerializedSize(name, oldLink.Cid, oldLink.Size)
		}
		if newLink != nil {
			d.estimatedSize += linkSerializedSize(name, newLink.Cid, newLink.Size)
		}
	case SizeEstimationLinks:
		if oldLink != nil {
			d.estimatedSize -= linksize.LinkSizeFunction(name, oldLink.Cid)
		}
		if newLink != nil {
			d.estimatedSize += linksize.LinkSizeFunction(name, newLink.Cid)
		}
	default:
		// SizeEstimationDisabled: no-op
	}

	if d.estimatedSize < 0 {
		log.Error("BasicDirectory's estimatedSize went below 0")
		d.computeEstimatedSizeAndTotalLinks()
	}
}

// SetCidBuilder implements the `Directory` interface.
func (d *BasicDirectory) SetCidBuilder(builder cid.Builder) {
	d.cidBuilder = builder
	if d.node != nil {
		d.node.SetCidBuilder(builder)
	}
}

// AddChild implements the `Directory` interface. It adds (or replaces)
// a link to the given `node` under `name`.
func (d *BasicDirectory) AddChild(ctx context.Context, name string, node ipld.Node) error {
	link, err := ipld.MakeLink(node)
	if err != nil {
		return err
	}

	return d.addLinkChild(ctx, name, link)
}

// getEffectiveShardingSize returns the per-directory HAMT sharding threshold if set,
// otherwise falls back to the global HAMTShardingSize.
func (d *BasicDirectory) getEffectiveShardingSize() int {
	if d.hamtShardingSize > 0 {
		return d.hamtShardingSize
	}
	return HAMTShardingSize
}

func (d *BasicDirectory) needsToSwitchToHAMTDir(name string, nodeToAdd ipld.Node) (bool, error) {
	shardingSize := d.getEffectiveShardingSize()
	if shardingSize == 0 { // Option disabled.
		return false, nil
	}

	switch d.GetSizeEstimationMode() {
	case SizeEstimationDisabled:
		return d.needsToSwitchByLinkCount(name, nodeToAdd)
	case SizeEstimationBlock:
		return d.needsToSwitchByBlockSize(name, nodeToAdd)
	default:
		return d.needsToSwitchByLinkSize(name, nodeToAdd)
	}
}

// needsToSwitchByLinkCount only considers MaxLinks, ignoring size-based threshold.
func (d *BasicDirectory) needsToSwitchByLinkCount(name string, nodeToAdd ipld.Node) (bool, error) {
	entryToRemove, _ := d.node.GetNodeLink(name)
	return d.checkMaxLinksExceeded(nodeToAdd, entryToRemove), nil
}

// needsToSwitchByLinkSize uses the legacy estimation based on link names + CID bytes.
func (d *BasicDirectory) needsToSwitchByLinkSize(name string, nodeToAdd ipld.Node) (bool, error) {
	operationSizeChange := 0
	// Find if there is an old entry under that name that will be overwritten.
	entryToRemove, err := d.node.GetNodeLink(name)
	if err != mdag.ErrLinkNotFound {
		if err != nil {
			return false, err
		}
		operationSizeChange -= linksize.LinkSizeFunction(name, entryToRemove.Cid)
	}
	if nodeToAdd != nil {
		operationSizeChange += linksize.LinkSizeFunction(name, nodeToAdd.Cid())
	}

	// Switch to HAMT when size exceeds threshold (> not >=).
	// Directory exactly at threshold stays basic.
	switchShardingSize := d.estimatedSize+operationSizeChange > d.getEffectiveShardingSize()
	switchMaxLinks := d.checkMaxLinksExceeded(nodeToAdd, entryToRemove)
	return switchShardingSize || switchMaxLinks, nil
}

// needsToSwitchByBlockSize uses accurate estimation based on full serialized dag-pb block size.
// The estimatedSize is kept accurate via linkSerializedSize() and dataFieldSerializedSize()
// which compute exact protobuf encoding sizes arithmetically.
func (d *BasicDirectory) needsToSwitchByBlockSize(name string, nodeToAdd ipld.Node) (bool, error) {
	link, err := ipld.MakeLink(nodeToAdd)
	if err != nil {
		return false, err
	}

	// Calculate size delta for this operation
	newLinkSize := linkSerializedSize(name, link.Cid, link.Size)
	oldLinkSize := 0
	var entryToRemove *ipld.Link
	if oldLink, err := d.node.GetNodeLink(name); err == nil {
		entryToRemove = oldLink
		oldLinkSize = linkSerializedSize(name, oldLink.Cid, oldLink.Size)
	}

	estimatedNewSize := d.estimatedSize - oldLinkSize + newLinkSize

	// Switch to HAMT when size exceeds threshold (> not >=).
	// Directory exactly at threshold stays basic.
	switchShardingSize := estimatedNewSize > d.getEffectiveShardingSize()
	switchMaxLinks := d.checkMaxLinksExceeded(nodeToAdd, entryToRemove)
	return switchShardingSize || switchMaxLinks, nil
}

// checkMaxLinksExceeded returns true if adding a new link would exceed maxLinks.
func (d *BasicDirectory) checkMaxLinksExceeded(nodeToAdd ipld.Node, entryToRemove *ipld.Link) bool {
	return nodeToAdd != nil && entryToRemove == nil && d.maxLinks > 0 && (d.totalLinks+1) > d.maxLinks
}

// addLinkChild adds the link as an entry to this directory under the given
// name. Plumbing function for the AddChild API.
func (d *BasicDirectory) addLinkChild(ctx context.Context, name string, link *ipld.Link) error {
	// Remove old link and account for size change (if it existed; ignore
	// `ErrNotExist` otherwise). RemoveChild updates both estimatedSize and
	// totalLinks for the removed link.
	err := d.RemoveChild(ctx, name)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// Entry didn't exist, so this is a new link. Check maxLinks.
		if d.maxLinks > 0 && d.totalLinks+1 > d.maxLinks {
			return errors.New("BasicDirectory: cannot add child: maxLinks reached")
		}
	}
	// else: entry existed and was removed, no maxLinks check needed for replacement

	err = d.node.AddRawLink(name, link)
	if err != nil {
		return err
	}
	d.updateEstimatedSize(name, nil, link)
	d.totalLinks++
	return nil
}

// EnumLinksAsync returns a channel which will receive Links in the directory
// as they are enumerated, where order is not guaranteed
func (d *BasicDirectory) EnumLinksAsync(ctx context.Context) <-chan format.LinkResult {
	linkResults := make(chan format.LinkResult)
	go func() {
		defer close(linkResults)
		for _, l := range d.node.Links() {
			select {
			case linkResults <- format.LinkResult{
				Link: l,
				Err:  nil,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return linkResults
}

// ForEachLink implements the `Directory` interface.
func (d *BasicDirectory) ForEachLink(_ context.Context, f func(*ipld.Link) error) error {
	for _, l := range d.node.Links() {
		if err := f(l); err != nil {
			return err
		}
	}
	return nil
}

// Links implements the `Directory` interface.
func (d *BasicDirectory) Links(ctx context.Context) ([]*ipld.Link, error) {
	return d.node.Links(), nil
}

// Find implements the `Directory` interface.
func (d *BasicDirectory) Find(ctx context.Context, name string) (ipld.Node, error) {
	lnk, err := d.node.GetNodeLink(name)
	if err != nil {
		if errors.Is(err, mdag.ErrLinkNotFound) {
			err = os.ErrNotExist
		}
		return nil, err
	}

	return d.dserv.Get(ctx, lnk.Cid)
}

// RemoveChild implements the `Directory` interface.
func (d *BasicDirectory) RemoveChild(ctx context.Context, name string) error {
	// We need to *retrieve* the link before removing it to update the estimated
	// size. This means we may iterate the links slice twice: if traversing this
	// becomes a problem, a factor of 2 isn't going to make much of a difference.
	// We'd likely need to cache a link resolution map in that case.
	link, err := d.node.GetNodeLink(name)
	if err != nil {
		if errors.Is(err, mdag.ErrLinkNotFound) {
			return os.ErrNotExist
		}
		return err // at the moment there is no other error besides ErrLinkNotFound
	}

	// The name actually existed so we should update the estimated size.
	d.updateEstimatedSize(name, link, nil)
	d.totalLinks--

	return d.node.RemoveNodeLink(name)
	// GetNodeLink didn't return ErrLinkNotFound so this won't fail with that
	// and we don't need to convert the error again.
}

// GetNode implements the `Directory` interface.
func (d *BasicDirectory) GetNode() (ipld.Node, error) {
	return d.node, nil
}

// GetCidBuilder implements the `Directory` interface.
func (d *BasicDirectory) GetCidBuilder() cid.Builder {
	return d.node.CidBuilder()
}

// switchToSharding returns a HAMT implementation of this directory.
func (d *BasicDirectory) switchToSharding(ctx context.Context, opts ...DirectoryOption) (*HAMTDirectory, error) {
	hamtDir, err := NewHAMTDirectory(d.dserv, 0, opts...)
	if err != nil {
		return nil, err
	}

	for _, lnk := range d.node.Links() {
		err = hamtDir.shard.SetLink(ctx, lnk.Name, lnk)
		if err != nil {
			return nil, err
		}
		hamtDir.totalLinks++
	}

	return hamtDir, nil
}

// SetCidBuilder implements the `Directory` interface.
func (d *HAMTDirectory) SetCidBuilder(builder cid.Builder) {
	d.cidBuilder = builder
	if d.shard != nil {
		d.shard.SetCidBuilder(builder)
	}
}

// GetMaxLinks returns the maxLinks value from a HAMTDirectory.
func (d *HAMTDirectory) GetMaxLinks() int {
	return d.maxLinks
}

// SetMaxLinks has no effect and only exists to support Dynamic directories.
func (d *HAMTDirectory) SetMaxLinks(n int) {
	d.maxLinks = n
}

// GetMaxHAMTFanout returns the maxHAMTFanout value from a HAMTDirectory.
func (d *HAMTDirectory) GetMaxHAMTFanout() int {
	return d.maxHAMTFanout
}

// SetMaxHAMTFanout sets the HAMT fanout. The value is validated when the
// directory is created via NewHAMTDirectory.
// Valid values: positive power of 2 and multiple of 8. Use 0 for default.
func (d *HAMTDirectory) SetMaxHAMTFanout(n int) {
	d.maxHAMTFanout = n
}

// SetStat stores mode and/or mtime values for use during HAMT->Basic conversions
// and also propagates them to the underlying shard for inclusion in GetNode().
// Pass zero for mode or zero time for mtime to leave that field unchanged.
// See BasicDirectory.SetStat for full documentation.
func (d *HAMTDirectory) SetStat(mode os.FileMode, mtime time.Time) {
	if mode > 0 {
		d.mode = mode
	}
	if !mtime.IsZero() {
		d.mtime = mtime
	}
	// Also propagate to the shard so GetNode() includes mode/mtime
	if d.shard != nil {
		d.shard.SetStat(d.mode, d.mtime)
	}
}

// GetSizeEstimationMode returns the method used to estimate serialized dag-pb block size
// for HAMTDirectory to BasicDirectory conversion decisions.
// Returns the instance-specific mode if set, otherwise the global HAMTSizeEstimation.
func (d *HAMTDirectory) GetSizeEstimationMode() SizeEstimationMode {
	if d.sizeEstimation != nil {
		return *d.sizeEstimation
	}
	return HAMTSizeEstimation // fall back to global
}

// SetSizeEstimationMode sets the method used to estimate serialized dag-pb block size.
// Used when inheriting settings from a parent directory after loading from disk.
func (d *HAMTDirectory) SetSizeEstimationMode(mode SizeEstimationMode) {
	d.sizeEstimation = &mode
}

// GetHAMTShardingSize returns the per-directory threshold for HAMT sharding.
// If not set (0), the global HAMTShardingSize is used.
func (d *HAMTDirectory) GetHAMTShardingSize() int {
	return d.hamtShardingSize
}

// SetHAMTShardingSize sets the per-directory threshold for HAMT sharding.
// Used when inheriting settings from a parent directory after loading from disk.
func (d *HAMTDirectory) SetHAMTShardingSize(size int) {
	d.hamtShardingSize = size
}

// getEffectiveShardingSize returns the per-directory HAMT sharding threshold if set,
// otherwise falls back to the global HAMTShardingSize.
func (d *HAMTDirectory) getEffectiveShardingSize() int {
	if d.hamtShardingSize > 0 {
		return d.hamtShardingSize
	}
	return HAMTShardingSize
}

// AddChild implements the `Directory` interface.
func (d *HAMTDirectory) AddChild(ctx context.Context, name string, nd ipld.Node) error {
	oldChild, err := d.shard.Swap(ctx, name, nd)
	if err != nil {
		return err
	}

	if oldChild != nil {
		d.removeFromSizeChange(oldChild.Name, oldChild.Cid)
	}
	d.addToSizeChange(name, nd.Cid())
	if oldChild == nil {
		d.totalLinks++
	}
	return nil
}

// ForEachLink implements the `Directory` interface.
func (d *HAMTDirectory) ForEachLink(ctx context.Context, f func(*ipld.Link) error) error {
	return d.shard.ForEachLink(ctx, f)
}

// EnumLinksAsync returns a channel which will receive Links in the directory
// as they are enumerated, where order is not guaranteed
func (d *HAMTDirectory) EnumLinksAsync(ctx context.Context) <-chan format.LinkResult {
	return d.shard.EnumLinksAsync(ctx)
}

// Links implements the `Directory` interface.
func (d *HAMTDirectory) Links(ctx context.Context) ([]*ipld.Link, error) {
	return d.shard.EnumLinks(ctx)
}

// Find implements the `Directory` interface. It will traverse the tree.
func (d *HAMTDirectory) Find(ctx context.Context, name string) (ipld.Node, error) {
	lnk, err := d.shard.Find(ctx, name)
	if err != nil {
		return nil, err
	}

	return lnk.GetNode(ctx, d.dserv)
}

// RemoveChild implements the `Directory` interface.
func (d *HAMTDirectory) RemoveChild(ctx context.Context, name string) error {
	oldChild, err := d.shard.Take(ctx, name)
	if err != nil {
		return err
	}

	if oldChild != nil {
		d.removeFromSizeChange(oldChild.Name, oldChild.Cid)
		d.totalLinks--
	}

	return nil
}

// GetNode implements the `Directory` interface.
func (d *HAMTDirectory) GetNode() (ipld.Node, error) {
	return d.shard.Node()
}

// GetCidBuilder implements the `Directory` interface.
func (d *HAMTDirectory) GetCidBuilder() cid.Builder {
	return d.shard.CidBuilder()
}

// switchToBasic returns a BasicDirectory implementation of this directory.
func (d *HAMTDirectory) switchToBasic(ctx context.Context, opts ...DirectoryOption) (*BasicDirectory, error) {
	// needsoSwichToBasicDir checks d.maxLinks is appropriate. No check is
	// performed here.
	basicDir, err := NewBasicDirectory(d.dserv, opts...)
	if err != nil {
		return nil, err
	}

	err = d.ForEachLink(ctx, func(lnk *ipld.Link) error {
		err := basicDir.addLinkChild(ctx, lnk.Name, lnk)
		if err != nil {
			return err
		}

		return nil
		// This function enumerates all the links in the Directory requiring all
		// shards to be accessible but it is only called *after* sizeBelowThreshold
		// returns true, which means we have already enumerated and fetched *all*
		// shards in the first place (that's the only way we can be really sure
		// we are actually below the threshold).
	})
	if err != nil {
		return nil, err
	}

	return basicDir, nil
}

func (d *HAMTDirectory) addToSizeChange(name string, linkCid cid.Cid) {
	d.sizeChange += linksize.LinkSizeFunction(name, linkCid)
}

func (d *HAMTDirectory) removeFromSizeChange(name string, linkCid cid.Cid) {
	d.sizeChange -= linksize.LinkSizeFunction(name, linkCid)
}

// Evaluate a switch from HAMTDirectory to BasicDirectory in case the size will
// go above the threshold when we are adding or removing an entry.
// In both the add/remove operations any old name will be removed, and for the
// add operation in particular a new entry will be added under that name (otherwise
// nodeToAdd is nil). We compute both (potential) future subtraction and
// addition to the size change.
func (d *HAMTDirectory) needsToSwitchToBasicDir(ctx context.Context, name string, nodeToAdd ipld.Node) (switchToBasic bool, err error) {
	if d.getEffectiveShardingSize() == 0 { // Option disabled.
		return false, nil
	}

	// Find if there is an old entry under that name that will be overwritten
	// (AddEntry) or flat out removed (RemoveEntry).
	entryToRemove, err := d.shard.Find(ctx, name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	// Calculate new total link count after this operation
	newTotalLinks := d.totalLinks
	if nodeToAdd != nil {
		newTotalLinks++
	}
	if entryToRemove != nil {
		newTotalLinks--
	}

	// Check if we can switch based on MaxLinks constraint
	canSwitchMaxLinks := true
	if d.maxLinks > 0 && newTotalLinks > d.maxLinks {
		// prevent switching as we would end up with too many links
		canSwitchMaxLinks = false
	}

	// When size estimation is disabled, only consider link count
	if d.GetSizeEstimationMode() == SizeEstimationDisabled {
		// With SizeEstimationDisabled, we only switch back to BasicDirectory
		// if explicitly allowed by maxLinks (which must be set)
		return canSwitchMaxLinks && d.maxLinks > 0 && newTotalLinks <= d.maxLinks, nil
	}

	operationSizeChange := 0
	if entryToRemove != nil {
		operationSizeChange -= d.linkSizeFor(entryToRemove)
	}
	if nodeToAdd != nil {
		link, err := ipld.MakeLink(nodeToAdd)
		if err != nil {
			return false, err
		}
		operationSizeChange += d.linkSizeFor(link)
	}

	// We must switch if size and maxlinks are below threshold
	canSwitchSize := false
	// Directory size reduced, perhaps below limit.
	if d.sizeChange+operationSizeChange < 0 {
		canSwitchSize, err = d.sizeBelowThreshold(ctx, operationSizeChange)
		if err != nil {
			return false, err
		}
	}

	return canSwitchSize && canSwitchMaxLinks, nil
}

// linkSizeFor returns the size contribution of a link based on the current estimation mode.
func (d *HAMTDirectory) linkSizeFor(link *ipld.Link) int {
	switch d.GetSizeEstimationMode() {
	case SizeEstimationBlock:
		return linkSerializedSize(link.Name, link.Cid, link.Size)
	default:
		return linksize.LinkSizeFunction(link.Name, link.Cid)
	}
}

// Evaluate directory size and a future sizeChange and check if it will be below
// HAMTShardingSize threshold (to trigger a transition to a BasicDirectory).
// Instead of enumerating the entire tree we eagerly call EnumLinksAsync
// until we either reach a value above the threshold (in that case no need
// to keep counting) or an error occurs (like the context being canceled
// if we take too much time fetching the necessary shards).
func (d *HAMTDirectory) sizeBelowThreshold(ctx context.Context, sizeChange int) (bool, error) {
	shardingSize := d.getEffectiveShardingSize()
	if shardingSize == 0 {
		panic("asked to compute HAMT size with HAMTShardingSize option off (0)")
	}

	// Start with Data field overhead for hypothetical BasicDirectory.
	// This is the size of the serialized UnixFS Data message that would exist
	// in a single-block BasicDirectory (Type=Directory + optional mode/mtime).
	// For SizeEstimationLinks mode, we don't include this overhead to maintain
	// backward compatibility with the legacy behavior.
	partialSize := 0
	if d.GetSizeEstimationMode() == SizeEstimationBlock {
		partialSize = dataFieldSerializedSize(d.mode, d.mtime)
	}

	var err error
	below := true

	// We stop the enumeration once we have enough information and exit this function.
	ctx, cancel := context.WithCancel(ctx)
	linkResults := d.EnumLinksAsync(ctx)

	for linkResult := range linkResults {
		if linkResult.Err != nil {
			below = false
			err = linkResult.Err
			break
		}

		partialSize += d.linkSizeFor(linkResult.Link)
		// Check if size exceeds threshold (> not >=, matching upgrade logic).
		// Early exit: no need to enumerate more links once we know we're above.
		if partialSize+sizeChange > shardingSize {
			below = false
			break
		}
	}
	cancel()

	if !below {
		// Wait for channel to close so links are not being read after return.
		for range linkResults {
		}
		return false, err
	}

	// Enumerated all links in all shards before threshold reached.
	return true, nil
}

// DynamicDirectory wraps a Directory interface and provides extra logic
// to switch from BasicDirectory to HAMTDirectory and backwards based on
// size.
type DynamicDirectory struct {
	Directory
}

var _ Directory = (*DynamicDirectory)(nil)

// SetMaxLinks sets the maximum number of links for the underlying Basic
// directory when used. This operation does not produce any side effects, but
// the new value may trigger Basic-to-HAMT conversions when adding new
// children to Basic directories, or HAMT-to-Basic conversion when operating
// with HAMT directories.
func (d *DynamicDirectory) SetMaxLinks(n int) {
	d.Directory.SetMaxLinks(n)
}

// SetMaxHAMTFanout sets the maximum shard width for HAMT directories. This
// operation does not produce any side effect and only takes effect on a
// conversion from Basic to HAMT directory.
func (d *DynamicDirectory) SetMaxHAMTFanout(n int) {
	d.Directory.SetMaxHAMTFanout(n)
}

// SetStat stores mode and/or mtime values for use during Basic<->HAMT conversions.
// Pass zero for mode or zero time for mtime to leave that field unchanged.
// See BasicDirectory.SetStat for full documentation.
func (d *DynamicDirectory) SetStat(mode os.FileMode, mtime time.Time) {
	d.Directory.SetStat(mode, mtime)
}

// AddChild implements the `Directory` interface. We check when adding new entries
// if we should switch to HAMTDirectory according to global option(s).
func (d *DynamicDirectory) AddChild(ctx context.Context, name string, nd ipld.Node) error {
	hamtDir, ok := d.Directory.(*HAMTDirectory)
	if ok {
		// We evaluate a switch in the HAMTDirectory case even for an AddChild
		// as it may overwrite an existing entry and end up actually reducing
		// the directory size.
		switchToBasic, err := hamtDir.needsToSwitchToBasicDir(ctx, name, nd)
		if err != nil {
			return err
		}

		if switchToBasic {
			basicDir, err := hamtDir.switchToBasic(ctx,
				WithMaxLinks(hamtDir.maxLinks),
				WithMaxHAMTFanout(hamtDir.maxHAMTFanout),
				WithCidBuilder(hamtDir.GetCidBuilder()),
				WithStat(hamtDir.mode, hamtDir.mtime),
				WithSizeEstimationMode(hamtDir.GetSizeEstimationMode()),
			)
			if err != nil {
				return err
			}
			err = basicDir.AddChild(ctx, name, nd)
			if err != nil {
				return err
			}
			d.Directory = basicDir
			return nil
		}

		return d.Directory.AddChild(ctx, name, nd)
	}

	// BasicDirectory
	basicDir := d.Directory.(*BasicDirectory)
	switchToHAMT, err := basicDir.needsToSwitchToHAMTDir(name, nd)
	if err != nil {
		return err
	}
	if !switchToHAMT {
		return basicDir.AddChild(ctx, name, nd)
	}

	hamtFanout := DefaultShardWidth
	// Verify that our maxLinks is usuable for ShardWidth (power of 2, multiple of 8)
	if validShardWidth(basicDir.maxHAMTFanout) {
		hamtFanout = basicDir.maxHAMTFanout
	}

	hamtDir, err = basicDir.switchToSharding(ctx,
		WithMaxHAMTFanout(hamtFanout),
		WithMaxLinks(basicDir.maxLinks),
		WithCidBuilder(basicDir.GetCidBuilder()),
		WithStat(basicDir.mode, basicDir.mtime),
		WithSizeEstimationMode(basicDir.GetSizeEstimationMode()),
	)
	if err != nil {
		return err
	}
	// Propagate per-directory HAMT sharding size (not a DirectoryOption)
	hamtDir.SetHAMTShardingSize(basicDir.GetHAMTShardingSize())
	err = hamtDir.AddChild(ctx, name, nd)
	if err != nil {
		return err
	}
	d.Directory = hamtDir
	return nil
}

// RemoveChild implements the `Directory` interface. Used in the case where we wrap
// a HAMTDirectory that might need to be downgraded to a BasicDirectory. The
// upgrade path is in AddChild.
func (d *DynamicDirectory) RemoveChild(ctx context.Context, name string) error {
	hamtDir, ok := d.Directory.(*HAMTDirectory)
	if !ok {
		return d.Directory.RemoveChild(ctx, name)
	}

	switchToBasic, err := hamtDir.needsToSwitchToBasicDir(ctx, name, nil)
	if err != nil {
		return err
	}

	if !switchToBasic {
		return hamtDir.RemoveChild(ctx, name)
	}

	maxLinks := hamtDir.maxLinks
	// We have not removed the element that violates MaxLinks, so we have to +1 the limit. We -1 below.
	if maxLinks > 0 {
		maxLinks++
	}

	basicDir, err := hamtDir.switchToBasic(ctx,
		WithMaxLinks(maxLinks),
		WithMaxHAMTFanout(hamtDir.maxHAMTFanout),
		WithCidBuilder(hamtDir.GetCidBuilder()),
		WithStat(hamtDir.mode, hamtDir.mtime),
		WithSizeEstimationMode(hamtDir.GetSizeEstimationMode()),
	)
	if err != nil {
		return err
	}
	// Propagate per-directory HAMT sharding size (not a DirectoryOption)
	basicDir.SetHAMTShardingSize(hamtDir.GetHAMTShardingSize())

	err = basicDir.RemoveChild(ctx, name)
	if err != nil {
		return err
	}

	if maxLinks > 0 {
		basicDir.maxLinks--
	}
	d.Directory = basicDir
	return nil
}

// validShardWidth verifies that the given number is positive, a power of 2
// and a multiple of 8.
func validShardWidth(n int) bool {
	return n > 0 && (n&(n-1)) == 0 && n&7 == 0
}
