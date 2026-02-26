package io

import (
	"github.com/alecthomas/units"
	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// UnixFSProfile defines a set of UnixFS import settings for CID determinism.
// Profiles ensure that different implementations produce the same CID for
// the same input when using the same profile.
//
// See IPIP-499 for details: https://github.com/ipfs/specs/pull/499
type UnixFSProfile struct {
	// CIDVersion specifies the CID version (0 or 1).
	// CIDv0 only supports dag-pb codec, CIDv1 supports multiple codecs.
	CIDVersion int

	// MhType is the multihash function code that determines the hash algorithm.
	// Historical default is mh.SHA2_256 (0x12).
	MhType uint64

	// ChunkSize is the maximum size of file chunks (in bytes).
	// Common values: 256 KiB (legacy), 1 MiB (modern).
	ChunkSize int64

	// FileDAGWidth is the maximum number of links per file DAG node.
	// Common values: 174 (legacy), 1024 (modern).
	FileDAGWidth int

	// RawLeaves controls whether file leaf nodes use raw codec (true)
	// or dag-pb wrapped UnixFS nodes (false).
	// Raw leaves require CIDv1; CIDv0 only supports dag-pb leaves.
	RawLeaves bool

	// HAMTShardingSize is the threshold (in bytes) for switching to HAMT directories.
	// 0 disables HAMT sharding entirely.
	HAMTShardingSize int

	// HAMTSizeEstimation controls how directory size is estimated for HAMT threshold.
	// Use SizeEstimationBlock for accurate estimation, SizeEstimationLinks for legacy
	// behavior, or SizeEstimationDisabled to rely solely on link count.
	HAMTSizeEstimation SizeEstimationMode

	// HAMTShardWidth is the fanout for HAMT directory nodes.
	// Must be a power of 2 and multiple of 8.
	HAMTShardWidth int
}

// Predefined profiles matching IPIP-499 specifications.
var (
	// UnixFS_v0_2015 matches the unixfs-v0-2015 profile from IPIP-499.
	// Documents default UnixFS DAG construction parameters used by Kubo
	// through version 0.39 when producing CIDv0. Use when compatibility
	// with historical CIDv0 references is required.
	//
	// Historical note on FileDAGWidth=174:
	//
	//	var roughLinkBlockSize = 1 << 13 // 8KB
	//	var roughLinkSize = 34 + 8 + 5   // sha256 multihash + size + no name
	//	                                 // + protobuf framing
	//	var DefaultLinksPerBlock = (roughLinkBlockSize / roughLinkSize)
	//	                         = ( 8192 / 47 )
	//	                         = (approximately) 174
	UnixFS_v0_2015 = UnixFSProfile{
		CIDVersion:         0,
		MhType:             mh.SHA2_256,
		ChunkSize:          int64(256 * units.KiB),
		FileDAGWidth:       174,
		RawLeaves:          false, // dag-pb leaves for CIDv0
		HAMTShardingSize:   int(256 * units.KiB),
		HAMTSizeEstimation: SizeEstimationLinks,
		HAMTShardWidth:     256,
	}

	// UnixFS_v1_2025 matches the unixfs-v1-2025 profile from IPIP-499.
	// Opinionated profile for deterministic CID generation with CIDv1.
	// Use for cross-implementation CID determinism with modern settings.
	UnixFS_v1_2025 = UnixFSProfile{
		CIDVersion:         1,
		MhType:             mh.SHA2_256,
		ChunkSize:          int64(1 * units.MiB),
		FileDAGWidth:       1024,
		RawLeaves:          true, // raw leaves for CIDv1
		HAMTShardingSize:   int(256 * units.KiB),
		HAMTSizeEstimation: SizeEstimationBlock,
		HAMTShardWidth:     256,
	}
)

// ApplyGlobals sets the global variables to match this profile's settings.
// This affects all subsequent file and directory import operations.
// Note: RawLeaves and CidBuilder are not globals; pass them to DAG builder options.
//
// Thread safety: this function modifies global variables and is not safe
// for concurrent use. Call it once during program initialization, before
// starting any imports. Do not call from multiple goroutines.
func (p UnixFSProfile) ApplyGlobals() {
	// File settings
	chunk.DefaultBlockSize = p.ChunkSize
	helpers.DefaultLinksPerBlock = p.FileDAGWidth

	// Directory settings
	HAMTShardingSize = p.HAMTShardingSize
	HAMTSizeEstimation = p.HAMTSizeEstimation
	DefaultShardWidth = p.HAMTShardWidth
}

// CidBuilder returns a cid.Builder configured for this profile.
// Pass this to DagBuilderParams.CidBuilder when importing files.
func (p UnixFSProfile) CidBuilder() cid.Builder {
	return cid.Prefix{
		Version:  uint64(p.CIDVersion),
		Codec:    cid.DagProtobuf,
		MhType:   p.MhType,
		MhLength: -1, // default length for the hash function
	}
}
