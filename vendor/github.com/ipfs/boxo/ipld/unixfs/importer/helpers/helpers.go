package helpers

import (
	"errors"

	chunker "github.com/ipfs/boxo/chunker"
)

// BlockSizeLimit specifies the maximum size an imported block can have.
// Defaults to chunker.BlockSizeLimit (2MiB), the bitswap spec maximum.
// https://specs.ipfs.tech/bitswap-protocol/#block-sizes
var BlockSizeLimit = chunker.BlockSizeLimit

// rough estimates on expected sizes
var (
	roughLinkBlockSize = 1 << 13    // 8KB
	roughLinkSize      = 34 + 8 + 5 // sha256 multihash + size + no name + protobuf framing
)

// DefaultLinksPerBlock governs how the importer decides how many links there
// will be per block. This calculation is based on expected distributions of:
//   - the expected distribution of block sizes
//   - the expected distribution of link sizes
//   - desired access speed
//
// For now, we use:
//
//	var roughLinkBlockSize = 1 << 13 // 8KB
//	var roughLinkSize = 34 + 8 + 5   // sha256 multihash + size + no name
//	                                 // + protobuf framing
//	var DefaultLinksPerBlock = (roughLinkBlockSize / roughLinkSize)
//	                         = ( 8192 / 47 )
//	                         = (approximately) 174
//
// For CID-deterministic imports, prefer using UnixFSProfile presets from
// ipld/unixfs/io/profile.go which set this and other related globals.
var DefaultLinksPerBlock = roughLinkBlockSize / roughLinkSize

// ErrSizeLimitExceeded signals that a block is larger than BlockSizeLimit.
var ErrSizeLimitExceeded = errors.New("object size limit exceeded")
