// Package io implements convenience objects for working with the IPFS
// UnixFS data format.
//
// # Directory Types
//
// The package provides three directory implementations:
//
//   - [BasicDirectory]: A flat directory storing all entries in a single node.
//   - [HAMTDirectory]: A sharded directory using HAMT (Hash Array Mapped Trie)
//     for large directories.
//   - [DynamicDirectory]: Automatically switches between Basic and HAMT based
//     on size thresholds.
//
// # HAMT Sharding
//
// Directories can automatically convert between basic (flat) and HAMT
// (sharded) formats based on configurable thresholds:
//
//   - [HAMTShardingSize]: Byte threshold for conversion (global default: 256 KiB)
//   - [HAMTSizeEstimation]: How size is estimated ([SizeEstimationLinks],
//     [SizeEstimationBlock], or [SizeEstimationDisabled])
//   - [DefaultShardWidth]: Fanout for HAMT nodes (default: 256)
//
// Use [WithMaxLinks], [WithMaxHAMTFanout], and [WithSizeEstimationMode] to
// configure directories at creation time.
//
// # File Reading
//
// [DagReader] provides io.Reader and io.Seeker interfaces for reading
// UnixFS file content from the DAG.
//
// # CID Profiles (IPIP-499)
//
// [UnixFSProfile] defines import settings for deterministic CID generation.
// Predefined profiles ensure cross-implementation compatibility:
//
//   - [UnixFS_v0_2015]: Legacy CIDv0 settings (256 KiB chunks, dag-pb leaves)
//   - [UnixFS_v1_2025]: Modern CIDv1 settings (1 MiB chunks, raw leaves)
//
// See https://specs.ipfs.tech/ipips/ipip-0499/ for specification details.
package io
