package bsnet

import "github.com/ipfs/boxo/bitswap/network/bsnet/internal"

var (
	// ProtocolBitswapNoVers is equivalent to the legacy bitswap protocol
	ProtocolBitswapNoVers = internal.ProtocolBitswapNoVers
	// ProtocolBitswapOneZero is the prefix for the legacy bitswap protocol
	ProtocolBitswapOneZero = internal.ProtocolBitswapOneZero
	// ProtocolBitswapOneOne is the prefix for version 1.1.0
	ProtocolBitswapOneOne = internal.ProtocolBitswapOneOne
	// ProtocolBitswap is the current version of the bitswap protocol: 1.2.0
	ProtocolBitswap = internal.ProtocolBitswap
)
