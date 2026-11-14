// Package bitswap implements the [Bitswap protocol] for exchanging blocks
// between IPFS peers. It supports [protocol versions] 1.0.0, 1.1.0, and
// 1.2.0.
//
// [Bitswap] combines a [client.Client] for requesting blocks and a
// [server.Server] for serving them. Create instances with [New], which
// accepts both client and server options.
//
//	bs := bitswap.New(ctx, network, providerFinder, blockstore)
//	defer bs.Close()
//
//	block, err := bs.GetBlock(ctx, c)
//
// [Bitswap protocol]: https://specs.ipfs.tech/bitswap-protocol/
// [protocol versions]: https://specs.ipfs.tech/bitswap-protocol/#bitswap-protocol-versions
package bitswap
