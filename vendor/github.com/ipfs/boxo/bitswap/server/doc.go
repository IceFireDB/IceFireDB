// Package server implements the [Bitswap protocol] server that handles
// incoming block requests from peers (see [interaction pattern]).
//
// [Server] uses a decision engine to determine which blocks to send and in
// what order. Create instances with [New] and customize behavior with [Option]
// functions such as task worker count, peer block request filters, and score
// ledger configuration.
//
// [Bitswap protocol]: https://specs.ipfs.tech/bitswap-protocol/
// [interaction pattern]: https://specs.ipfs.tech/bitswap-protocol/#bitswap-1-2-0-interaction-pattern
package server
