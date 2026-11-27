// Package dht implements the libp2p Kademlia distributed hash table (DHT),
// including the rules of the public IPFS Amino DHT served under
// /ipfs/kad/1.0.0.
//
// A DHT node finds peers by peer ID, announces and finds content providers,
// and stores and retrieves validated records such as IPNS records and
// public keys. [IpfsDHT] implements the libp2p routing interfaces; create
// one with [New].
//
// By default the node runs in [ModeAuto]: it acts as a client until libp2p
// reports it publicly reachable, then serves queries from other peers. Use
// [Mode] to change this, [BootstrapPeers] to join a network, and
// [ProtocolPrefix] to run a DHT separate from Amino.
//
// Specifications:
//   - https://github.com/libp2p/specs/tree/master/kad-dht
//   - https://specs.ipfs.tech/routing/kad-dht/
package dht
