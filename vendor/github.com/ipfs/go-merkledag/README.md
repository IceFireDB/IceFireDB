go-merkledag
==================

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)
[![Coverage Status](https://codecov.io/gh/ipfs/go-merkledag/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-merkledag/branch/master)

> go-merkledag implements the 'DAGService' interface and adds two ipld node types, Protobuf and Raw 

## Status

❗ This library is maintained, but not actively developed. It will continue to receive fixes and security updates for users that depend on it. However, it may be deprecated in the future and it is recommended that you use alternatives to the functionality in go-merkledag, including:

* A fork of this library for use by Kubo is being maintained here: [github.com/ipfs/boxo/ipld/merkledag](https://pkg.go.dev/github.com/ipfs/boxo/ipld/merkledag)
* Working directly with DAG-PB (ProtoNode) should directly use [github.com/ipld/go-codec-dagpb](https://pkg.go.dev/github.com/ipld/go-codec-dagpb) in conjunction with [github.com/ipld/go-ipld-prime](https://pkg.go.dev/github.com/ipld/go-ipld-prime)
* Traversals / DAG walking should use [github.com/ipld/go-ipld-prime/traversal](https://pkg.go.dev/github.com/ipld/go-ipld-prime/traversal)

## License

MIT © Juan Batiz-Benet
