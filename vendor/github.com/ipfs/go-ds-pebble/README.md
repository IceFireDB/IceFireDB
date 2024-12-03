# go-ds-pebble: Pebble-backed datastore

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://protocol.ai)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)
[![](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23ipfs)
[![GoDoc](https://godoc.org/github.com/ipfs/go-ds-pebble?status.svg)](https://godoc.org/github.com/ipfs/go-ds-pebble)
[![Build Status](https://travis-ci.org/ipfs/go-ds-pebble.svg?branch=master)](https://travis-ci.org/ipfs/go-ds-pebble)

> 🐣 Status: experimental
> A datastore implementation using [cockroachdb/pebble](https://github.com/cockroachdb/pebble) (a native-Go RocksDB equivalent) as a backend.

This is a simple adapter to plug in [cockroachdb/pebble](https://github.com/cockroachdb/pebble) as a backend
anywhere that accepts a [go-datastore](https://github.com/ipfs/go-datastore).

Amongst other software, this includes:

* [go-ipfs](https://github.com/ipfs/go-ipfs/)
* [go-libp2p](https://github.com/libp2p/go-libp2p/)
* [Lotus](https://github.com/filecoin-project/lotus), the reference Filecoin implementation written in Go.

## Status

This implementation is experimental. It is currently exercised against the test
suite under [go-datastore](https://github.com/ipfs/go-datastore).

The road to maturity includes:
 
 * Benchmarks against go-ds-badger, go-ds-badger2, and go-ds-leveldb.
 * Exposing metrics.
 * Testing it in IPFS, libp2p, and Lotus, and characterising its behaviour,
   performance and footprint under real, practical workloads.

## Contribute

Feel free to join in. All welcome. Open an [issue](https://github.com/ipfs/go-ds-pebble/issues)!

This repository falls under the IPFS [Code of Conduct](https://github.com/ipfs/community/blob/master/code-of-conduct.md).

### Want to hack on IPFS?

[![](https://cdn.rawgit.com/jbenet/contribute-ipfs-gif/master/img/contribute.gif)](https://github.com/ipfs/community/blob/master/CONTRIBUTING.md)

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/test-vectors/blob/master/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/test-vectors/blob/master/LICENSE-APACHE)
