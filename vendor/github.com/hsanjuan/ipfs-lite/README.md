# IPFS-Lite

<p align="center">
<img src="logo.png" alt="ipfs-lite" title="ipfs-lite" />
</p>

[![Build Status](https://github.com/hsanjuan/ipfs-lite/actions/workflows/go.yml/badge.svg)](https://github.com/hsanjuan/ipfs-lite/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/hsanjuan/ipfs-lite.svg)](https://pkg.go.dev/github.com/hsanjuan/ipfs-lite)

IPFS-Lite is an embeddable, lightweight IPFS peer which runs the minimal setup
to provide an `ipld.DAGService` and UnixFS-files addition and retrieval.

It can:

* Add, Get, Remove IPLD Nodes to/from the IPFS Network (remove is a local blockstore operation).
* Add single files (chunk, build the DAG and Add) from a `io.Reader`.
* Get single files given a their CID.

It needs:

* A [libp2p Host](https://pkg.go.dev/github.com/libp2p/go-libp2p#New)
* A [libp2p DHT](https://pkg.go.dev/github.com/libp2p/go-libp2p-kad-dht#New)
* A [datastore](https://pkg.go.dev/github.com/ipfs/go-datastore), such as [BadgerDB](https://pkg.go.dev/github.com/ipfs/go-ds-badger), [go-ds-flatfs](https://pkg.go.dev/github.com/ipfs/go-ds-flatfs) or an [in-memory](https://pkg.go.dev/github.com/hsanjuan/ipfs-lite#NewInMemoryDatastore) one.

Some helper functions are provided to
[initialize these quickly](https://pkg.go.dev/github.com/hsanjuan/ipfs-lite#SetupLibp2p).

It provides:

* An [`ipld.DAGService`](https://pkg.go.dev/github.com/ipfs/go-ipld-format#DAGService).
* An [`AddFile` method](https://pkg.go.dev/github.com/hsanjuan/ipfs-lite#Peer.AddFile) to add content from a reader.
* A [`GetFile` method](https://pkg.go.dev/github.com/hsanjuan/ipfs-lite#Peer.GetFile) to get a file from IPFS.

The goal of IPFS-Lite is to run the **bare minimal** functionality for any
IPLD-based application to interact with the IPFS Network by getting and
putting blocks to it, rather than having to deal with the complexities of
using a full IPFS daemon, and with the liberty of sharing the needed libp2p
Host and DHT for [other things](https://github.com/ipfs/go-ds-crdt).

## License

Apache 2.0
