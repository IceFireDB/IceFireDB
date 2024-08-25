# go-ds-crdt

> A distributed [go-datastore](https://github.com/ipfs/go-datastore)
> implementation using Merkle-CRDTs.

`go-ds-crdt` is a key-value store implementation using Merkle CRDTs, as
described in
[the paper by Héctor Sanjuán, Samuli Pöyhtäri and Pedro Teixeira](https://arxiv.org/abs/2004.00107).
It satisfies the
[`Datastore`](https://pkg.go.dev/github.com/ipfs/go-datastore#Datastore)
and [`Batching`](https://pkg.go.dev/github.com/ipfs/go-datastore#Batching)
interfaces from `go-datastore`.

This means that you can create a network of nodes that use this datastore, and 
that each key-value pair written to it will automatically replicate to every
other node. Updates can be published by any node. Network messages can be dropped, 
reordered, corrupted or duplicated. It is not necessary to know beforehand
the number of replicas participating in the system. Replicas can join and leave 
at will, without informing any other replica. There can be network partitions 
but they are resolved as soon as connectivity is re-established between replicas.

Internally it uses a delta-CRDT Add-Wins Observed-Removed set. The current
value for a key is the one with highest priority. Priorities are defined as
the height of the Merkle-CRDT node in which the key was introduced.

Implementation is independent from Broadcaster and DAG syncer layers, although the 
easiest is to use out of the box components from the IPFS stack (see below).

## Performance

Using batching, Any `go-ds-crdt` replica can easily process and sync 400 keys/s at least. The largest known deployment has 100M keys.

`go-ds-crdt` is used in production as state-synchronization layer for [IPFS Clusters](https://ipfscluster.io).

## Usage

`go-ds-crdt` needs:
  * A user-provided, thread-safe,
    [`go-datastore`](https://github.com/ipfs/go-datastore) implementation to
    be used as permanent storage. We recommend using the
    [Badger implementation](https://pkg.go.dev/github.com/ipfs/go-ds-badger).
  * A user-defined `Broadcaster` component to broadcast and receive updates
    from a set of replicas. If your application uses
    [libp2p](https://libp2p.io), you can use
    [libp2p PubSub](https://pkg.go.dev/github.com/libp2p/go-libp2p-pubsub) and
    the provided
    [`PubsubBroadcaster`](https://pkg.go.dev/github.com/ipfs/go-ds-crdt?utm_source=godoc#PubSubBroadcaster).
  * A user-defined "DAG syncer" component ([`ipld.DAGService`](https://pkg.go.dev/github.com/ipfs/go-ipld-format?utm_source=godoc#DAGService)) to publish and
    retrieve Merkle DAGs to the network. For example, you can use
    [IPFS-Lite](https://github.com/hsanjuan/ipfs-lite) which casually
    satisfies this interface.

The permanent storage layout is optimized for KV stores with fast indexes and
key-prefix support.

See https://pkg.go.dev/github.com/ipfs/go-ds-crdt for more information.

## Captain

This project is captained by @hsanjuan.

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2019. Protocol Labs, Inc.

