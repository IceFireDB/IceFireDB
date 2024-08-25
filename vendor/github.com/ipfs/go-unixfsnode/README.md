# go-unixfsnode

This is an IPLD ADL that provides string based pathing for protobuf nodes. The top level node behaves like a map where LookupByString returns the Hash property on the Link in the protobufs list of Links whos Name property matches the key. This should enable selector traversals that work based of paths.

Note that while it works internally with go-codec-dagpb, the Reify method (used to get a UnixFSNode from a DagPB node should actually work successfully with go-ipld-prime-proto nodes)

## Usage

The primary interaction with this package is to register an ADL on a link system. This is done with via a helper method.

```go
AddUnixFSReificationToLinkSystem(lsys *ipld.LinkSystem)
```

For link systems which have UnixFS reification registered, two ADLs will be available to the [`InterpretAs`](https://ipld.io/specs/selectors/) selector: 'unixfs' and 'unixfs-preload'. The different between these two ADLs is that the preload variant will access all blocks within a UnixFS Object (file or directory) when that object is accessed by a selector traversal. The non-preload variant in contrast will only access the subset of blocks strictly needed for the traversal. In practice, this means the subset of a sharded directory needed to access a specific file, or the sub-range of a file directly accessed by a range selector.


## License

Apache-2.0/MIT © Protocol Labs
