# go-libp2p-kad-dht

[![Go Reference](https://pkg.go.dev/badge/github.com/libp2p/go-libp2p-kad-dht.svg)](https://pkg.go.dev/github.com/libp2p/go-libp2p-kad-dht)
[![Go Test](https://github.com/libp2p/go-libp2p-kad-dht/actions/workflows/go-test.yml/badge.svg)](https://github.com/libp2p/go-libp2p-kad-dht/actions/workflows/go-test.yml)

The Go implementation of the libp2p Kademlia DHT, and the implementation behind
the [Amino DHT](https://probelab.io/ipfs/dht/), the public DHT of the IPFS
network.

A distributed hash table (DHT) lets peers find each other, and the content they
store, without a central server. With this module a [libp2p](https://libp2p.io)
node can:

- **find peers**: resolve a peer ID to its addresses,
- **find content**: announce that it provides a CID, and find who provides one,
- **store records**: put and get key-value records that every node checks
  with a validator, such as IPNS records and public keys.

It runs on the public IPFS network and on private networks alike. Projects
built on it include:

- [kubo](https://github.com/ipfs/kubo): the reference IPFS implementation
- [lotus](https://github.com/filecoin-project/lotus): a Filecoin node implementation
- [spegel](https://github.com/spegel-org/spegel): a peer-to-peer container image
  mirror for Kubernetes clusters
- [tau](https://github.com/taubyte/tau): a self-hosted cloud platform
- [berty](https://github.com/berty/berty): a peer-to-peer messenger
- [celestia-node](https://github.com/celestiaorg/celestia-node): Celestia data
  availability nodes

## Install

```sh
go get github.com/libp2p/go-libp2p-kad-dht
```

## Quick start

Start a private two-node DHT on this machine. Alice provides a CID, and Bob,
who joined through Alice, finds her as its provider:

```go
package main

import (
 "context"
 "fmt"
 "log"
 "time"

 "github.com/ipfs/go-cid"
 "github.com/libp2p/go-libp2p"
 dht "github.com/libp2p/go-libp2p-kad-dht"
 "github.com/libp2p/go-libp2p/core/host"
 "github.com/libp2p/go-libp2p/core/peer"
)

// newNode starts a libp2p host and a DHT server on the /example network.
func newNode(bootstrap ...peer.AddrInfo) (host.Host, *dht.IpfsDHT) {
 h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
 if err != nil {
  log.Fatal(err)
 }
 kad, err := dht.New(h,
  dht.ProtocolPrefix("/example"),
  dht.Mode(dht.ModeServer),
  dht.BootstrapPeers(bootstrap...),
 )
 if err != nil {
  log.Fatal(err)
 }
 return h, kad
}

func main() {
 ctx := context.Background()

 alice, aliceDHT := newNode()
 defer alice.Close()
 defer aliceDHT.Close()

 // Bob joins the network through Alice.
 bob, bobDHT := newNode(peer.AddrInfo{ID: alice.ID(), Addrs: alice.Addrs()})
 defer bob.Close()
 defer bobDHT.Close()

 // The DHT dials its bootstrap peers in the background.
 for aliceDHT.RoutingTable().Size() == 0 {
  time.Sleep(100 * time.Millisecond)
 }

 c := cid.MustParse("bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e")
 if err := aliceDHT.Provide(ctx, c, true); err != nil {
  log.Fatal(err)
 }
 fmt.Println("alice", alice.ID(), "provides", c)

 for p := range bobDHT.FindProvidersAsync(ctx, c, 1) {
  fmt.Println("bob found provider", p.ID)
 }
}
```

```console
$ go run .
alice 12D3KooWB1QGdDCnBbuDHckMv71awV4N8fbsMiTQmUHBDtdP4MFZ provides bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e
bob found provider 12D3KooWB1QGdDCnBbuDHckMv71awV4N8fbsMiTQmUHBDtdP4MFZ
```

The `/example` prefix keeps these nodes off the public Amino DHT. Both run in
server mode because loopback addresses are never publicly reachable, so
`ModeAuto` would leave them as clients. See [joining the Amino
DHT](#joining-the-amino-dht) to use the public network instead.

`*dht.IpfsDHT` implements the libp2p
[`routing.Routing`](https://pkg.go.dev/github.com/libp2p/go-libp2p/core/routing#Routing)
interface, so it plugs in anywhere a content or peer router is expected. See the
[API reference](https://pkg.go.dev/github.com/libp2p/go-libp2p-kad-dht) for
`Provide`, `FindPeer`, `PutValue`, `GetValue`, and the rest.

## Packages

| Package                                                                       | Use it for                                                                                |
| ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| [`dht`](https://pkg.go.dev/github.com/libp2p/go-libp2p-kad-dht)               | A DHT node. Start here.                                                                   |
| [`dual`](https://pkg.go.dev/github.com/libp2p/go-libp2p-kad-dht/dual)         | Two DHTs side by side, one for the public internet and one for the local network.         |
| [`provider`](https://pkg.go.dev/github.com/libp2p/go-libp2p-kad-dht/provider) | Announcing and reproviding many CIDs. The sweep provider batches them by keyspace region. |
| [`amino`](https://pkg.go.dev/github.com/libp2p/go-libp2p-kad-dht/amino)       | The protocol ID and parameters of the Amino DHT.                                          |
| [`fullrt`](https://pkg.go.dev/github.com/libp2p/go-libp2p-kad-dht/fullrt)     | Faster lookups by crawling the whole network. Opt-in, for well provisioned nodes only.    |

## Client and server mode

A DHT **server** answers queries and sits in other peers' routing tables. A DHT
**client** only sends queries: it does not advertise the DHT protocol, so no
one routes queries to it.

Only nodes others can dial should be servers, otherwise they fill routing
tables with peers that never answer. By default the DHT works this out itself:

| Mode                     | Behavior                                                                                            |
| ------------------------ | --------------------------------------------------------------------------------------------------- |
| `dht.ModeAuto` (default) | Client until libp2p reports the node publicly reachable, then server. Follows reachability changes. |
| `dht.ModeAutoServer`     | Like `ModeAuto`, but server while reachability is unknown.                                          |
| `dht.ModeServer`         | Always server. Only for nodes known to be publicly reachable.                                       |
| `dht.ModeClient`         | Always client.                                                                                      |

Leave it on `ModeAuto` unless you know better. A reachable home node becomes a
full server, and the network needs those.

## Configuration

`dht.New(host, options...)` takes
[options](https://pkg.go.dev/github.com/libp2p/go-libp2p-kad-dht#Option). The
ones most programs set:

| Option                    | Default    | What it does                                                                                                                             |
| ------------------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `ProtocolPrefix`          | `/ipfs`    | Run a separate DHT network.                                                                                                              |
| `BootstrapPeers`          | none       | Peers to dial when the routing table is empty. Without them the node finds no one. `dht.GetDefaultBootstrapPeerAddrInfos()` joins Amino. |
| `Mode`                    | `ModeAuto` | See [client and server mode](#client-and-server-mode).                                                                                   |
| `Datastore`               | in memory  | Holds records, provider records, and the saved routing table. Use a persistent one to keep them across restarts.                         |
| `RoutingTablePersistence` | 1 hour     | How often the routing table is saved, so a restart reconnects without the bootstrap peers. `0` turns it off.                             |

### Joining the Amino DHT

Amino is the public DHT of the IPFS network, under the `/ipfs` prefix. Join it
only if your node takes part in IPFS: it provides or finds IPFS content, or
resolves IPNS names. Anything else belongs in [its own
network](#running-your-own-network).

To join, keep the default prefix and pass Amino's bootstrap peers:

```go
kad, err := dht.New(h, dht.BootstrapPeers(dht.GetDefaultBootstrapPeerAddrInfos()...))
```

Your node then shares a network with every other Amino implementation, and has
to be a good citizen:

- It speaks the whole protocol: it stores and serves provider records and
  values, and validates `/pk` and `/ipns` records.
- It serves queries only when others can dial it. Keep `ModeAuto` unless you
  know the node is publicly reachable.

`dht.New` enforces the protocol part on `/ipfs` and returns an error for these
options:

| Option                              | Rejected on `/ipfs` when                                           |
| ----------------------------------- | ------------------------------------------------------------------ |
| `BucketSize`                        | set to anything but `amino.DefaultBucketSize`                      |
| `DisableProviders`, `DisableValues` | used at all                                                        |
| `Validator`, `NamespacedValidator`  | the validators are anything but the default `/pk` and `/ipns` ones |

### Running your own network

> [!IMPORTANT]
> An application that needs a DHT should start its own network, not join
> Amino. Amino nodes serve the IPFS network, many of them from home machines,
> and should not carry another application's peers, records, or traffic.

Give your network its own prefix and its own bootstrap peers. Nodes under
different prefixes never talk to each other, so Amino's bootstrap peers are of
no use here.

```go
kad, err := dht.New(h,
 dht.ProtocolPrefix("/myapp"),
 dht.BootstrapPeers(myBootstrapPeers...),
 dht.BucketSize(10),
 dht.DisableValues(),
)
```

Under its own prefix the DHT skips the Amino checks, so every option is open:
bucket size, lookup `Concurrency` and `Resiliency`, record validators, and
turning providers or values off.

## Learn more

- [libp2p Kademlia DHT spec](https://github.com/libp2p/specs/tree/master/kad-dht)
  and [IPFS Kademlia DHT spec](https://specs.ipfs.tech/routing/kad-dht/), which
  adds the Amino rules
- [optimizations.md](./optimizations.md): client-side optimizations specific to
  this implementation.
- [ProbeLab](https://probelab.io/ipfs/dht/): measurements of the live Amino DHT

## Contributing

Bug reports and pull requests are welcome in the
[issue tracker](https://github.com/libp2p/go-libp2p-kad-dht/issues).

This code must stay compatible with every other Kademlia implementation on the
network. Protocol changes start in the specs:
[ipfs/specs](https://github.com/ipfs/specs) for Amino and
[libp2p/specs](https://github.com/libp2p/specs) for base Kademlia.

Maintained by [@guillaumemichel](https://github.com/guillaumemichel).

## License

[MIT](LICENSE) © Protocol Labs, Inc.
