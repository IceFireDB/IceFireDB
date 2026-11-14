# Client-side optimizations

This document reflects client-side optimizations that are implemented in this repository. Client-side optimizations are not part of the [Kademlia spec](https://github.com/libp2p/specs/tree/master/kad-dht), and are not required to be implemented on all clients.

## Checking before Adding

A Kademlia server should try to add remote peers querying it to its routing table. However, the Kademlia server has no guarantee that remote peers issuing requests are able to answer Kademlia requests correctly, even though they advertise speaking the Kademlia server protocol. It is important that only server nodes able to answer Kademlia requests end up in other peers' routing tables. Hence, before adding a remote peer to the Kademlia server's routing table, the Kademlia server will send a trivial `FIND_NODE` request to the remote peer, and add it to its routing table only if it is able to provide a valid response.

## Bounding peer record size

Kademlia messages carry _peer records_: a peer ID together with the multiaddresses at which that peer can be reached. They appear as the closer peers in a `FIND_NODE` or `GET_PROVIDERS` response, and as the provider records in an `ADD_PROVIDER` request. A peer record only needs an identity and enough addresses to be dialable, so this implementation bounds a single peer record to 8 KiB when serializing it, keeping the peer ID and as many addresses as fit and dropping the rest. The 8 KiB ceiling matches the budget the libp2p identify protocol allows for a peer's entire signed self-description, so it is comfortably larger than any well-formed peer requires.

The bound is applied both to the records this node emits and to the records it ingests from other peers, keeping records a predictable size on the wire and in the peerstore. It requires no coordination between peers: a trimmed record is an ordinary, smaller peer record, which every Kademlia implementation already accepts.

## Routing table persistence

A node saves the peers in its routing table, with their addresses, to its datastore periodically and when it shuts down. The next time its routing table is empty, for example after a restart, it can dial these saved peers as well as its bootstrap peers: before them, only if the bootstrap peers fail, or at the same time, depending on configuration. Dialing them first lets an established node rejoin the network through peers it already knew, instead of going through a handful of bootstrap nodes on every restart. The bootstrap peers stay the fallback for a node without a snapshot, or whose saved peers are gone. A node without bootstrap peers dials only its saved peers. A much smaller routing table, such as one that failing queries shrank during an outage, doesn't replace a snapshot.

This requires no coordination between peers: to a dialed peer, a node reconnecting from its snapshot is an ordinary incoming connection. Saved peers are only ever dialed, never inserted into the routing table directly. Each one rejoins the routing table the way any peer does: after identify and the check described in [Checking before Adding](#checking-before-adding), and within the IP diversity limits where those are configured, as on the public Amino DHT.
