# Client-side optimizations

This document reflects client-side optimizations that are implemented in this repository. Client-side optimizations are not part of the [Kademlia spec](https://github.com/libp2p/specs/tree/master/kad-dht), and are not required to be implemented on all clients.

## Checking before Adding

A Kademlia server should try to add remote peers querying it to its routing table. However, the Kademlia server has no guarantee that remote peers issuing requests are able to answer Kademlia requests correctly, even though they advertise speaking the Kademlia server protocol. It is important that only server nodes able to answer Kademlia requests end up in other peers' routing tables. Hence, before adding a remote peer to the Kademlia server's routing table, the Kademlia server will send a trivial `FIND_NODE` request to the remote peer, and add it to its routing table only if it is able to provide a valid response.