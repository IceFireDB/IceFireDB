<p align="center">
<img 
    src="logo.png" 
    height="200" border="0" alt="IceFireDB">
</p>

# IceFireDB - Decentralized Database Infrastructure
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/IceFireDB/IceFireDB)
![test](https://github.com/IceFireDB/IceFireDB/actions/workflows/test.yml/badge.svg)
![build](https://github.com/IceFireDB/IceFireDB/actions/workflows/build.yml/badge.svg)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FIceFireDB%2FIceFireDB.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FIceFireDB%2FIceFireDB?ref=badge_shield)

- [IceFireDB - Decentralized Database Infrastructure](#icefiredb---decentralized-database-infrastructure)
- [Decentralized database engine](#decentralized-database-engine)
- [Documentation center](#documentation-center)
- [Project composition](#project-composition)
  - [IceFireDB-SQLite](#icefiredb-sqlite)
  - [IceFireDB-SQLProxy](#icefiredb-sqlproxy)
  - [IceFireDB-Redis-Proxy](#icefiredb-redis-proxy)
  - [IceFireDB-PubSub](#icefiredb-pubsub)
  - [IceFireDB-NoSQL](#icefiredb-nosql)
  - [NoSQL Command support](#nosql-command-support)
- [System Design](#system-design)
- [Quick Start](#quick-start)
- [Performance](#performance)
- [Project direction](#project-direction)
- [Thanks support](#thanks-support)



Based on cutting-edge computer science, the IceFireDB project integrates new ideas and research in the fields of message passing, event instruction processing, data consistency and replication, decentralized network, reliable data storage, high-performance network framework, etc.The team of researchers and engineers at IceFireDB combines the cutting-edge ideas from distributed systems and concurrent databases. These ideas combine collision-free replication data types (CRDT) and decentralized appendix-only logs, which can be used to simulate the variable sharing state between peers in P2P applications to create a new data infrastructure with high security, high performance and low latency.

The core of IceFireDB architecture is geographically distributed event source and decentralized Log source, with log-level CRDT replication.In order to realize the consistency of replication, IceFireDB provides a stable decentralized networking model, which allows the combination of public networks among different sites. Multiple IceFireDB nodes can be run inside each site, and RAFT network can be formed between nodes, which ensures the data consistency and stable storage within the same site.

![image](https://github.com/user-attachments/assets/b706f093-7d71-4253-94b7-0ecb55bfbcef)

# Decentralized database engine

<p align="center">
<img 
    src="https://github.com/user-attachments/assets/cce03ccd-b6e9-427e-a492-51637db04991" 
     alt="icefiredb-bridge">
</p>

IceFireDB is a database built for web3 and web2. The core mission of the project is to help applications quickly achieve decentralization and data immutability. At present, the storage layer supports various storage methods such as disk, OSS, and IPFS. The protocol layer currently supports SQL and RESP protocols, and will support GraphQL protocols in the future.A blockchain fusion layer based on immutable transparent logs and Ethereum is under construction to support integration with higher-level decentralized computing platforms and applications as well as identity, financial assets, intellectual property and sidechain protocols. IceFireDB strives to fill the gap of the decentralized stack, making the data ecology of web3 applications more complete, and making it easier for web2 applications to achieve decentralization and data immutability.


<p align="center">
<img 
    src="https://github.com/user-attachments/assets/9e35731b-fea1-4e11-88a1-089323a7f2aa"
     alt="project_purpose">
</p>


| Feature                                                                                                    | Status                        |
|------------------------------------------------------------------------------------------------------------|-------------------------------|
| High performance                                                                                           | Ongoing optimization           |
| Support LSM disk, OSS, IPFS underlying storage                                                             | Implemented                   |
| Distributed consistency (support Raft, p2p-crdt, IPFS-log modes)                                           | Implemented                   |
| Based on IPFS decentralized storage, build a persistent data distributed storage layer                     | Beta version                  |
| Support P2P automatic networking, build decentralized NoSQL and SQL data synchronization                   | Beta version                  |
| Support decentralized consistent KV storage engine ([icefiredb-crdt-kv](https://github.com/IceFireDB/icefiredb-crdt-kv) and [icefiredb-ipfs-log](https://github.com/IceFireDB/icefiredb-ipfs-log)) | Beta version                  |
| Support AI vector database storage and instructions                                                        | In progress                         |
| Integrate with [NATS](https://nats.io/) for high-performance decentralized networking                      | In progress                  |
| Supports scalable, auditable, and high-performance tamper-resistant logging (combined with [qed](https://github.com/BBVA/qed)) | Planned                    |
| Build an immutable transparent log witness layer between Web2 and Web3, build a data hot and cold hybrid structure, and an immutable data bridge layer (inspired by Google Trillian-Witness and IPLD) | Planned             |
| Support for tiered hot/cold storage via the `hybriddb` driver                                            | Implemented                |
| More advanced cache implementation, faster LSM persistent storage ([Source](https://dl.acm.org/doi/10.1145/3448016.3452819)) | Planned              |


# Documentation center

<p align="center">
<img 
    src="https://github.com/user-attachments/assets/f127e510-db14-489b-aec4-479688c20e6c" 
     alt="IceFireDB_Architecture">
</p>



[**Documentation center**](https://www.icefiredb.xyz/icefiredb_docs/) : https://www.icefiredb.xyz/icefiredb_docs

# Project composition

## [IceFireDB-SQLite](https://github.com/IceFireDB/IceFireDB/tree/main/IceFireDB-SQLite)
IceFireDB-SQLite database is a decentralized SQLite database. Provide a convenient mechanism to build a global distributed database system. Support users to write data to IceFireDB-SQLite using MySQL protocol. IceFireDB-SQLite stores the data in the SQLite database and synchronizes the data among the nodes in the P2P automatic network.

## [IceFireDB-SQLProxy](https://github.com/IceFireDB/IceFireDB/tree/main/IceFireDB-SQLProxy)

IceFireDB-SQLProxy is a decentralized SQL database networking system that helps web2 traditional SQL database data decentralization. Provide a convenient mechanism to build a globally distributed storage system with automatic networking. Commands are automatically synchronized between IceFireDB-SQLProxy in the network, and each IceFireDB-SQLProxy writes data to MySQL storage.

Decentralized networking through IceFireDB-SQLProxy provides web2 program read and write support for SQL, enabling decentralized data synchronization for MySQL database read and write scenarios commonly used in web2 applications.

## [IceFireDB-Redis-Proxy](https://github.com/IceFireDB/IceFireDB/tree/main/IceFireDB-Redis-Proxy)

IceFireDB-Redis-proxy database proxy adds decentralization wings to traditional redis databases. Provide a convenient mechanism to build a globally distributed storage system with automatic networking. The instructions are automatically synchronized between the networked redis agents, and the redis agent writes data to the cluster or single-point redis storage. Through the decentralized middleware network proxy, decentralized data synchronization can be enabled for the Redis database commonly used in web2 applications.

## [IceFireDB-PubSub](https://github.com/IceFireDB/IceFireDB/tree/main/IceFireDB-PubSub)

IceFireDB-PubSub is a high performance, high availability and decentralized subscription system.It can seamlessly migrate web2 applications using redis publish and subscribe into a decentralized p2p subscription network.

## [IceFireDB-NoSQL](https://github.com/IceFireDB/IceFireDB)
It supports distributed raft disk Redis database mode in web2 mode, and also supports decentralized IPFS storage mode.

## NoSQL Command support

| Strings  | Hashes |Lists | Sets |Sorted Sets |
| ------------- | ------------- | ------------- |------------- |------------- |
| APPEND  | HSET|RPUSH | SADD| ZADD|
| BITCOUNT | HGET |LPOP  | SCARD| ZCARD|
| BITOP | HDEL |LINDEX  | SDIFF| ZCOUNT|
| BITPOS | HEXISTS |LPUSH  | SDIFFSTORE| ZREM|
| DECR| HGETALL |RPOP | SINTER| ZCLEAR|
| DECRBY | HINCRBY |LRANGE  | SINTERSTORE| ZRANK|
| DEL | HKEYS |LSET  | SISMEMBER| ZRANGE|
| EXISTS  | HLEN  |LLEN  | SMEMBERS| ZREVRANGE|
| GET | HMGET|RPOPLPUSH  | SREM| ZSCORE|
| GETBIT | HMSET |LCLEAR  | SUNION|ZINCRBY |
| SETBIT | HSETEX  |LCLEAR  | SUNIONSTORE| ZREVRANK|
| GETRANGE| HSTRLEN  |LMCLEAR  | SCLEAR|ZRANGEBYSCORE |
| GETSET | HVALS  |LEXPIRE  |SMCLEAR | ZREVRANGEBYSCORE|
| INCR | HCLEAR  |LEXPIREAT  |SEXPIRE | ZREMRANGEBYSCORE|
| EXISTS  | HMCLEAR  |LKEYEXISTS  | SEXPIRE| ZREMRANGEBYRANK|
| GET | HEXPIRE |LTRIM  |SEXPIREAT | |
| GETBIT | HEXPIREAT |LTTL  |STTL | |
| SETBIT | HKEYEXIST |  |SPERSIST | |
| GETRANGE| HTTL  |   |SKEYEXISTS | |
| GETSET |   |  | | |
| INCRBY |   |  | | |
| GET |  |  | | |
| MGET |  |  | | |
| MSET |   |  | | |
| SET|   |  | | |
| SETEX |   |  | | |
| SETEXAT |   |  | | |
| SETRANGE |   |  | | |
| EXPIRE|   |  | | |
| EXPIREAT |   |  | | |
| TTL |   |  | | |

# System Design
**IceFireDB refines and implements the following important system components.**

| System components | describe | technology used |
| ------------- | ------------- | ------------- |
| **[Network layer](https://www.icefiredb.xyz/icefiredb_docs/icefiredb/icefiredb-nosql/designs/network/)**  |  1. RAFT guarantees data consistency within a single availability zone. <br />2. P2P network construction decentralized database communication. <br />3. NATS is a new network layer being built.  |P2P、RAFT、NATS  |
| **[Storage layer](https://www.icefiredb.xyz/icefiredb_docs/icefiredb/icefiredb-nosql/designs/storage/)** | Many types of storage are currently supported. Under the codec computing layer, we abstract the KV storage driver layer, which is compatible with different storage engines of web2 and web3.  |goleveldb、badger、hybriddb、IPFS、CRDT、IPFS-LOG、IPFS-SYNCKV、OSS  |
| **[Protocol layer](https://www.icefiredb.xyz/icefiredb_docs/icefiredb/icefiredb-nosql/designs/protocol/)**| Based on the codec layer, we have built a protocol layer. A good communication protocol allows more applications to easily access the IceFireDB data network. Currently, we support the Redis-RESP NoSQL protocol and the MySQL protocol.  |RESP、SQL |
| **[Codec layer](https://www.icefiredb.xyz/icefiredb_docs/icefiredb/icefiredb-nosql/designs/codec/)**| The codec layer is the core of our system. For NoSQL scenarios, any data type will be abstracted into a KV storage model. With the flexible coding layer, we can build rich data operation structures and instructions, such as hash, sets, strings, etc.  |KV、Strings、Hashes、Lists、Sorted Sets、Sets、SQL、PubSub  |


# Quick Start 

[https://www.icefiredb.xyz/icefiredb_docs/icefiredb/icefiredb-nosql/quick_start/](https://www.icefiredb.xyz/icefiredb_docs/icefiredb/icefiredb-nosql/quick_start/)


# Performance 

**leveldb driver**

```shell
corerman@ubuntu:~/DATA/ICODE/GoLang/IceFireDB$ redis-benchmark  -h 127.0.0.1 -p 11001 -n 10000000 -t set,get -c 512 -P 512 -q

SET: 253232.12 requests per second
GET: 2130875.50 requests per second
```

# Project direction

IceFireDB originated from the distributed NoSQL database in the web2 scenario. We will continue to support the web2 distributed NoSQL database, while investing more energy in the direction of web3 and web2 decentralized databases. We are very grateful to our community partners for their continued interest, the community has been our driving force.

# Thanks support

 **I stood on the shoulders of giants and did only simple things. Thank you for your attention.**

* https://github.com/tidwall/uhaha
* https://github.com/syndtr/goleveldb
* https://github.com/dgraph-io/ristretto
* https://github.com/ledisdb/ledisdb
* https://github.com/dgraph-io/badger
* https://github.com/ipfs/ipfs
* https://github.com/libp2p


<table>
  <tr>
    <td align="center"><a href="https://protocol.ai/"><img src="https://user-images.githubusercontent.com/34047788/188373221-4819fd05-ef2f-4e53-b784-dcfffe9c018c.png" width="100px;" alt=""/><br /><sub><b>Protocol Labs</b></sub></a></td>
    <td align="center"><a href="https://filecoin.io/"><img src="https://user-images.githubusercontent.com/34047788/188373584-e245e0bb-8a3c-4773-a741-17e4023bde65.png" width="100px;" alt=""/><br /><sub><b>Filecoin</b></sub></a></td>
  <td align="center"><a href="https://fvm.filecoin.io/"><img src="https://user-images.githubusercontent.com/34047788/220075045-48286b37-b708-4ecf-94f5-064c55e79fa3.png" width="110px;" alt="FVM"/><br /><sub><b>FVM</b></sub></a></td>
  <td align="center"><a href="https://libp2p.io/"><img src="https://github.com/IceFireDB/.github/assets/34047788/36e39958-76ad-4b3a-96e1-1614e87ac1a3" width="100px;" alt="libp2p"/><br /><sub><b>libp2p</b></sub></a></td>  
</tr>
</table>


**License**

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FIceFireDB%2FIceFireDB.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FIceFireDB%2FIceFireDB?ref=badge_large)


**Disclaimers**

When you use this software, you have agreed and stated that the author, maintainer and contributor of this software are not responsible for any risks, costs or problems you encounter. If you find a software defect or BUG, please submit a patch to help improve it!
