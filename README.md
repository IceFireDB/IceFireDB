# IceFireDB  - [ DataBase for web3 standing on IPFS  ]


<p align="center">
<img 
    src="https://raw.githubusercontent.com/IceFireDB/IceFireDB/main/icefiredb-bridge.png" 
     alt="icefiredb-bridge">
</p>


- [IceFireDB  - \[ DataBase for web3 standing on IPFS  \]](#icefiredb-----database-for-web3-standing-on-ipfs--)
- [Project Description](#project-description)
- [Project value](#project-value)
- [Project composition](#project-composition)
  - [IceFireDB-NoSQL](#icefiredb-nosql)
  - [IceFireDB-SQLite](#icefiredb-sqlite)
  - [IceFireDB-SQLProxy](#icefiredb-sqlproxy)
  - [IceFireDB-Redis-Proxy](#icefiredb-redis-proxy)
  - [IceFireDB-PubSub](#icefiredb-pubsub)
- [Some Q&A](#some-qa)
  - [Would you be able to write IPLD Schemas and specs for the data structures you're using? This would allow others to implement writers/readers for their data.](#1-would-you-be-able-to-write-ipld-schemas-and-specs-for-the-data-structures-youre-using-this-would-allow-others-to-implement-writersreaders-for-their-data)
  - [How does the DB run?](#2-how-does-the-db-run)
  - [How will you address mutability of data?](#3-how-will-you-address-mutability-of-data)
  - [What program languages are you targeting?](#4-what-program-languages-are-you-targeting)
- [Thanks supports](#thanks-supports)


# Project Description

IPFS and Filecoin are excellent decentralized data storage infrastructures, which have revolutionary significance for the construction of web3. However, with the development of the application ecology, archived data such as pictures and videos can be stored in IPFS or FileCoin, but there is still a lack of database-level storage expression. Although the community also has a decentralized storage solution similar to the KV model, it is only the KV model. It cannot meet the use of upper-layer applications. We believe that more and more complex data structures need to be supported to support the decentralized development of applications, including NoSQL data models (such as hash, list, set data structures) and SQL data models. Just like the prosperity of web2's application ecology is inseparable from the contribution of database infrastructures such as memcached, redis, and mysql, the IPFS ecosystem also needs database infrastructure with NoSQL and SQL models.

Apart from storage, IPFS has many excellent components in the ecology, such as libp2p, crdt, ipfs-log and ipld. These components will greatly support the decentralization of data and the ecology of web2. However, since many databases under web2 are mysql and redis at present, if you want to help the applications using these databases achieve decentralization, you need to design more database middleware to facilitate application docking and connect the preceding and the following.

In addition to the huge demand for decentralized data storage in the web3 ecosystem, in the current digital age, decentralized networking, decentralized subscriptions, and decentralized storage are used in scenarios such as edge computing, big data, and cloud native. There are more and more strong demands. We combine the demands of web2 and the current situation of web3 to propose the IceFireDB Storage Stack project.

IceFireDB Storage Stack is committed to creating a complete database storage and database middleware software system for the data decentralization ecosystem. The project currently mainly includes three directions of development: decentralized NoSQL database, decentralized SQLite database, decentralized communication Components and database decentralized middleware (ecological application communication middleware, database decentralized middleware).

Let's elaborate on the three main construction directions of IceFireDB Storage Stack:

1. Decentralized NoSQL database (IceFireDB-NoSQL): Using IPFS as the underlying KV engine, using KV coding technology to achieve more complex data structures, such as hash, set, list, etc., integrate the standard Redis network protocol, allowing applications The IceFIreDB-IPFS-NoSQL database can be used by using the Redis client; the network networking is performed by using libp2p, and the decentralized NoSQL database is constructed by crdt, ipfs-log, and ipld.
2. Decentralized SQLite database (IceFireDB-SQLite): The SQL protocol is currently widely used in the web2 application layer. We design and implement a decentralized SQLite database. The bottom layer uses IPFS-libp2p to build a decentralized network and IPFS-pubsub and peer-to-peer Wait for nodes to synchronize data, and use IPFS CRDT, ipfs-log, and ipld to ensure decentralized consistency of SQL statements.
3. Decentralized database middleware (IceFireDB-PubSub, IceFireDB-Redis-Proxy, IceFireDB-SQLProxy): While we are paying attention to the application development of web3, we also see that IPFS provides data decentralization for applications around the world. Very good libp2p, crdt and other components, we should provide decentralized capabilities for traditional web2 databases and traditional web2 applications, but most web2 applications currently use redis, mysql databases, IceFireDB combined with libp2p, crdt, ipld, ipfs-log The technology adds the wings of data decentralization to traditional mysql and redis, and provides insensitive data decentralization and application decentralization communication capabilities for massive web2 applications, traditional nosql and SQL databases.

# Project value
<p align="center">
<img 
    src="https://raw.githubusercontent.com/IceFireDB/IceFireDB/main/imgs/project_purpose.png" 
     alt="project_purpose">
</p>

For the web3 world, there is currently no way to write a full-blown client-side application as easily and completely decentralized as in Web2. In Web2, you spin up a database on AWS and have your client applications call that database for reading and writing. But there is nothing like that in Web3. You can't just write data to Ethereum, it's too expensive for most users. Storage protocols such as Filecoin and Arweave are mainly used for archiving data, but do not provide enterprise-level performance guarantees for writing and reading data. IceFireDB Storage Stack uses IPFS as the underlying KV engine, and uses KV encoding technology to achieve more complex NoSQL data structures, For example, hash, set, list, etc. use libp2p, crdt, ipld, ipfs-log to build decentralized NoSQL and SQL databases, and provide RESP and SQL protocol support for easy application access, so that existing massive applications can be changed Solve the decentralized communication and storage capabilities of IPFS at the lowest cost, and expand the application access speed and application ecology of IPFS and Filecoin.

For the traditional application fields of web2, most applications currently use Nosql and SQL databases such as redis and mysql. However, with the development of edge computing, big data, cloud native and other fields, these fields also need the support of decentralized network communication and decentralized database storage. We should provide decentralized nosql database and decentralized SQL database to provide decentralized database storage and use support for these applications. It is also necessary to provide decentralized database middleware to increase the capabilities of data broadcast communication and decentralized data storage for traditional redis and mysql databases. The decentralized database, decentralized networking subscription and decentralized database middleware provided by IceFireDB Storage Stack will help IPFS and Filecoin ecology to support decentralized web2 massive applications.

Database technology is the foundation of application storage and application innovation. The development of decentralized application ecology in any era is inseparable from the support of databases and database middleware. IceFireDB Storage Stack is the wings that add data decentralization to applications. It is believed that IceFireDB Storage Stack will be able to support a large number of new applications.

IceFireDB Storage Stack has been striving to build decentralized NoSQL\SQL databases and database middleware with richer functions and easier access to application systems based on the decentralized network and decentralized storage technologies of IPFS and Filecoin, helping massive applications to improve Easy access to network decentralization and data decentralization technologies, helping the decentralized application ecosystem to build storage infrastructure, and helping the prosperity of IPFS, Filecoin and the decentralized application ecosystem.

# Project composition

## [IceFireDB-NoSQL](https://github.com/IceFireDB/IceFireDB)
The Redis database based on IPFS technology can break the simple kv situation of the current IPFS database and support complex data structures such as hash and list.

## [IceFireDB-SQLite](https://github.com/IceFireDB/IceFireDB/tree/main/IceFireDB-SQLite)
IceFireDB-SQLite database is a decentralized SQLite database. Provide a convenient mechanism to build a global distributed database system. Support users to write data to IceFireDB-SQLite using MySQL protocol. IceFireDB-SQLite stores the data in the SQLite database and synchronizes the data among the nodes in the P2P automatic network.

## [IceFireDB-SQLProxy](https://github.com/IceFireDB/IceFireDB/tree/main/IceFireDB-SQLProxy)

IceFireDB-SQLProxy is a decentralized SQL database networking system that helps web2 traditional SQL database data decentralization. Provide a convenient mechanism to build a globally distributed storage system with automatic networking. Commands are automatically synchronized between IceFireDB-SQLProxy in the network, and each IceFireDB-SQLProxy writes data to MySQL storage.

Decentralized networking through IceFireDB-SQLProxy provides web2 program read and write support for SQL, enabling decentralized data synchronization for MySQL database read and write scenarios commonly used in web2 applications.

## [IceFireDB-Redis-Proxy](https://github.com/IceFireDB/IceFireDB/tree/main/IceFireDB-Redis-Proxy)

IceFireDB-Redis-proxy database proxy adds decentralization wings to traditional redis databases. Provide a convenient mechanism to build a globally distributed storage system with automatic networking. The instructions are automatically synchronized between the networked redis agents, and the redis agent writes data to the cluster or single-point redis storage. Through the decentralized middleware network proxy, decentralized data synchronization can be enabled for the Redis database commonly used in web2 applications.

## [IceFireDB-PubSub](https://github.com/IceFireDB/IceFireDB/tree/main/IceFireDB-PubSub)

IceFireDB-PubSub is a high performance, high availability and decentralized subscription system.It can seamlessly migrate web2 applications using redis publish and subscribe into a decentralized p2p subscription network.

# Some Q&A
## 1. Would you be able to write IPLD Schemas and specs for the data structures you're using? This would allow others to implement writers/readers for their data.
Our current database underlying data storage engine implementation is divided into two categories:

- The first kind: build database synchronization technology based on RESP instructions and SQL statements, which can only grow and cannot be tampered with, and realize data synchronization and consistency guarantee between nodes based on decentralized instruction broadcasting and storage instruction playback.
- Type two: realize KV storage engine based on IPFS kv, and build rich nosql data structure by coding kv-value, so that the data of database can grow on ipfs completely, instead of relying on ipfs-log function.

At present, the first type of database is mainly implemented. We mainly use ipfs-log, an immutable and conflict-free replication data model for distributed systems. Based on ipfs-log, the kv engine is abstracted. Based on this kv engine, it is one of the storage drivers of IceFireDB. Other projects can rely on this kv engine to implement their own writers and readers, but we have not designed a proprietary IPLD data structure at present.

IceFireDB abstracts a data coding layer on the kv engine, which can support more complex data structures such as hash\list besides the basic KV operation. Currently, ipfs kv storage has been realized in the data storage directory of Icefire DB, and we hope that the data of the database can be fully grown on ipfs, but at present, we need to solve the need of kv key synchronization first. The expression of nosql and sql data relationship for ipfs kv is also conceiving the design of ipld, but it is not in the current major construction milestone. ipld is what we have been learning recently,If more complicated IPLD design is involved later, we will practice it in the process of learning.

##  2. How does the DB run? 

**In this milestone, our implementation mode is the server running mode, which needs to run in a server somewhere.** Our server will support Redis-RESP protocol and MYSQL communication protocol, and clients can use Golang, JS, PHP and other computer languages to connect. 


![image](https://user-images.githubusercontent.com/34047788/188294875-3e4fff36-1ec7-4e50-bb41-64580c1e9c62.png)


**There is also a framework way, which allows users to directly embed into the application code.** This integration method is not in the current milestone, and we plan to concentrate on realizing it after this milestone.For example:[redhub](https://github.com/IceFireDB/redhub),[IceFireDB-crdt-kv](https://github.com/IceFireDB/IceFireDB-crdt-kv),[ipfs-nosql-frame](https://github.com/IceFireDB/ipfs-nosql-frame).These projects are open sourced by IceFireDB and are licensed under the Apache-2.0 open source license, and are mainly used to build ipfs-nosql-frame project, so that other Golang applications can be integrated more conveniently.

Compared with the framework mode, the server operation mode has the following advantages:
 * Provide standard data protocols (RESP, MYSQL), which can make the application minimize changes and use decentralized database.
 * Mask the complexity under IPFS, including libp2p, ipfs-log and crdt.

## 3. How will you address mutability of data? 
We know the immutability of IPFS itself, and now IceFireDB has two implementation models to solve the variability of data:

**The first implementation: instruction broadcast model based on ipfs-log**: Based on ipfs-log,crdt and libp2p(pubsub), an immutable and operation-based conflict-free replication data model for distributed systems is implemented. Based on ipfs-log, various data structures such as event and kv are encapsulated, and multi-node database instruction broadcast is implemented based on this engine;At that bottom of IceFireDB, we abstract the variable kv engine base on badgerdb and leveldb. any node will broadcast the whole network when it is writing instruction, and the bottom driver of IceFireDB of each node will execute the broadcast instruction to ensure the final consistency of data.

![image](https://user-images.githubusercontent.com/34047788/188294894-dc036304-a7cd-4b9d-bdd6-bbdcf1c87017.png)


**The second implementation: full storage model based on ipfs**: In addition to the first implementation mode, we are also building the structure of the second type of data, so that the complete data will grow on ipfs. At first, there is an ipfs driver in the IceFireDB driver layer, which will encode and process the upper-level commands into a unified kv data structure, store and change the value, and the generated new cid will be connected with key. However, at present, there is no key broadcast network with multiple nodes and data synchronization. When we connect with the broadcast network, we can build a data model originally grown on ipfs.

![image](https://user-images.githubusercontent.com/34047788/188294904-99a83a0e-d9ca-46e8-97a7-e57ca1246ca4.png)


## 4. What program languages are you targeting? 

Our program implementation is Golang.

We provide standard Redis-RESP and MYSQL communication protocols, so Redis and MYSQL clients of other computer programming languages can communicate with IceFireDB (JS, Rust)

## Additional Information

**IceFireDB has a good download volume, we think we have done it right, thank you for the support of the hackathon, and hope that more interested partners will participate in the open source construction.**

![image](https://user-images.githubusercontent.com/34047788/188555783-795a473c-a05f-414e-a4e2-53a22b8717c8.png)

# Thanks supports

<table>
  <tr>
    <td align="center"><a href="https://protocol.ai/"><img src="https://user-images.githubusercontent.com/34047788/188373221-4819fd05-ef2f-4e53-b784-dcfffe9c018c.png" width="100px;" alt=""/><br /><sub><b>Protocol Labs</b></sub></a></td>
    <td align="center"><a href="https://filecoin.io/"><img src="https://user-images.githubusercontent.com/34047788/188373584-e245e0bb-8a3c-4773-a741-17e4023bde65.png" width="100px;" alt=""/><br /><sub><b>FileCoin</b></sub></a></td>
  </tr>
</table>
