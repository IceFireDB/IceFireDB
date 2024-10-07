# IceFireDB-Redis-Proxy

IceFireDB-Redis-Proxy is a database proxy that adds decentralization capabilities to traditional Redis databases. It provides a convenient mechanism to build a globally distributed storage system with automatic networking. Commands are automatically synchronized between networked Redis agents, and the Redis agent writes data to either a cluster or single-point Redis storage. Through the decentralized middleware network proxy, decentralized data synchronization can be enabled for the Redis databases commonly used in web2 applications.

## Features

1. **Complete Data Source Mode Support**: Stand-alone and cluster modes.
2. **Rich Command Support**: Comprehensive support for Redis commands.
3. **Excellent Cluster State Management and Failover**: Ensures high availability and reliability.
4. **Excellent Traffic Control Strategy**: Includes traffic read-write separation and multi-tenant data isolation.
5. **P2P Automatic Networking**: The agent helps traditional Redis databases achieve data dispersion.

## How It Works

### Scene Introduction
![image](https://user-images.githubusercontent.com/34047788/179436964-00aa6ffd-f6f4-4c60-a46c-fb0165296049.png)

### System Structure
![image](https://user-images.githubusercontent.com/34047788/179439830-e0e4c480-553a-4274-9d12-9afacfcdfe77.png)

## Configuration

In the `config` directory, users store the project configuration file named `config.yaml`, which can be modified according to their needs.

```yaml
# Project proxy configuration
proxy:
  local_port: 16379
  enable_mtls: false

# p2p configuration
p2p:
  enable: true
  service_discovery_id: "p2p_redis_proxy_service_test"
  # pubsub topic
  service_command_topic: "p2p_redis_proxy_service_topic_test"
  # Node Discovery Mode
  service_discover_mode: "advertise" # advertise or announce
...
```

## Quickstart

### Video Tutorial
[![Quickstart Video](https://user-images.githubusercontent.com/52234994/173170991-08713e52-291c-4fae-bf46-ce87b959ce90.mp4)](https://user-images.githubusercontent.com/52234994/173170991-08713e52-291c-4fae-bf46-ce87b959ce90.mp4)

### Running the Proxy
Run the binary file directly. If you need to run it in the background, you can add it to the systemd system management.

```shell
$ make
$ ./bin/IceFireDB-Redis-Proxy -c ./config/config.yaml
```

## Usage

### Supported Commands

#### String
- APPEND
- BITCOUNT
- BITPOS
- DECR
- DECRBY
- DEL
- EXISTS
- GET
- GETBIT
- SETBIT
- GETRANGE
- GETSET
- INCR
- INCRBY
- MGET
- MSET
- SET
- SETEX
- SETEXAT
- SETRANGE
- EXPIRE
- EXPIREAT
- TTL

#### Set
- SADD
- SCARD
- SETBIT
- SISMEMBER
- SMEMBERS
- SPOP
- SRANDMEMBER
- SREM
- SSCAN

#### List
- LINDEX
- LINSERT
- LLEN
- LPOP
- LPUSH
- LPUSHX
- LRANGE
- LREM
- LSET
- LTRIM
- RPOP
- RPUSH
- RPUSHX

#### Hash
- HDEL
- HEXISTS
- HGET
- HGETALL
- HINCRBY
- HINCRBYFLOAT
- HKEYS
- HLEN
- HMGET
- HMSET
- HSCAN
- HSET
- HSETNX
- HSTRLEN
- HVALS

#### Sorted Sets
- ZADD
- ZCARD
- ZCOUNT
- ZINCRBY
- ZLEXCOUNT
- ZPOPMAX
- ZPOPMIN
- ZRANGE
- ZRANGEBYLEX
- ZRANGEBYSCORE
- ZRANK
- ZREM
- ZREMRANGEBYLEX
- ZREMRANGEBYRANK
- ZREMRANGEBYSCORE
- ZREVRANGE
- ZREVRANGEBYLEX
- ZREVRANGEBYSCORE
- ZREVRANK
- ZSCAN
- ZSCORE

#### Stream
- XACK
- XADD
- XCLAIM
- XDEL
- XLEN
- XINFO
- XPENDING
- XRANGE
- XREADGROUP
- XREVRANGE
- XTRIM
- XGROUP

#### Others
- COMMAND
- PING
- QUIT

## License
IceFireDB-Redis-Proxy is licensed under the MIT License. See [LICENSE](LICENSE) for details.

### Disclaimers
By using this software, you agree that the author, maintainer, and contributors are not responsible for any risks, costs, or problems you may encounter. If you find a software defect or bug, please submit a patch to help improve it!