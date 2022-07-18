# IceFireDB-Redis-proxy

IceFireDB-Redis-proxy database proxy adds decentralization wings to traditional redis databases. Provide a convenient mechanism to build a globally distributed storage system with automatic networking. The instructions are automatically synchronized between the networked redis agents, and the redis agent writes data to the cluster or single-point redis storage. Through the decentralized middleware network proxy, decentralized data synchronization can be enabled for the Redis database commonly used in web2 applications.

1. Complete data source mode support: stand-alone, cluster mode
2. Rich command support
3. Excellent cluster state management and failover
4. Excellent traffic control strategy: traffic read-write separation and multi-tenant data isolation
6. Supports P2P automatic networking, and the agent helps the traditional Redis database to achieve data dispersion.

## How does it work?

### scene introduction
![image](https://user-images.githubusercontent.com/34047788/179436964-00aa6ffd-f6f4-4c60-a46c-fb0165296049.png)


### system structure
![image](https://user-images.githubusercontent.com/34047788/179439830-e0e4c480-553a-4274-9d12-9afacfcdfe77.png)


### Configuration

In the config directory, the user stores the project configuration file, the file name: config.yaml, which can be modified according to their own needs

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
# proxy redis configuration
redisdb:
# prometheus metrics configuration
prometheus_exporter:
...
```

### Quickstart

https://user-images.githubusercontent.com/52234994/173170991-08713e52-291c-4fae-bf46-ce87b959ce90.mp4

Run the binary file directly, if you need to run in the background, you can add it to the systemd system management
```shell
$ make
$ ./bin/Icefiredb-proxy -c ./config/config.yaml
```

### Usage

#### String
* APPEND
* BITCOUNT
* BITPOS
* DECR
* DECRBY
* DEL
* EXISTS
* GET
* GETBIT
* SETBIT
* GETRANGE
* GETSET
* INCR
* INCRBY
* MGET
* MSET
* SET
* SETEX
* SETEXAT
* SETRANGE
* EXPIRE
* EXPIREAT
* TTL


#### Set
* SADD
* SCARD
* SETBIT
* SISMEMBER
* SMEMBERS
* SPOP
* SRANDMEMBER
* SREM
* SSCAN

#### List
* LINDEX
* LINSERT
* LLEN
* LPOP
* LPUSH
* LPUSHX
* LRANGE
* LREM
* LSET
* LTRIM
* RPOP
* RPUSH
* RPUSHX

#### Hash
* HDEL
* HEXISTS
* HGET
* HGETALL
* HINCRBY
* HINCRBYFLOAT
* HKEYS
* HLEN
* HMGET
* HMSET
* HSCAN
* HSET
* HSETNX
* HSTRLEN
* HVALS

#### Sorted Sets
* ZADD
* ZCARD
* ZCOUNT
* ZINCRBY
* ZLEXCOUNT
* ZPOPMAX
* ZPOPMIN
* ZLEXCOUNT
* ZRANGE
* ZRANGEBYLEX
* ZRANGEBYSCORE
* ZRANK
* ZREM
* ZREMRANGEBYLEX
* ZREMRANGEBYRANK
* ZREMRANGEBYSCORE
* ZREVRANGE
* ZREVRANGEBYLEX
* ZREVRANGEBYSCORE
* ZREVRANK
* ZSCAN
* ZSCORE

#### Stream
* XACK
* XADD
* XCLAIM
* XDEL
* XLEN
* XINFO
* XPENDING
* XRANGE
* XREADGROUP
* XREVRANGE
* XTRIM
* XGROUP


#### Others

* COMMAND
* PING
* QUIT

## License
Icefiredb proxy uses the Apache 2.0 license. See [LICENSE](.LICENSE) for details.

## Disclaimers
When you use this software, you have agreed and stated that the author, maintainer and contributor of this software are not responsible for any risks, costs or problems you encounter. If you find a software defect or BUG, ​​please submit a patch to help improve it!
