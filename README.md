<!--
 * @Author: gitsrc
 * @Date: 2020-12-23 13:30:07
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-09-09 11:21:16
 * @FilePath: /IceFireDB/README.md
-->

<p align="center">
<img 
    src="logo.png" 
    width="201" height="300" border="0" alt="IceFireDB">
</p>

# IceFireDB  
![test](https://github.com/IceFireDB/IceFireDB/actions/workflows/test.yml/badge.svg)
![build](https://github.com/IceFireDB/IceFireDB/actions/workflows/build.yml/badge.svg)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FIceFireDB%2FIceFireDB.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FIceFireDB%2FIceFireDB?ref=badge_shield)


Distributed disk storage system based on Raft and RESP protocol.

1. High performance
2. Distributed consistency
3. Reliable LSM disk storage
4. Cold and hot mixed data storage structure
5. Multiple groups of raft clusters, horizontal expansion and contraction of data groups（Upgrading soon）
6. More advanced cache implementation, faster LSM persistent storage

# Imagine Architecture

<p align="center">
<img 
    src="IceFireDB_Architecture.png" 
     alt="IceFireDB_Architecture">
</p>

# Command support
## 1. String operating
* APPEND
* BITCOUNT
* BITOP
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
## 2. Hash operating
* HSET
* HGET
* HDEL
* HEXISTS
* HGETALL
* HINCRBY
* HKEYS
* HLEN
* HMGET
* HMSET
* HSETEX
* HSTRLEN
* HVALS
* HCLEAR
* HMCLEAR
* HEXPIRE
* HEXPIREAT
* HKEYEXIST
* HTTL

## 3. List operating
* RPUSH
* LPOP
* LINDEX
* LPUSH
* RPOP
* LRANGE
* LSET
* LLEN
* RPOPLPUSH
* LCLEAR
* LMCLEAR
* LEXPIRE
* LEXPIREAT
* LKEYEXISTS
* LTRIM
* LTTL

## 4. Sorted Sets
* ZADD
* ZCARD
* ZCOUNT
* ZREM
* ZCLEAR
* ZRANK
* ZRANGE
* ZREVRANGE
* ZSCORE
* ZINCRBY
* ZREVRANK
* ZRANGEBYSCORE
* ZREVRANGEBYSCORE
* ZREMRANGEBYSCORE
* ZREMRANGEBYRANK

## 5. Sets
* SADD
* SCARD
* SDIFF
* SDIFFSTORE
* SINTER
* SINTERSTORE
* SISMEMBER
* SMEMBERS
* SREM
* SUNION
* SUNIONSTORE
* SCLEAR
* SMCLEAR
* SEXPIRE
* SEXPIREAT
* STTL
* SPERSIST
* SKEYEXISTS

## 6. System cmd
* INFO
* FLUSHALL
* HSCAN
* SSCAN
* ZSCAN
* XSCAN
* XHSCAN
* XSSCAN
* XZSCAN

# Performance
### 
```shell
corerman@ubuntu:~/DATA/ICODE/GoLang/IceFireDB$ redis-benchmark  -h 127.0.0.1 -p 11001 -n 10000000 -t set,get -c 512 -P 512 -q

SET: 253232.12 requests per second
GET: 2130875.50 requests per second
```

## License

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FIceFireDB%2FIceFireDB.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FIceFireDB%2FIceFireDB?ref=badge_large)

# Thanks 

### I stood on the shoulders of giants and did only simple things. Thank you for your attention.

* https://github.com/tidwall/uhaha
* https://github.com/syndtr/goleveldb
* https://github.com/dgraph-io/ristretto
* https://github.com/ledisdb/ledisdb
* https://github.com/dgraph-io/badger

# Disclaimers
When you use this software, you have agreed and stated that the author, maintainer and contributor of this software are not responsible for any risks, costs or problems you encounter. If you find a software defect or BUG, ​​please submit a patch to help improve it!

