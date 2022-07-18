/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package router

var charmap [256]byte

func init() {
	for i := range charmap {
		c := byte(i)
		switch {
		case c >= 'A' && c <= 'Z':
			charmap[i] = c
		case c >= 'a' && c <= 'z':
			charmap[i] = c - 'a' + 'A'
		case c == ':':
			charmap[i] = ':'
		}
	}
}

type OpFlag uint32

func (f OpFlag) IsNotAllowed() bool {
	return (f & FlagNotAllow) != 0
}

func (f OpFlag) IsReadOnly() bool {
	const mask = FlagWrite | FlagMayWrite
	return (f & mask) == 0
}

func (f OpFlag) IsMasterOnly() bool {
	const mask = FlagWrite | FlagMayWrite | FlagMasterOnly
	return (f & mask) != 0
}

type VerifyFunc func(argsLen int) bool

func equal(num int) VerifyFunc {
	return func(argsLen int) bool {
		return argsLen == num
	}
}

func less(num int) VerifyFunc {
	return func(argsLen int) bool {
		return argsLen < num
	}
}

func greater(num int) VerifyFunc {
	return func(argsLen int) bool {
		return argsLen >= num
	}
}

func modulos(num, modVal int) VerifyFunc {
	return func(argsLen int) bool {
		return argsLen%num == modVal
	}
}

type OpInfo struct {
	Name       string
	Flag       OpFlag
	ArgsVerify VerifyFunc
}

const (
	FlagWrite = 1 << iota
	FlagMasterOnly
	FlagMayWrite
	FlagNotAllow
)

var OpTable = make(map[string]OpInfo, 256)

func init() {
	for _, i := range []OpInfo{
		{"APPEND", FlagWrite, equal(3)},
		{"ASKING", FlagNotAllow, equal(2)},
		{"AUTH", 0, equal(2)},
		{"BGREWRITEAOF", FlagNotAllow, greater(1)},
		{"BGSAVE", FlagNotAllow, greater(1)},
		{"BITCOUNT", 0, greater(2)},
		{"BITFIELD", FlagWrite, greater(2)},
		{"BITOP", FlagWrite | FlagNotAllow, greater(4)},
		{"BITPOS", 0, greater(3)},
		{"BLPOP", FlagWrite | FlagNotAllow, greater(3)},
		{"BRPOP", FlagWrite | FlagNotAllow, greater(3)},
		{"BRPOPLPUSH", FlagWrite | FlagNotAllow, equal(4)},
		{"CLIENT", FlagNotAllow, greater(1)},
		{"CLUSTER", FlagNotAllow, greater(1)},
		{"COMMAND", 0, greater(1)},
		{"CONFIG", FlagNotAllow, greater(1)},
		{"DBSIZE", FlagNotAllow, greater(1)},
		{"DEBUG", FlagNotAllow, greater(1)},
		{"DECR", FlagWrite, equal(2)},
		{"DECRBY", FlagWrite, equal(3)},
		{"DEL", FlagWrite, greater(2)},
		{"DISCARD", FlagNotAllow, greater(1)},
		{"DUMP", 0, equal(2)},
		{"ECHO", 0, equal(2)},
		{"EVAL", FlagWrite, greater(3)},
		{"EVALSHA", FlagWrite, greater(3)},
		{"EXEC", FlagNotAllow, greater(1)},
		{"EXISTS", 0, greater(2)},
		{"EXPIRE", FlagWrite, equal(3)},
		{"EXPIREAT", FlagWrite, equal(3)},
		{"FLUSHALL", FlagWrite | FlagNotAllow, greater(1)},
		{"FLUSHDB", FlagWrite | FlagNotAllow, greater(1)},
		{"GEOADD", FlagWrite, greater(5)},
		{"GEODIST", 0, greater(4)},
		{"GEOHASH", 0, greater(3)},
		{"GEOPOS", 0, greater(3)},
		{"GEORADIUS", FlagWrite, greater(6)},
		{"GEORADIUSBYMEMBER", FlagWrite, greater(5)},
		{"GET", 0, equal(2)},
		{"GETBIT", 0, equal(3)},
		{"GETRANGE", 0, equal(4)},
		{"GETSET", FlagWrite, equal(3)},
		{"HDEL", FlagWrite, greater(3)},
		{"HEXISTS", 0, equal(3)},
		{"HGET", 0, equal(3)},
		{"HGETALL", 0, equal(2)},
		{"HINCRBY", FlagWrite, equal(4)},
		{"HINCRBYFLOAT", FlagWrite, equal(4)},
		{"HKEYS", 0, equal(2)},
		{"HLEN", 0, equal(2)},
		{"HMGET", 0, greater(3)},
		{"HMSET", FlagWrite, greater(4)},
		{"HOST:", FlagNotAllow, greater(1)},
		{"HSCAN", FlagMasterOnly, greater(3)},
		{"HSET", FlagWrite, greater(4)},
		{"HSETNX", FlagWrite, equal(4)},
		{"HSTRLEN", 0, equal(3)},
		{"HVALS", 0, equal(2)},
		{"INCR", FlagWrite, equal(2)},
		{"INCRBY", FlagWrite, equal(3)},
		{"INCRBYFLOAT", FlagWrite, equal(3)},
		{"INFO", FlagNotAllow, greater(1)},
		{"KEYS", FlagNotAllow, greater(1)},
		{"LASTSAVE", FlagNotAllow, greater(1)},
		{"LATENCY", FlagNotAllow, greater(1)},
		{"LINDEX", 0, equal(3)},
		{"LINSERT", FlagWrite, equal(5)},
		{"LLEN", 0, equal(2)},
		{"LPOP", FlagWrite, greater(2)},
		{"LPUSH", FlagWrite, greater(3)},
		{"LPUSHX", FlagWrite, greater(3)},
		{"LRANGE", 0, equal(4)},
		{"LREM", FlagWrite, equal(4)},
		{"LSET", FlagWrite, equal(4)},
		{"LTRIM", FlagWrite, equal(4)},
		{"MGET", 0, greater(2)},
		{"MIGRATE", FlagWrite | FlagNotAllow, greater(1)},
		{"MONITOR", FlagNotAllow, greater(1)},
		{"MOVE", FlagWrite | FlagNotAllow, greater(1)},
		{"MSET", FlagWrite, modulos(2, 1)},
		{"MSETNX", FlagWrite | FlagNotAllow, greater(1)},
		{"MULTI", FlagNotAllow, greater(1)},
		{"OBJECT", FlagNotAllow, greater(1)},
		{"PERSIST", FlagWrite, equal(2)},
		{"PEXPIRE", FlagWrite, equal(3)},
		{"PEXPIREAT", FlagWrite, equal(3)},
		{"PFADD", FlagWrite, greater(3)},
		{"PFCOUNT", 0, greater(2)},
		{"PFDEBUG", FlagWrite, greater(1)},
		{"PFMERGE", FlagWrite, greater(3)},
		{"PFSELFTEST", 0, greater(1)},
		{"PING", 0, greater(1)},
		{"POST", FlagNotAllow, greater(1)},
		{"PSETEX", FlagWrite, equal(4)},
		{"PSUBSCRIBE", FlagNotAllow, greater(2)},
		{"PSYNC", FlagNotAllow, greater(1)},
		{"PTTL", 0, equal(2)},
		{"PUBLISH", FlagNotAllow, equal(3)},
		{"PUBSUB", 0, greater(2)},
		{"PUNSUBSCRIBE", FlagNotAllow, greater(1)},
		{"QUIT", FlagNotAllow, greater(1)},
		{"RANDOMKEY", FlagNotAllow, greater(1)},
		{"READONLY", FlagNotAllow, greater(1)},
		{"READWRITE", FlagNotAllow, greater(1)},
		{"RENAME", FlagWrite | FlagNotAllow, greater(1)},
		{"RENAMENX", FlagWrite | FlagNotAllow, greater(1)},
		{"REPLCONF", FlagNotAllow, greater(1)},
		{"RESTORE", FlagWrite | FlagNotAllow, greater(1)},
		{"RESTORE-ASKING", FlagWrite | FlagNotAllow, greater(1)},
		{"ROLE", FlagNotAllow, greater(1)},
		{"RPOP", FlagWrite, equal(2)},
		{"RPOPLPUSH", FlagNotAllow, equal(3)},
		{"RPUSH", FlagWrite, greater(3)},
		{"RPUSHX", FlagWrite, greater(3)},
		{"SADD", FlagWrite, greater(3)},
		{"SAVE", FlagNotAllow, greater(1)},
		{"SCAN", FlagMasterOnly | FlagNotAllow, greater(1)},
		{"SCARD", 0, equal(2)},
		{"SCRIPT", FlagNotAllow, greater(1)},
		{"SDIFF", FlagNotAllow, greater(2)},
		{"SDIFFSTORE", FlagWrite | FlagNotAllow, greater(1)},
		{"SELECT", FlagNotAllow, equal(1)},
		{"SET", FlagWrite, greater(3)},
		{"SETBIT", FlagWrite, equal(4)},
		{"SETEX", FlagWrite, equal(4)},
		{"SETNX", FlagWrite, equal(3)},
		{"SETRANGE", FlagWrite, equal(4)},
		{"SHUTDOWN", FlagNotAllow, greater(1)},
		{"SINTER", FlagNotAllow, equal(2)},
		{"SINTERSTORE", FlagNotAllow, greater(3)},
		{"SISMEMBER", 0, equal(3)},
		{"SLAVEOF", FlagNotAllow, greater(1)},
		{"SLOTSCHECK", FlagNotAllow, greater(1)},
		{"SLOTSDEL", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSHASHKEY", FlagNotAllow, greater(1)},
		{"SLOTSINFO", FlagMasterOnly | FlagNotAllow, greater(1)},
		{"SLOTSMAPPING", FlagNotAllow, greater(1)},
		{"SLOTSMGRTONE", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSMGRTSLOT", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSMGRTTAGONE", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSMGRTTAGSLOT", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSRESTORE", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSMGRTONE-ASYNC", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSMGRTSLOT-ASYNC", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSMGRTTAGONE-ASYNC", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSMGRTTAGSLOT-ASYNC", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSMGRT-ASYNC-FENCE", FlagNotAllow, greater(1)},
		{"SLOTSMGRT-ASYNC-CANCEL", FlagNotAllow, greater(1)},
		{"SLOTSMGRT-ASYNC-STATUS", FlagNotAllow, greater(1)},
		{"SLOTSMGRT-EXEC-WRAPPER", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSRESTORE-ASYNC", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSRESTORE-ASYNC-AUTH", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSRESTORE-ASYNC-ACK", FlagWrite | FlagNotAllow, greater(1)},
		{"SLOTSSCAN", FlagMasterOnly | FlagNotAllow, greater(1)},
		{"SLOWLOG", FlagNotAllow, greater(1)},
		{"SMEMBERS", 0, equal(2)},
		{"SMOVE", FlagWrite | FlagNotAllow, greater(1)},
		{"SORT", FlagWrite, greater(2)},
		{"SPOP", FlagWrite, greater(2)},
		{"SRANDMEMBER", 0, greater(2)},
		{"SREM", FlagWrite, greater(3)},
		{"SSCAN", FlagMasterOnly, greater(3)},
		{"STRLEN", 0, equal(2)},
		{"SUBSCRIBE", FlagNotAllow, greater(1)},
		{"SUBSTR", 0, equal(4)},
		{"SUNION", FlagNotAllow, greater(1)},
		{"SUNIONSTORE", FlagWrite | FlagNotAllow, greater(1)},
		{"SYNC", FlagNotAllow, greater(1)},
		{"TIME", FlagNotAllow, greater(1)},
		{"TOUCH", FlagWrite | FlagNotAllow, greater(1)},
		{"TTL", 0, equal(2)},
		{"TYPE", 0, equal(2)},
		{"UNSUBSCRIBE", FlagNotAllow, greater(1)},
		{"UNWATCH", FlagNotAllow, greater(1)},
		{"WAIT", FlagNotAllow, greater(1)},
		{"WATCH", FlagNotAllow, greater(1)},
		{"ZADD", FlagWrite, greater(4)},
		{"ZCARD", 0, equal(2)},
		{"ZCOUNT", 0, equal(4)},
		{"ZINCRBY", FlagWrite, equal(4)},
		{"ZINTERSTORE", FlagWrite | FlagNotAllow, greater(1)},
		{"ZLEXCOUNT", 0, equal(4)},
		{"ZPOPMAX", FlagMayWrite, greater(2)},
		{"ZPOPMIN", FlagMayWrite, greater(2)},
		{"ZLEXCOUNT", 0, equal(4)},
		{"ZRANGE", 0, greater(4)},
		{"ZRANGEBYLEX", 0, greater(4)},
		{"ZRANGEBYSCORE", 0, greater(4)},
		{"ZRANK", 0, equal(3)},
		{"ZREM", FlagWrite, greater(3)},
		{"ZREMRANGEBYLEX", FlagWrite, equal(4)},
		{"ZREMRANGEBYRANK", FlagWrite, equal(4)},
		{"ZREMRANGEBYSCORE", FlagWrite, equal(4)},
		{"ZREVRANGE", 0, greater(4)},
		{"ZREVRANGEBYLEX", 0, greater(4)},
		{"ZREVRANGEBYSCORE", 0, greater(4)},
		{"ZREVRANK", 0, equal(3)},
		{"ZSCAN", FlagMasterOnly, greater(3)},
		{"ZSCORE", 0, equal(3)},
		{"ZUNIONSTORE", FlagWrite | FlagNotAllow, greater(1)},
		{"XACK", FlagWrite, greater(4)},
		{"XADD", FlagWrite, greater(5)},
		{"XCLAIM", FlagWrite, greater(6)},
		{"XDEL", FlagWrite, greater(3)},
		{"XLEN", 0, greater(2)},
		{"XINFO", 0, greater(3)},
		{"XPENDING", 0, greater(3)},
		{"XRANGE", 0, greater(4)},
		{"XREAD", FlagWrite | FlagNotAllow, greater(4)},
		{"XREADGROUP", FlagWrite, greater(7)},
		{"XREVRANGE", FlagWrite, greater(4)},
		{"XTRIM", FlagWrite, greater(4)},
		{"XGROUP", FlagWrite, greater(4)},
		{"WCONFIG", FlagWrite, greater(5)},
	} {
		OpTable[i.Name] = i
	}
}
