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

package test

import (
	"testing"

	"github.com/IceFireDB/IceFireDB-Proxy/test/proto"

	"github.com/IceFireDB/IceFireDB-Proxy/test/server"
)

func TestHash(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()

	must1(t, c, "HSET", "aap", "noot", "mies")
	t.Run("basic", func(t *testing.T) {
		mustDo(t, c,
			"HGET", "aap", "noot",
			proto.String("mies"),
		)
		equals(t, "mies", directDoString(t, c, "HGET", "aap", "noot"))

		// Existing field.
		must0(t, c, "HSET", "aap", "noot", "mies")

		// Multiple fields.
		mustDo(t, c,
			"HSET", "aaa", "bbb", "cc", "ddd", "ee",
			proto.Int(2),
		)

		mustDo(t, c,
			"HGET", "aaa", "bbb",
			proto.String("cc"),
		)
		equals(t, "cc", directDoString(t, c, "HGET", "aaa", "bbb"))
		mustDo(t, c,
			"HGET", "aaa", "ddd",
			proto.String("ee"),
		)
		equals(t, "ee", directDoString(t, c, "HGET", "aaa", "ddd"))
	})

	t.Run("wrong key type", func(t *testing.T) {
		mustOK(t, c, "SET", "foo", "bar")
		mustDo(t, c,
			"HSET", "foo", "noot", "mies",
			proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"),
		)
	})

	t.Run("unmatched pairs", func(t *testing.T) {
		mustDo(t, c,
			"HSET", "a", "b", "c", "d",
			proto.Error(errWrongNumber("hset")),
		)
	})

	t.Run("no such key", func(t *testing.T) {
		mustNil(t, c, "HGET", "aap", "nosuch")
	})

	t.Run("no such hash", func(t *testing.T) {
		mustNil(t, c, "HGET", "nosuch", "nosuch")
		equals(t, "", directDoString(t, c, "HGET", "nosuch", "nosuch"))
	})

	t.Run("wrong type", func(t *testing.T) {
		mustDo(t, c,
			"HGET", "aap",
			proto.Error("ERR wrong number of arguments for 'HGET' command"),
		)
	})

	t.Run("direct HSet()", func(t *testing.T) {
		directDo(t, c, "HSET", "wim", "zus", "jet")
		mustDo(t, c,
			"HGET", "wim", "zus",
			proto.String("jet"),
		)

		directDo(t, c, "HSET", "xxx", "yyy", "a", "zzz", "b")
		mustDo(t, c,
			"HGET", "xxx", "yyy",
			proto.String("a"),
		)
		mustDo(t, c,
			"HGET", "xxx", "zzz",
			proto.String("b"),
		)
	})
}

func TestHashSetNX(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()

	// New Hash
	must1(t, c, "HSETNX", "wim", "zusnx", "jet")

	must0(t, c, "HSETNX", "wim", "zusnx", "jet")

	// Just a new key
	must1(t, c, "HSETNX", "wim", "aapnx", "noot")

	// Wrong key type
	directDoString(t, c, "SET", "foo", "bar")
	mustDo(t, c,
		"HSETNX", "foo", "nosuch", "nosuch",
		proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"),
	)
}

func TestHashMSet(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()

	// New Hash
	{
		mustOK(t, c, "HMSET", "hash", "wim", "zus", "jet", "vuur")

		equals(t, "zus", directDoString(t, c, "HGET", "hash", "wim"))
		equals(t, "vuur", directDoString(t, c, "HGET", "hash", "jet"))
	}

	// Doesn't touch ttl.
	{
		i, _ := directDoIntErr(t, c, "EXPIRE", "hash", 999)
		equals(t, i, 1)
		mustOK(t, c, "HMSET", "hash", "gijs", "lam")
		time, err := directDoIntErr(t, c, "TTL", "hash")

		equals(t, 999, time)
		equals(t, nil, err)
	}

	{
		// Wrong key type
		directDoString(t, c, "SET", "str", "value")
		mustDo(t, c, "HMSET", "str", "key", "value", proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"))

		// Usage error
		mustDo(t, c, "HMSET", "str", proto.Error(errWrongNumber("HMSET")))
		mustDo(t, c, "HMSET", "str", "odd", proto.Error(errWrongNumber("HMSET")))
		mustDo(t, c, "HMSET", "str", "key", "value", "odd", proto.Error(errWrongNumber("HMSET")))
	}
}

func TestHashDel(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()

	assert(t, !directDoBool(t, c, "EXISTS", "wim"), "no more wim key")

	// Key doesn't exists.
	must0(t, c, "HDEL", "nosuch", "nosuch")

	// Wrong key type
	directDoString(t, c, "SET", "foo", "bar")
	mustDo(t, c, "HDEL", "foo", "nosuch", proto.Error(msgWrongType))

	// Direct HDel()
	directDo(t, c, "HSET", "aap", "noot", "mies")
	directDo(t, c, "HDEL", "aap", "noot")
	equals(t, "", directDoString(t, c, "HGET", "aap", "noot"))
}

func TestHashExists(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()
	must1(t, c, "HSET", "wim", "zus", "zus")
	must1(t, c, "HEXISTS", "wim", "zus")
	must0(t, c, "HEXISTS", "wim", "nosuch")
	must0(t, c, "HEXISTS", "nosuch", "nosuch")

	// Wrong key type
	directDoString(t, c, "SET", "foo", "bar")
	mustDo(t, c,
		"HEXISTS", "foo", "nosuch",
		proto.Error(msgWrongType),
	)
}

func TestHashGetall(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()
	mustOK(t, c, "HMSET", "wim",
		"gijs", "lam",
		"kees", "bok",
		"teun", "vuur",
		"zus", "jet")

	mustDo(t, c,
		"HGETALL", "wim",
		proto.Strings(
			"gijs", "lam",
			"kees", "bok",
			"teun", "vuur",
			"zus", "jet",
		),
	)

	mustDo(t, c, "HGETALL", "nosuch",
		proto.Strings(),
	)

	// Wrong key type
	directDoString(t, c, "SET", "foo", "bar")
	mustDo(t, c, "HGETALL", "foo",
		proto.Error(msgWrongType),
	)
}

func TestHashKeys(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()
	mustOK(t, c, "HMSET", "wim",
		"gijs", "lam",
		"kees", "bok",
		"teun", "vuur",
		"zus", "jet")

	mustDo(t, c,
		"HKEYS", "wim",
		proto.Strings(
			"gijs",
			"kees",
			"teun",
			"zus",
		),
	)

	t.Run("direct", func(t *testing.T) {
		direct, err := directDoStringArrErr(t, c, "HKEYS", "wim")
		ok(t, err)
		equals(t, []string{
			"gijs",
			"kees",
			"teun",
			"zus",
		}, direct)
		res, err := directDoStringArrErr(t, c, "HKEYS", "nosuch")
		equals(t, err, nil)
		equals(t, len(res), 0)
	})

	mustDo(t, c, "HKEYS", "nosuch", proto.Strings())

	// Wrong key type
	directDoString(t, c, "SET", "foo", "bar")
	mustDo(t, c, "HKEYS", "foo", proto.Error(msgWrongType))
}

func TestHashValues(t *testing.T) {
	server.Clear()
	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()
	directDo(t, c, "HSET", "wim", "zus", "jet")
	directDo(t, c, "HSET", "wim", "teun", "vuur")
	directDo(t, c, "HSET", "wim", "gijs", "lam")
	directDo(t, c, "HSET", "wim", "kees", "bok")
	mustDo(t, c, "HVALS", "wim",
		proto.Strings(
			"bok",
			"jet",
			"lam",
			"vuur",
		),
	)

	mustDo(t, c, "HVALS", "nosuch", proto.Strings())

	// Wrong key type
	directDo(t, c, "SET", "foo", "bar")
	mustDo(t, c, "HVALS", "foo", proto.Error(msgWrongType))
}

func TestHashLen(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()
	mustOK(t, c, "HMSET", "wim",
		"gijs", "lam",
		"kees", "bok",
		"teun", "vuur",
		"zus", "jet")
	mustDo(t, c, "HLEN", "wim", proto.Int(4))

	must0(t, c, "HLEN", "nosuch")

	// Wrong key type
	directDoString(t, c, "SET", "foo", "bar")
	mustDo(t, c, "HLEN", "foo", proto.Error(msgWrongType))
}

func TestHashMget(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()
	directDo(t, c, "HSET", "wim", "zus", "jet")
	directDo(t, c, "HSET", "wim", "teun", "vuur")
	directDo(t, c, "HSET", "wim", "gijs", "lam")
	directDo(t, c, "HSET", "wim", "kees", "bok")
	mustDo(t, c,
		"HMGET", "wim", "zus", "nosuch", "kees",
		proto.Array(
			proto.String("jet"),
			proto.Nil,
			proto.String("bok"),
		),
	)

	mustDo(t, c,
		"HMGET", "nosuch", "zus", "kees",
		proto.Array(
			proto.Nil,
			proto.Nil,
		),
	)

	// Wrong key type
	directDo(t, c, "SET", "foo", "bar")
	mustDo(t, c,
		"HMGET", "foo", "bar",
		proto.Error(msgWrongType),
	)
}

func TestHashIncrby(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()

	// New key
	must1(t, c, "HINCRBY", "hash", "field", "1")

	// Existing key
	mustDo(t, c,
		"HINCRBY", "hash", "field", "100",
		proto.Int(101),
	)

	// Minus works.
	mustDo(t, c,
		"HINCRBY", "hash", "field", "-12",
		proto.Int(101-12),
	)

	t.Run("direct", func(t *testing.T) {
		directDo(t, c, "HINCRBY", "hash", "field", -3)

		equals(t, "86", directDoString(t, c, "HGET", "hash", "field"))
	})

	t.Run("errors", func(t *testing.T) {
		// Wrong key type
		directDoString(t, c, "SET", "str", "cake")
		mustDo(t, c,
			"HINCRBY", "str", "case", "4",
			proto.Error(msgWrongType),
		)

		mustDo(t, c,
			"HINCRBY", "str", "case", "foo",
			proto.Error("ERR value is not an integer or out of range"),
		)

		mustDo(t, c,
			"HINCRBY", "str",
			proto.Error(errWrongNumber("HINCRBY")),
		)
	})
}

func TestHashIncrbyfloat(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		directDo(t, c, "HSET", "hash", "field", "12")
		mustDo(t, c,
			"HINCRBYFLOAT", "hash", "field", "400.12",
			proto.String("412.12"),
		)
		equals(t, "412.12", directDoString(t, c, "HGET", "hash", "field"))
	}

	// Existing key, not a number
	{
		directDo(t, c, "HSET", "hash", "field", "noint")
		mustDo(t, c,
			"HINCRBYFLOAT", "hash", "field", "400",
			proto.Error("ERR value is not a valid float"),
		)
	}

	// New key
	{
		mustDo(t, c,
			"HINCRBYFLOAT", "hash", "newfield", "40.33",
			proto.String("40.33"),
		)
		equals(t, "40.33", directDoString(t, c, "HGET", "hash", "newfield"))
	}

	t.Run("direct", func(t *testing.T) {
		directDo(t, c, "HSET", "hash", "field", "500.1")
		equals(t, "500.1", directDoString(t, c, "HGET", "hash", "field"))
		// f, err := s.HIncrfloat("hash", "field", 12)
		f, err := directDoFloatErr(t, c, "HINCRBYFLOAT", "hash", "field", "12")
		ok(t, err)
		equalFloat(t, 512.1, f)
	})

	t.Run("errors", func(t *testing.T) {
		directDoString(t, c, "SET", "wrong", "type")
		mustDo(t, c,
			"HINCRBYFLOAT", "wrong", "type", "400",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"HINCRBYFLOAT",
			proto.Error(errWrongNumber("HINCRBYFLOAT")),
		)
		mustDo(t, c,
			"HINCRBYFLOAT", "wrong",
			proto.Error(errWrongNumber("HINCRBYFLOAT")),
		)
		mustDo(t, c,
			"HINCRBYFLOAT", "wrong", "value",
			proto.Error(errWrongNumber("HINCRBYFLOAT")),
		)
		mustDo(t, c,
			"HINCRBYFLOAT", "wrong", "value", "noint",
			proto.Error("ERR value is not a valid float"),
		)
		mustDo(t, c,
			"HINCRBYFLOAT", "foo", "bar", "12", "tomanye",
			proto.Error(errWrongNumber("HINCRBYFLOAT")),
		)
	})
}

func TestHscan(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()

	// We cheat with hscan. It always returns everything.
	directDo(t, c, "HSET", "h", "field1", "value1")
	directDo(t, c, "HSET", "h", "field2", "value2")

	// No problem
	mustDo(t, c,
		"HSCAN", "h", "0",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("field1"),
				proto.String("value1"),
				proto.String("field2"),
				proto.String("value2"),
			),
		),
	)

	// Invalid cursor
	mustDo(t, c,
		"HSCAN", "h", "42",
		proto.Array(
			proto.String("0"),
			proto.Array(),
		),
	)

	// COUNT (ignored)
	mustDo(t, c,
		"HSCAN", "h", "0", "COUNT", "200",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("field1"),
				proto.String("value1"),
				proto.String("field2"),
				proto.String("value2"),
			),
		),
	)

	// MATCH
	directDo(t, c, "HSET", "h", "aap", "a")
	directDo(t, c, "HSET", "h", "noot", "b")
	directDo(t, c, "HSET", "h", "mies", "m")
	mustDo(t, c,
		"HSCAN", "h", "0", "MATCH", "mi*",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("mies"),
				proto.String("m"),
			),
		),
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"HSCAN",
			proto.Error(errWrongNumber("hscan")),
		)
		mustDo(t, c,
			"HSCAN", "set",
			proto.Error(errWrongNumber("hscan")),
		)
		mustDo(t, c,
			"HSCAN", "set", "noint",
			proto.Error("ERR invalid cursor"),
		)
		mustDo(t, c,
			"HSCAN", "set", "1", "MATCH",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"HSCAN", "set", "1", "COUNT",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"HSCAN", "set", "1", "COUNT", "noint",
			proto.Error("ERR value is not an integer or out of range"),
		)
	})
}

func TestHstrlen(t *testing.T) {
	server.Clear()

	c, err := proto.Dial(server.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("basic", func(t *testing.T) {
		directDo(t, c, "HSET", "myhash", "foo", "bar")
		mustDo(t, c,
			"HSTRLEN", "myhash", "foo",
			proto.Int(3),
		)
	})

	t.Run("no such key", func(t *testing.T) {
		directDo(t, c, "HSET", "myhash", "foo", "bar")
		must0(t, c,
			"HSTRLEN", "myhash", "nosuch",
		)
	})

	t.Run("no such hash", func(t *testing.T) {
		directDo(t, c, "HSET", "myhash", "foo", "bar")
		must0(t, c,
			"HSTRLEN", "yourhash", "foo",
		)
	})

	t.Run("utf8", func(t *testing.T) {
		directDo(t, c, "HSET", "myhash", "snow", "☃☃☃")
		mustDo(t, c,
			"HSTRLEN", "myhash", "snow",
			proto.Int(9),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"HSTRLEN",
			proto.Error("ERR wrong number of arguments for 'HSTRLEN' command"),
		)

		mustDo(t, c,
			"HSTRLEN", "bar",
			proto.Error("ERR wrong number of arguments for 'HSTRLEN' command"),
		)

		mustDo(t, c,
			"HSTRLEN", "bar", "baz", "bak",
			proto.Error("ERR wrong number of arguments for 'HSTRLEN' command"),
		)

		directDoString(t, c, "SET", "notahash", "bar")
		mustDo(t, c,
			"HSTRLEN", "notahash", "bar",
			proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"),
		)
	})
}
