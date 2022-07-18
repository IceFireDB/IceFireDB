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

import (
	"bytes"
	"sync"
)

type makeKeyFunc func(arg []interface{}) []uint8

var FirstKeyIndex = []uint8{1}

func firstKey(_ []interface{}) []uint8 {
	return FirstKeyIndex
}

func allKey(arg []interface{}) []uint8 {
	index := make([]uint8, len(arg)-1)
	for k := range index {
		index[k] = uint8(k + 1)
	}
	return index
}

func OddKey(arg []interface{}) []uint8 {
	index := make([]uint8, (len(arg)-1)/2)
	var val int
	for k := range index {
		val = k
		index[k] = uint8(1 + val<<1)
	}
	return index
}

var cmdKeyMap = map[string]makeKeyFunc{
	"MGET":   allKey,
	"MSET":   OddKey,
	"DEL":    allKey,
	"EXISTS": allKey,
}

func Namespace(prefix []byte) HandlerFunc {
	npool := sync.Pool{New: func() interface{} {
		return bytes.NewBuffer(nil)
	}}
	if prefix[len(prefix)-1] != ':' {
		prefix = append(prefix, ':')
	}
	return func(c *Context) error {
		if len(c.Args) == 1 {
			return c.Next()
		}
		fn, ok := cmdKeyMap[c.Cmd]
		if !ok {
			fn = firstKey
		}
		keyIndex := fn(c.Args)
		for _, v := range keyIndex {
			buf := npool.Get().(*bytes.Buffer)
			defer func() {
				buf.Reset()
				npool.Put(buf)
			}()
			buf.Write(prefix)
			buf.Write(c.Args[v].([]byte))
			c.Args[v] = buf.Bytes()
		}
		return c.Next()
	}
}
