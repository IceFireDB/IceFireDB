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
	"math"

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/RESPHandle"
)

const AbortIndex int8 = math.MaxInt8 / 2

type Context struct {
	Writer *RESPHandle.WriterHandle
	// Args     [][]byte
	Args     []interface{}
	Cmd      string
	Handlers HandlersChain // Middleware and final handler functions
	Index    int8
	Op       OpFlag
	Reply    interface{}
}

func (c *Context) Reset() {
	c.Writer = nil
	c.Args = c.Args[0:0]
	c.Handlers = nil
	c.Index = -1
	c.Reply = nil
}

/************************************/
/*********** FLOW CONTROL ***********/
/************************************/

// Next should be used only inside middleware.
// It executes the pending Handlers in the chain inside the calling handler.
// See example in GitHub.
func (c *Context) Next() error {
	c.Index++
	for c.Index < int8(len(c.Handlers)) {
		err := c.Handlers[c.Index](c)
		if err != nil {
			return err
		}
		c.Index++
	}
	return nil
}

// IsAborted returns true if the current context was aborted.
func (c *Context) IsAborted() bool {
	return c.Index >= AbortIndex
}

// Abort prevents pending Handlers from being called. Note that this will not stop the current handler.
// Let's say you have an authorization middleware that validates that the current request is authorized.
// If the authorization fails (ex: the password does not match), call Abort to ensure the remaining Handlers
// for this request are not called.
func (c *Context) Abort() {
	c.Index = AbortIndex
}
