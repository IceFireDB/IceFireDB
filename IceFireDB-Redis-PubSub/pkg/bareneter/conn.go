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

package bareneter

import (
	"net"
)

type conn struct {
	conn   net.Conn
	addr   string
	ctx    interface{}
	closed bool
}

// Conn represents a client connection
type Conn interface {
	// RemoteAddr returns the remote address of the client connection.
	RemoteAddr() string

	// Close closes the connection.
	Close() error
	IsClosed() bool
	Context() interface{}

	// SetContext sets a user-defined context
	SetContext(v interface{})
	NetConn() net.Conn
}

func (c *conn) Context() interface{} { return c.ctx }

func (c *conn) SetContext(v interface{}) { c.ctx = v }

func (c *conn) RemoteAddr() string { return c.addr }

func (c *conn) NetConn() net.Conn {
	return c.conn
}

func (c *conn) Close() error {
	c.closed = true
	return c.conn.Close()
}

func (c *conn) IsClosed() bool {
	return c.closed
}
