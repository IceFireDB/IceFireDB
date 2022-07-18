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
	"errors"
	"io"
)

// NewServerNetwork Create a new web server
func NewServerNetwork(
	net, laddr string,
	handler func(conn Conn),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error)) *Server {
	if handler == nil {
		panic("handler is nil")
	}
	s := &Server{
		net:     net,
		laddr:   laddr,
		handler: handler,
		accept:  accept,
		closed:  closed,
		conns:   make(map[*conn]bool),
	}
	return s
}

// ListenAndServe creates a new server and binds to addr configured on "tcp" network net.
func ListenAndServe(net string, addr string,
	handler func(conn Conn),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
) error {
	return ListenAndServeNetwork(net, addr, handler, accept, closed)
}

// ListenAndServeNetwork creates a new server and binds to addr. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func ListenAndServeNetwork(
	net, laddr string,
	handler func(conn Conn),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
) error {
	return NewServerNetwork(net, laddr, handler, accept, closed).ListenAndServe()
}

// handle manages the server connection.
func handle(s *Server, c *conn) {
	var err error
	defer func() {
		c.conn.Close()
		func() {
			// remove the conn from the server
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.conns, c)
			if s.closed != nil {
				if errors.Is(err, io.EOF) {
					err = nil
				}
				s.closed(c, err)
			}
		}()
	}()
	err = func() error {
		for {
			s.handler(c)
			if c.closed {
				return nil
			}
		}
	}()
}
