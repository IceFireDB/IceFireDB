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
	"net"
	"os"
	"sync"
)

// UnixFilePermissionMode is File default permissions
const unixFilePermissionMode = 0o777

// Server defines a server for clients for managing client connections.
type Server struct {
	mu      sync.RWMutex
	net     string
	laddr   string
	handler func(conn Conn)
	accept  func(conn Conn) bool
	closed  func(conn Conn, err error)
	conns   map[*conn]bool
	ln      net.Listener
	done    bool
}

// ListenServeAndSignal begin listen server and listen signal
func (s *Server) ListenServeAndSignal(signal chan error) error {
	// If it is a UNIX network, and the network monitoring file already exists, delete it
	err := s.RmUnixFile()
	if err != nil {
		return err
	}

	ln, err := net.Listen(s.net, s.laddr)
	if err != nil {
		if signal != nil {
			signal <- err
		}
		return err
	}
	s.ln = ln
	if signal != nil {
		signal <- nil
	}

	// Under the UNIX network, set permissions for network monitoring files
	err = s.ChmodUnixFile(unixFilePermissionMode)
	if err != nil {
		return err
	}

	return serve(s)
}

// Close stops listening on the TCP address.
// Already Accepted connections will be closed.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return errors.New("not serving")
	}
	s.done = true
	return s.ln.Close()
}

// ListenAndServe serves incoming connections.
func (s *Server) ListenAndServe() error {
	return s.ListenServeAndSignal(nil)
}

func serve(s *Server) error {
	defer func() {
		s.ln.Close()
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			for c := range s.conns {
				c.Close()
			}
			s.conns = nil
		}()
	}()

	for {
		lnconn, err := s.ln.Accept()
		if err != nil {
			s.mu.RLock()
			done := s.done
			s.mu.RUnlock()
			if done {
				return nil
			}
			return err
		}
		c := &conn{
			conn: lnconn,
			addr: lnconn.RemoteAddr().String(),
		}
		s.mu.Lock()
		s.conns[c] = true
		s.mu.Unlock()
		if s.accept != nil && !s.accept(c) {
			s.mu.Lock()
			delete(s.conns, c)
			s.mu.Unlock()
			c.Close()
			continue
		}
		go handle(s, c)
	}
}

// RmUnixFile do remove UNIX bind file
func (s *Server) RmUnixFile() (err error) {
	if s.net == "unix" {
		// If the file does not exist, do not operate
		if _, statErr := os.Stat(s.laddr); os.IsNotExist(statErr) {
			return
		}

		err = os.Remove(s.laddr)

		if err != nil {
			return err
		}
	}

	return
}

// ChmodUnixFile do chmod UNIX bind file
func (s *Server) ChmodUnixFile(fileMode os.FileMode) (err error) {
	if s.net == "unix" {
		// If the file does not exist, do not operate
		if _, statErr := os.Stat(s.laddr); os.IsNotExist(statErr) {
			return
		}

		err = os.Chmod(s.laddr, fileMode)
		if err != nil {
			return err
		}
	}

	return
}
