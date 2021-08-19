// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import "github.com/tidwall/redcon"

// FilterArgs ...
type FilterArgs []string

// Hijack is a function type that can be used to "hijack" a service client
// connection and allowing to perform I/O operations outside the standard
// network loop. An example of it's usage can be found in the examples/kvdb
// project.
type Hijack func(s Service, conn HijackedConn)

// HijackedConn is a connection that has been detached from the main service
// network loop. It's entirely up to the hijacker to performs all I/O
// operations. The Write* functions buffer write data and the Flush must be
// called to do the actual sending of the data to the connection.
// Close the connection to when done.
type HijackedConn interface {
	// RemoteAddr is the connection remote tcp address.
	RemoteAddr() string
	// ReadCommands is an iterator function that reads pipelined commands.
	// Returns a error when the connection encountared and error.
	ReadCommands(func(args []string) bool) error
	// ReadCommand reads one command at a time.
	ReadCommand() (args []string, err error)
	// WriteAny writes any type to the write buffer using the format rules that
	// are defined by the original Service.
	WriteAny(v interface{})
	// WriteRaw writes raw data to the write buffer.
	WriteRaw(data []byte)
	// Flush the write write buffer and send data to the connection.
	Flush() error
	// Close the connection
	Close() error
}

type redisHijackConn struct {
	dconn redcon.DetachedConn
	cmds  []redcon.Command
}

func newRedisHijackedConn(dconn redcon.DetachedConn) *redisHijackConn {
	return &redisHijackConn{dconn: dconn}
}

func (conn *redisHijackConn) ReadCommands(iter func(args []string) bool) error {
	if len(conn.cmds) == 0 {
		cmd, err := conn.dconn.ReadCommand()
		if err != nil {
			return err
		}
		if !iter(redisCommandToArgs(cmd)) {
			return nil
		}
		conn.cmds = conn.dconn.ReadPipeline()
	}
	for len(conn.cmds) > 0 {
		cmd := conn.cmds[0]
		conn.cmds = conn.cmds[1:]
		if !iter(redisCommandToArgs(cmd)) {
			return nil
		}
	}
	return nil
}

func (conn *redisHijackConn) WriteAny(v interface{}) {
	conn.dconn.WriteAny(v)
}

func (conn *redisHijackConn) WriteRaw(data []byte) {
	conn.dconn.WriteRaw(data)
}

func (conn *redisHijackConn) Flush() error {
	return conn.dconn.Flush()
}

func (conn *redisHijackConn) Close() error {
	return conn.dconn.Close()
}

func (conn *redisHijackConn) RemoteAddr() string {
	return conn.dconn.RemoteAddr()
}

func (conn *redisHijackConn) ReadCommand() (args []string, err error) {
	err = conn.ReadCommands(func(iargs []string) bool {
		args = iargs
		return false
	})
	return args, err
}
