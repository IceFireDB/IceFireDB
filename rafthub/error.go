/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:04:01
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:38:54
 * @FilePath: /RaftHub/error.go
 */

package rafthub

import (
	"errors"

	"github.com/hashicorp/raft"
)

var errWrongNumArgsRaft = errors.New("wrong number of arguments, try RAFT HELP")
var errWrongNumArgsCluster = errors.New("wrong number of arguments, " +
	"try CLUSTER HELP")

// ErrWrongNumArgs is returned when the arg count is wrong
var ErrWrongNumArgs = errors.New("wrong number of arguments")

// ErrUnauthorized is returned when a client connection has not been authorized
var ErrUnauthorized = errors.New("unauthorized")

// ErrUnknownCommand is returned when a command is not known
var ErrUnknownCommand = errors.New("unknown command")

// ErrInvalid is returned when an operation has invalid arguments or options
var ErrInvalid = errors.New("invalid")

// ErrCorrupt is returned when a data is invalid or corrupt
var ErrCorrupt = errors.New("corrupt")

// ErrSyntax is returned where there was a syntax error
var ErrSyntax = errors.New("syntax error")

// ErrNotLeader is returned when the raft leader is unknown
var ErrNotLeader = raft.ErrNotLeader

var errLeaderUnknown = errors.New("leader unknown")
