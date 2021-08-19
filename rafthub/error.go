// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

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
