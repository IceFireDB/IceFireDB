// Package nopfs implements content blocking for the IPFS stack.
//
// nopfs provides an implementation of the compact denylist format (IPIP-383),
// with methods to check whether IPFS paths and CIDs are blocked.
//
// In order to seamlessly be inserted into the IPFS stack, content-blocking
// wrappers for several components are provided (BlockService, NameSystem,
// Resolver...). A Kubo plugin (see kubo/) can be used to give Kubo
// content-blocking superpowers.
package nopfs

import (
	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("nopfs")
