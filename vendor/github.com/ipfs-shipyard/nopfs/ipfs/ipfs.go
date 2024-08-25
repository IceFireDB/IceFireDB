// Package ipfs provides wrapper implementations of key layers in the go-ipfs
// stack to enable content-blocking.
package ipfs

import (
	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("nopfs-blocks")
