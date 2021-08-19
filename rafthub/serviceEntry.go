// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"io"
	"net"
)

type serviceEntry struct {
	sniff func(rd io.Reader) bool
	serve func(s Service, ln net.Listener)
}
