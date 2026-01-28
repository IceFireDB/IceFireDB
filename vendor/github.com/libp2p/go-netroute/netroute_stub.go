// A stub routing table conformant interface for js/wasm environments.

//go:build (js && wasm) || (wasip1 && wasm)

package netroute

import (
	"net"
)

func New() (Router, error) {
	rtr := &router{}
	rtr.ifaces = make(map[int]net.Interface)
	rtr.ifaces[0] = net.Interface{}
	rtr.addrs = make(map[int]ipAddrs)
	rtr.addrs[0] = ipAddrs{}
	rtr.v4 = []*rtInfo{{}}
	rtr.v6 = []*rtInfo{{}}
	return rtr, nil
}
