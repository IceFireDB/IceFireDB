// Copyright 2012 Google, Inc. All rights reserved.
//
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

//go:build darwin || dragonfly || freebsd || netbsd || openbsd

// This is a BSD import for the routing structure initially found in
// https://github.com/google/gopacket/blob/master/routing/routing.go
// RIB parsing follows the BSD route format described in
// https://github.com/freebsd/freebsd/blob/master/sys/net/route.h
package netroute

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/route"
	"golang.org/x/sys/unix"
)

const (
	RTF_IFSCOPE     = 0x1000000
	ROUTE_MSGFILTER = 1
)

type bsdRouter struct {
	id  uintptr
	seq atomic.Uint32
}

// toIPAddr converts a route.Addr to a net.IP.
// Returns nil if the address type is not recognized.
func toIPAddr(a route.Addr) net.IP {
	switch t := a.(type) {
	case *route.Inet4Addr:
		return t.IP[:]
	case *route.Inet6Addr:
		return t.IP[:]
	default:
		return nil
	}
}

// toRouteAddr takes a net.IP and returns the corresponding route.Addr.
// Returns nil if the IP is empty or has an invalid length.
// IPv4 addresses are converted to route.Inet4Addr and IPv6 to route.Inet6Addr.
func toRouteAddr(ip net.IP) route.Addr {
	if len(ip) == 0 {
		return nil
	}

	if len(ip) != net.IPv4len && len(ip) != net.IPv6len {
		return nil
	}
	if p4 := ip.To4(); len(p4) == net.IPv4len {
		return &route.Inet4Addr{IP: [4]byte(p4)}
	}
	return &route.Inet6Addr{IP: [16]byte(ip)}
}

// ipToIfIndex takes an IP and returns the index of the interface with the given IP assigned.
// Returns an error if no interface is found with the specified IP address.
func ipToIfIndex(ip net.IP) (int, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return -1, fmt.Errorf("failed to get interfaces: %w", err)
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			inet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			if inet.IP.Equal(ip) {
				return iface.Index, nil
			}
		}
	}
	return -1, fmt.Errorf("no interface found for IP: %s", ip)
}

// macToIfIndex takes a MAC address and returns the index of the interface with the matching hardware address.
// Returns an error if no interface is found with the specified MAC address.
func macToIfIndex(hwAddr net.HardwareAddr) (int, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return -1, fmt.Errorf("failed to get interfaces: %w", err)
	}
	for _, iface := range ifaces {
		if hwAddr.String() == iface.HardwareAddr.String() {
			return iface.Index, nil
		}
	}
	return -1, fmt.Errorf("no interface found for MAC: %s", hwAddr.String())
}

// getIfIndex determines the interface index based on the provided MAC address and/or IP address.
// If both are provided, it ensures they resolve to the same interface.
// Returns -1 if neither MAC nor IP is provided, or an error if they resolve to different interfaces.
func getIfIndex(MACAddr net.HardwareAddr, ip net.IP) (int, error) {
	ipIndex := -1
	macIndex := -1
	var err error
	if ip == nil && MACAddr == nil {
		return -1, nil
	}

	if ip != nil {
		ipIndex, err = ipToIfIndex(ip)
		if err != nil {
			return -1, fmt.Errorf("failed to find interface with IP: %s", ip.String())
		}
	}

	if MACAddr != nil {
		macIndex, err = macToIfIndex(MACAddr)
		if err != nil {
			return -1, fmt.Errorf("failed to find interface with MAC: %s", MACAddr.String())
		}
	}

	switch {
	case (ipIndex >= 0 && macIndex >= 0) && (macIndex != ipIndex):
		return -1, fmt.Errorf("given MAC address and source IP resolve to different interfaces")
	case (ipIndex >= 0 && macIndex >= 0) && (macIndex == ipIndex):
		return ipIndex, nil
	case ipIndex >= 0:
		return ipIndex, nil
	case macIndex >= 0:
		return macIndex, nil
	default:
		return -1, fmt.Errorf("no index found for given ip and/or MAC address")
	}
}

// composeRouteMsg creates a RTM_GET RouteMessage for querying the routing table.
// It takes the process ID, optional MAC address, optional source IP, and destination IP.
// The function determines the appropriate interface index if MAC or source IP is provided.
func composeRouteMsg(id uintptr, seq int, MACAddr net.HardwareAddr, src, dst net.IP) (*route.RouteMessage, error) {
	dstAddr := toRouteAddr(dst)
	if dstAddr == nil {
		return nil, fmt.Errorf("failed to parse dst: %#v", dst)
	}

	msg := &route.RouteMessage{
		Version: syscall.RTM_VERSION,
		Type:    unix.RTM_GET,
		ID:      id,
		Seq:     seq,
		Addrs: []route.Addr{
			dstAddr,
			nil,
			nil,
			nil,
			&route.LinkAddr{},
		},
	}

	ifIndex, err := getIfIndex(MACAddr, src)
	if err != nil {
		return nil, err
	}

	if ifIndex >= 0 {
		msg.Flags = RTF_IFSCOPE
		msg.Index = ifIndex
	}

	return msg, nil
}

func (r *bsdRouter) getSeq() int {
	return int(r.seq.Add(1))
}

// getRouteMsgReply takes an RTM_GET RouteMessage and returns reply (RouteMessage)
func getRouteMsgReply(msg *route.RouteMessage) (*route.RouteMessage, error) {
	if msg.Type != syscall.RTM_GET {
		return nil, errors.New("message type is not RTM_GET")
	}

	fd, err := getRouteFD()
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

	err = writeMsg(fd, msg)
	if err != nil {
		return nil, err
	}

	// Read the response message
	return readMsg(msg.ID, msg.Seq, fd)
}

// getRouteFD opens a routing socket.
// Returns the file descriptor for the socket or an error if the socket cannot be created.
func getRouteFD() (int, error) {
	fd, err := unix.Socket(unix.AF_ROUTE, unix.SOCK_RAW, unix.AF_UNSPEC)
	if err != nil {
		scope := "route socket - open"
		return -1, annotateUnixError(err, scope)
	}

	return fd, nil
}

// writeMsg marshals and writes a RouteMessage to the specified file descriptor.
// Returns an error if marshaling fails or if the write operation fails.
func writeMsg(fd int, msg *route.RouteMessage) error {
	buf, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("invalid route message given: %w", err)
	}

	if _, err = unix.Write(fd, buf); err != nil {
		scope := "route socket - write"
		return annotateUnixError(err, scope)
	}

	return nil
}

// readMsg reads a response from the routing socket and returns the matching RouteMessage.
// It spawns a goroutine to read from the socket and filters messages based on the provided ID and sequence number.
// The function includes a 10-second timeout to prevent indefinite blocking.
func readMsg(id uintptr, seq, fd int) (*route.RouteMessage, error) {
	type readResult struct {
		msg *route.RouteMessage
		err error
	}

	// Create a channel to receive the result
	resultCh := make(chan readResult, 1)

	go func() {
		var rb [2 << 10]byte
		for {
			n, err := unix.Read(fd, rb[:])
			if err != nil {
				scope := "route socket - read"
				resultCh <- readResult{
					err: annotateUnixError(err, scope),
				}
				return
			}

			// Parse response messages
			msgs, err := route.ParseRIB(route.RIBTypeRoute, rb[:n])
			if err != nil {
				resultCh <- readResult{
					err: fmt.Errorf("failed to parse messages: %w", err),
				}
				return
			}

			if len(msgs) != 1 {
				resultCh <- readResult{
					err: fmt.Errorf("unexpected number of messages received: %d", len(msgs)),
				}
				return
			}

			msg, ok := msgs[0].(*route.RouteMessage)
			// confirm it is a reply to our query
			if !ok || (id != msg.ID && seq != msg.Seq) {
				continue
			}

			resultCh <- readResult{
				msg: msg,
			}

			return
		}
	}()

	select {
	case result := <-resultCh:
		return result.msg, result.err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("route socket - read: timedout")
	}
}

func annotateUnixError(err error, scope string) error {
	var msg string
	switch err {
	case nil:
		return nil
	case unix.ESRCH:
		msg = "route not found"
	// socket errors
	case unix.EACCES:
		msg = "permission denied"
	case unix.EMFILE:
		msg = "file system table full"
	case unix.ENOBUFS:
		msg = "insufficient buffer space"
	case unix.EPERM:
		msg = "insufficient privileges"
	case unix.EPROTOTYPE:
		msg = "socket type not supported"
	// read errors
	case unix.EBADF:
		msg = "invalid socket"
	case unix.ECONNRESET:
		msg = "socket closed"
	case unix.EFAULT:
		msg = "invalid buffer"
	case unix.EIO:
		msg = "I/O error"
	case unix.EBUSY:
		msg = "failed to read from file descriptor"
	case unix.EINVAL:
		msg = "invalid file descriptor"
	case unix.EAGAIN:
		msg = "no data available, try again later"
	default: // unexpected system error; fatal
		msg = "unexpected error"
	}
	if scope != "" {
		msg = scope + ": " + msg
	}
	return fmt.Errorf("%s: %w", msg, err)
}

// Route implements the routing.Router interface to find the best route to the destination IP.
// Returns the outgoing interface, gateway IP, preferred source IP, and any error encountered.
func (r *bsdRouter) Route(dst net.IP) (iface *net.Interface, gateway, preferredSrc net.IP, err error) {
	return r.RouteWithSrc(nil, nil, dst)
}

// RouteWithSrc extends the Route method to allow specifying a source MAC address and IP.
// This enables more precise routing decisions when multiple interfaces are available.
// Returns the outgoing interface, gateway IP, preferred source IP, and any error encountered.
func (r *bsdRouter) RouteWithSrc(MACAddr net.HardwareAddr, src, dst net.IP) (iface *net.Interface, gateway, preferredSrc net.IP, err error) {
	msg, err := composeRouteMsg(r.id, r.getSeq(), MACAddr, src, dst)
	if err != nil {
		return
	}

	var reply *route.RouteMessage
	if reply, err = getRouteMsgReply(msg); err != nil {
		return
	}

	if iface, err = net.InterfaceByIndex(reply.Index); err != nil {
		return
	}

	preferredSrc = toIPAddr(reply.Addrs[5])

	isGatewayRequired := dst.String() != preferredSrc.String()
	if !isGatewayRequired {
		return
	}

	gateway = toIPAddr(reply.Addrs[1])

	return
}

// New returns a new instance of a BSD-specific routing.Router implementation.
// The router is stateless and uses the routing tables of the host system.
func New() (Router, error) {
	r := &bsdRouter{}
	r.id = uintptr(os.Getpid())

	return r, nil
}
