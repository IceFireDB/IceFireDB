// Copyright 2012 Google, Inc. All rights reserved.
//
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

//go:build linux

// Generate a local routing table structure following the code at
// https://github.com/google/gopacket/blob/master/routing/routing.go

package netroute

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"sync/atomic"
	"syscall"
	"unsafe"
)

var nlSequence uint32

type linuxRouter struct{}

func New() (Router, error) {
	return &linuxRouter{}, nil
}

// Route implements Router.
func (l *linuxRouter) Route(dst net.IP) (iface *net.Interface, gateway net.IP, preferredSrc net.IP, err error) {
	return l.RouteWithSrc(nil, nil, dst)
}

// RouteWithSrc implements Router.
func (l *linuxRouter) RouteWithSrc(input net.HardwareAddr, src net.IP, dst net.IP) (iface *net.Interface, gateway net.IP, preferredSrc net.IP, err error) {
	if dst == nil || dst.IsUnspecified() {
		return nil, nil, nil, errors.New("destination IP must be specified")
	}

	// trim bytes if this is a v4 addr
	if v4 := dst.To4(); v4 != nil {
		dst = v4
	}

	dstFamily := addressFamily(dst)
	if src != nil && !src.IsUnspecified() {
		srcFamily := addressFamily(src)
		if srcFamily != dstFamily {
			return nil, nil, nil, fmt.Errorf("source %q and destination %q use different address families", src.String(), dst.String())
		}
	}

	var oif int
	if len(input) > 0 {
		ifaces, err := net.Interfaces()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("list interfaces: %w", err)
		}
		for i := range ifaces {
			iface := ifaces[i]
			if bytes.Equal(iface.HardwareAddr, input) {
				oif = iface.Index
				break
			}
		}
		if oif == 0 {
			return nil, nil, nil, fmt.Errorf("no interface with address %s found", input.String())
		}
	}

	fd, err := syscall.Socket(syscall.AF_NETLINK, syscall.SOCK_DGRAM, syscall.NETLINK_ROUTE)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open netlink socket: %w", err)
	}
	defer syscall.Close(fd)

	sa := &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK}
	if err := syscall.Bind(fd, sa); err != nil {
		return nil, nil, nil, fmt.Errorf("bind netlink socket: %w", err)
	}

	seq := atomic.AddUint32(&nlSequence, 1)
	request, err := buildRouteRequest(dstFamily, dst, src, oif, seq, uint32(os.Getpid()))
	if err != nil {
		return nil, nil, nil, err
	}

	if err := syscall.Sendto(fd, request, 0, &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK}); err != nil {
		return nil, nil, nil, fmt.Errorf("send netlink request: %w", err)
	}

	route, err := readRouteResponse(fd, seq)
	if err != nil {
		return nil, nil, nil, err
	}

	outIface, err := net.InterfaceByIndex(int(route.OutputIface))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get interface by index: %w", err)
	}

	if len(input) > 0 && iface != nil && len(iface.HardwareAddr) > 0 && !bytes.Equal(iface.HardwareAddr, input) {
		return nil, nil, nil, fmt.Errorf("route resolved to interface %s (%s), expected %s", iface.Name, iface.HardwareAddr, input)
	}

	var srcIP net.IP
	if len(route.PrefSrc) > 0 && !route.PrefSrc.IsUnspecified() {
		srcIP = route.PrefSrc
	} else if route.Src != nil {
		srcIP = route.Src.IP
	} else {
		return nil, nil, nil, fmt.Errorf("no source IP found")
	}

	// copyIP so we don't leak a reference to our working buffer
	return outIface, copyIP(route.Gateway), copyIP(srcIP), nil
}

func buildRouteRequest(family int, dst []byte, src []byte, oif int, seq, pid uint32) ([]byte, error) {
	bodyBuf := new(bytes.Buffer)

	rtm := syscall.RtMsg{
		Family:  uint8(family),
		Dst_len: uint8(len(dst) * 8),
		Src_len: uint8(len(src) * 8),
		Table:   syscall.RT_TABLE_UNSPEC,
	}
	if err := binary.Write(bodyBuf, binary.NativeEndian, rtm); err != nil {
		return nil, fmt.Errorf("marshal rtmsg: %w", err)
	}
	if len(dst) > 0 {
		if err := writeRouteAttr(bodyBuf, syscall.RTA_DST, dst); err != nil {
			return nil, err
		}
	}
	if len(src) > 0 {
		if err := writeRouteAttr(bodyBuf, syscall.RTA_SRC, src); err != nil {
			return nil, err
		}
	}
	if oif != 0 {
		oifBytes := make([]byte, 4)
		binary.NativeEndian.PutUint32(oifBytes, uint32(oif))
		if err := writeRouteAttr(bodyBuf, syscall.RTA_OIF, oifBytes); err != nil {
			return nil, err
		}
	}

	header := syscall.NlMsghdr{
		Len:   uint32(syscall.NLMSG_HDRLEN + bodyBuf.Len()),
		Type:  uint16(syscall.RTM_GETROUTE),
		Flags: uint16(syscall.NLM_F_REQUEST),
		Seq:   seq,
		Pid:   pid,
	}

	msgBuf := new(bytes.Buffer)
	if err := binary.Write(msgBuf, binary.NativeEndian, header); err != nil {
		return nil, fmt.Errorf("marshal nlmsghdr: %w", err)
	}
	if _, err := msgBuf.Write(bodyBuf.Bytes()); err != nil {
		return nil, fmt.Errorf("assemble netlink request: %w", err)
	}
	return msgBuf.Bytes(), nil
}

func writeRouteAttr(buf *bytes.Buffer, attrType uint16, payload []byte) error {
	attrLen := uint16(syscall.SizeofRtAttr + len(payload))
	attr := syscall.RtAttr{
		Len:  attrLen,
		Type: attrType,
	}
	if err := binary.Write(buf, binary.NativeEndian, attr); err != nil {
		return fmt.Errorf("marshal rtattr: %w", err)
	}
	if _, err := buf.Write(payload); err != nil {
		return fmt.Errorf("write rtattr payload: %w", err)
	}

	const nlMsghdrLenAlignment = 4
	padLen := alignTo(attrLen, nlMsghdrLenAlignment) - int(attrLen)
	if padLen > 0 {
		_, _ = buf.Write(make([]byte, padLen))
	}
	return nil
}

func alignTo(length uint16, alignment int) int {
	l := int(length)
	return (l + alignment - 1) & ^(alignment - 1)
}

func readRouteResponse(fd int, seq uint32) (*rtInfo, error) {
	buf := make([]byte, 1<<16)
	for {
		n, _, err := syscall.Recvfrom(fd, buf, 0)
		if err != nil {
			return nil, fmt.Errorf("receive netlink response: %w", err)
		}

		msgs, err := syscall.ParseNetlinkMessage(buf[:n])
		if err != nil {
			return nil, fmt.Errorf("parse netlink response: %w", err)
		}

		for _, m := range msgs {
			if m.Header.Seq != seq {
				continue
			}
			switch m.Header.Type {
			case syscall.NLMSG_ERROR:
				if err := parseNetlinkError(m.Data); err != nil {
					return nil, fmt.Errorf("netlink error: %w", err)
				}
			case syscall.NLMSG_DONE:
				return nil, errors.New("route lookup returned no result")
			case syscall.RTM_NEWROUTE:
				var routeInfo rtInfo
				if err := routeInfo.parse(&m); err != nil {
					return nil, err
				}
				if routeInfo.Dst == nil && routeInfo.Src == nil && routeInfo.Gateway == nil {
					continue
				}
				return &routeInfo, nil
			}
		}
	}
}

func parseNetlinkError(data []byte) error {
	if len(data) < syscall.SizeofNlMsgerr {
		return fmt.Errorf("short netlink error payload: %d", len(data))
	}
	var msg syscall.NlMsgerr
	reader := bytes.NewReader(data[:syscall.SizeofNlMsgerr])
	if err := binary.Read(reader, binary.NativeEndian, &msg); err != nil {
		return fmt.Errorf("decode nlmsgerr: %w", err)
	}
	if msg.Error == 0 {
		return nil
	}

	// Error is negative errno or 0 for acknowledgements
	return syscall.Errno(-msg.Error)
}

func (routeInfo *rtInfo) parse(msg *syscall.NetlinkMessage) error {
	rt := (*syscall.RtMsg)(unsafe.Pointer(&msg.Data[0]))
	if rt.Family != syscall.AF_INET && rt.Family != syscall.AF_INET6 {
		return errors.New("unsupported address family")
	}

	attrs, err := syscall.ParseNetlinkRouteAttr(msg)
	if err != nil {
		return err
	}
	for _, attr := range attrs {
		switch attr.Attr.Type {
		case syscall.RTA_DST:
			routeInfo.Dst = &net.IPNet{
				IP:   net.IP(attr.Value),
				Mask: net.CIDRMask(int(rt.Dst_len), len(attr.Value)*8),
			}
		case syscall.RTA_SRC:
			routeInfo.Src = &net.IPNet{
				// Copy the IP so we don't keep a reference to this buffer
				IP:   net.IP(attr.Value),
				Mask: net.CIDRMask(int(rt.Src_len), len(attr.Value)*8),
			}
		case syscall.RTA_GATEWAY:
			routeInfo.Gateway = net.IP(attr.Value)
		case syscall.RTA_PREFSRC:
			routeInfo.PrefSrc = net.IP(attr.Value)
		case syscall.RTA_IIF:
			routeInfo.InputIface = *(*uint32)(unsafe.Pointer(&attr.Value[0]))
		case syscall.RTA_OIF:
			routeInfo.OutputIface = *(*uint32)(unsafe.Pointer(&attr.Value[0]))
		case syscall.RTA_PRIORITY:
			routeInfo.Priority = *(*uint32)(unsafe.Pointer(&attr.Value[0]))
		}
	}
	return nil
}

func addressFamily(ip net.IP) int {
	if ip.To4() != nil {
		return syscall.AF_INET
	}
	return syscall.AF_INET6
}

func copyIP(ip net.IP) net.IP {
	if len(ip) == 0 {
		return nil
	}
	out := slices.Clone(ip)
	if v4 := out.To4(); v4 != nil {
		out = v4
	}
	return out
}
