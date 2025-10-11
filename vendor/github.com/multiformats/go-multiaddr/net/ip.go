package manet

import (
	"net"

	ma "github.com/multiformats/go-multiaddr"
)

// Loopback Addresses
var (
	// IP4Loopback is the ip4 loopback multiaddr
	IP4Loopback = ma.StringCast("/ip4/127.0.0.1")

	// IP6Loopback is the ip6 loopback multiaddr
	IP6Loopback = ma.StringCast("/ip6/::1")

	// IP4MappedIP6Loopback is the IPv4 Mapped IPv6 loopback address.
	IP4MappedIP6Loopback = ma.StringCast("/ip6/::ffff:127.0.0.1")
)

// Unspecified Addresses (used for )
var (
	IP4Unspecified = ma.StringCast("/ip4/0.0.0.0")
	IP6Unspecified = ma.StringCast("/ip6/::")
)

// IsThinWaist returns whether a Multiaddr starts with "Thin Waist" Protocols.
// This means: /{IP4, IP6}[/{TCP, UDP}]
func IsThinWaist(m ma.Multiaddr) bool {
	m = zoneless(m)
	if m == nil {
		return false
	}
	p := m.Protocols()

	// nothing? not even a waist.
	if len(p) == 0 {
		return false
	}

	if p[0].Code != ma.P_IP4 && p[0].Code != ma.P_IP6 {
		return false
	}

	// only IP? still counts.
	if len(p) == 1 {
		return true
	}

	switch p[1].Code {
	case ma.P_TCP, ma.P_UDP, ma.P_IP4, ma.P_IP6:
		return true
	default:
		return false
	}
}

// IsIPLoopback returns whether a Multiaddr starts with a "Loopback" IP address
// This means either /ip4/127.*.*.*/*, /ip6/::1/*, or /ip6/::ffff:127.*.*.*.*/*,
// or /ip6zone/<any value>/ip6/<one of the preceding ip6 values>/*
func IsIPLoopback(m ma.Multiaddr) bool {
	m = zoneless(m)
	if m == nil {
		return false
	}
	head, _ := splitFirstSlice(m)
	if len(head) == 0 {
		return false
	}
	switch head[0].Code() {
	case ma.P_IP4, ma.P_IP6:
		return net.IP(head[0].RawValue()).IsLoopback()
	}
	return false
}

// IsIP6LinkLocal returns whether a Multiaddr starts with an IPv6 link-local
// multiaddress (with zero or one leading zone). These addresses are non
// routable.
func IsIP6LinkLocal(m ma.Multiaddr) bool {
	m = zoneless(m)
	if m == nil {
		return false
	}
	head, _ := splitFirstSlice(m)
	if len(head) == 0 || head[0].Code() != ma.P_IP6 {
		return false
	}
	ip := net.IP(head[0].RawValue())
	return ip.IsLinkLocalMulticast() || ip.IsLinkLocalUnicast()
}

// IsIPUnspecified returns whether a Multiaddr starts with an Unspecified IP address
// This means either /ip4/0.0.0.0/* or /ip6/::/*
func IsIPUnspecified(m ma.Multiaddr) bool {
	m = zoneless(m)
	if len(m) == 0 {
		return false
	}
	head, _ := splitFirstSlice(m)
	if len(head) == 0 {
		return false
	}
	return net.IP(head[0].RawValue()).IsUnspecified()
}

func splitFirstSlice(m ma.Multiaddr) (ma.Multiaddr, ma.Multiaddr) {
	switch len(m) {
	case 0:
		return nil, nil
	case 1:
		return m, nil
	default:
		return m[:1], m[1:]
	}
}

// If m matches [zone,ip6,...], return [ip6,...]
// else if m matches [], [zone], or [zone,...], return nil
// else return m
func zoneless(m ma.Multiaddr) ma.Multiaddr {
	head, tail := splitFirstSlice(m)
	if len(head) == 0 {
		return nil
	}
	if head[0].Code() == ma.P_IP6ZONE {
		if len(tail) == 0 {
			return nil
		}
		if tail[0].Code() != ma.P_IP6 {
			return nil
		}
		return tail
	} else {
		return m
	}
}

// IsNAT64IPv4ConvertedIPv6Addr returns whether addr is a well-known prefix "64:ff9b::/96" addr
// used for NAT64 Translation. See RFC 6052
func IsNAT64IPv4ConvertedIPv6Addr(addr ma.Multiaddr) bool {
	head, _ := splitFirstSlice(addr)
	return len(head) > 0 && head[0].Code() == ma.P_IP6 &&
		inAddrRange(head[0].RawValue(), nat64)
}
