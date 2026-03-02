package client

import (
	"errors"
	"fmt"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multibase"
)

// RFC 1035 max length for a DNS label. IPv6 addresses never exceed this
// after escaping (longest case is 41 chars), but we validate defensively.
const maxDNSLabelLength = 63

// ForgeAddrInfo contains the components needed to build a forge domain address
type ForgeAddrInfo struct {
	EscapedIP    string // RFC-compliant DNS label for the IP address
	IPVersion    string // "4" or "6"
	IPMaStr      string // Original IP component string from multiaddr (e.g., "/ip4/1.2.3.4")
	TCPPort      string // TCP port number
	PeerIDBase36 string // Base36-encoded peer ID
}

// ExtractForgeAddrInfo extracts and formats address components from a libp2p multiaddr
// for use in forge domain names. It ensures RFC-compliant DNS labels by properly
// escaping IPv6 addresses with leading/trailing zero handling.
func ExtractForgeAddrInfo(addr multiaddr.Multiaddr, peerID peer.ID) (*ForgeAddrInfo, error) {
	info := &ForgeAddrInfo{}

	// Extract peer ID as base36
	info.PeerIDBase36 = peer.ToCid(peerID).Encode(multibase.MustNewEncoder(multibase.Base36))

	// Parse multiaddr components
	var ipAddr string
	var found bool

	index := 0
	multiaddr.ForEach(addr, func(c multiaddr.Component) bool {
		switch index {
		case 0: // IP component
			switch c.Protocol().Code {
			case multiaddr.P_IP4:
				info.IPVersion = "4"
				info.IPMaStr = c.String()
				ipAddr = c.Value()
				info.EscapedIP = strings.ReplaceAll(ipAddr, ".", "-")
				found = true
			case multiaddr.P_IP6:
				info.IPVersion = "6"
				info.IPMaStr = c.String()
				ipAddr = c.Value()
				info.EscapedIP = escapeIPv6ForDNS(ipAddr)
				found = true
			default:
				return false
			}
		case 1: // TCP component
			if c.Protocol().Code != multiaddr.P_TCP {
				return false
			}
			info.TCPPort = c.Value()
		default:
			return false
		}
		index++
		return true
	})

	if !found || info.TCPPort == "" {
		return nil, errors.New("invalid multiaddr: missing IP or TCP component")
	}

	// Sanity check: valid IPv6 never exceeds this, but catch edge cases from fuzz tests.
	if len(info.EscapedIP) > maxDNSLabelLength {
		return nil, fmt.Errorf("DNS label too long: %d characters (max %d)", len(info.EscapedIP), maxDNSLabelLength)
	}

	return info, nil
}

// escapeIPv6ForDNS converts an IPv6 address to an RFC-compliant DNS label
// by replacing colons with dashes and adding leading/trailing zeros as needed
// to ensure the label doesn't start or end with a hyphen.
func escapeIPv6ForDNS(ipv6Addr string) string {
	escapedIP := strings.ReplaceAll(ipv6Addr, ":", "-")

	// RFC 1035: DNS labels cannot start with a hyphen
	if len(escapedIP) > 0 && escapedIP[0] == '-' {
		escapedIP = "0" + escapedIP
	}

	// RFC 1035: DNS labels cannot end with a hyphen
	if len(escapedIP) > 0 && escapedIP[len(escapedIP)-1] == '-' {
		escapedIP = escapedIP + "0"
	}

	return escapedIP
}

// BuildShortForgeMultiaddr constructs a short forge multiaddr using the production address format
// Format: /dns{4|6}/<escaped-ip>.<peer-id>.<forge-domain>/tcp/<port>/tls/ws
func BuildShortForgeMultiaddr(forgeAddrInfo *ForgeAddrInfo, forgeDomain string) string {
	return fmt.Sprintf("/dns%s/%s.%s.%s/tcp/%s/tls/ws",
		forgeAddrInfo.IPVersion, forgeAddrInfo.EscapedIP, forgeAddrInfo.PeerIDBase36, forgeDomain, forgeAddrInfo.TCPPort)
}

// BuildLongForgeMultiaddr constructs a long forge multiaddr using the production address format
// Format: /{ip4|ip6}/<ip>/tcp/<port>/tls/sni/<escaped-ip>.<peer-id>.<forge-domain>/ws
func BuildLongForgeMultiaddr(forgeAddrInfo *ForgeAddrInfo, forgeDomain string) string {
	return fmt.Sprintf("%s/tcp/%s/tls/sni/%s.%s.%s/ws",
		forgeAddrInfo.IPMaStr, forgeAddrInfo.TCPPort, forgeAddrInfo.EscapedIP, forgeAddrInfo.PeerIDBase36, forgeDomain)
}
