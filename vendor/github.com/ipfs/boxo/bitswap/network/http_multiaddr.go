package network

import (
	"fmt"
	"net"
	"net/url"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// ParsedURL contains the result of parsing an "http" transport multiaddress.
// SNI is set when the multiaddress specifies an SNI value.
type ParsedURL struct {
	URL *url.URL
	SNI string
}

// ExtractHTTPAddress extracts the HTTP schema+host+port from a multiaddress
// and returns a *url.URL and an SNI string if present.
func ExtractHTTPAddress(ma multiaddr.Multiaddr) (ParsedURL, error) {
	components := ma.Protocols()
	var host, port, schema, sni string
	var tls bool

	for _, comp := range components {
		switch comp.Name {
		case "dns", "dns4", "dns6", "ip4", "ip6":
			hostVal, err := ma.ValueForProtocol(comp.Code)
			if err != nil {
				return ParsedURL{}, fmt.Errorf("failed to extract host: %w", err)
			}
			host = hostVal
		case "tcp", "udp":
			portVal, err := ma.ValueForProtocol(comp.Code)
			if err != nil {
				return ParsedURL{}, fmt.Errorf("failed to extract port: %w", err)
			}
			port = portVal
		case "tls":
			tls = true
		case "http":
			schema = "http"
			if tls {
				schema = "https"
			}
		case "https":
			schema = "https"
		case "sni":
			schema = "https"
			sniVal, err := ma.ValueForProtocol(comp.Code)
			if err != nil {
				return ParsedURL{}, fmt.Errorf("failed to extract SNI: %w", err)
			}
			sni = sniVal
		}
	}

	if host == "" || port == "" || schema == "" {
		return ParsedURL{}, fmt.Errorf("multiaddress is missing required components (host/port/schema)")
	}

	// Construct the URL object
	address := fmt.Sprintf("%s://%s:%s", schema, host, port)
	pURL, err := url.Parse(address)
	if err != nil {
		return ParsedURL{}, fmt.Errorf("failed to parse URL: %w", err)
	}

	parsedURL := ParsedURL{
		URL: pURL,
		SNI: sni,
	}

	// Error on addresses which are not https nor local
	ip := net.ParseIP(host)
	if ip != nil {
		if schema != "https" && !(ip.IsLoopback() || ip.IsPrivate()) {
			return parsedURL, fmt.Errorf("multiaddress is not a TLS endpoint nor a local or private IP address")
		}
	} else if schema != "https" {
		return parsedURL, fmt.Errorf("multiaddress is not a TLS endpoint nor a local or private IP address")
	}

	return parsedURL, nil
}

// ExtractURLsFromPeer extracts all HTTP schema+host+port addresses as ParsedURL from a peer.AddrInfo object.
func ExtractURLsFromPeer(info peer.AddrInfo) []ParsedURL {
	var addresses []ParsedURL

	for _, addr := range info.Addrs {
		purl, err := ExtractHTTPAddress(addr)
		if err != nil {
			// Skip invalid or non-HTTP addresses but continue with others
			continue
		}
		addresses = append(addresses, purl)
	}

	return addresses
}

// SplitHTTPAddrs splits a peer.AddrInfo into two: one containing HTTP/HTTPS addresses, and the other containing the rest.
func SplitHTTPAddrs(pi peer.AddrInfo) (httpPeer peer.AddrInfo, otherPeer peer.AddrInfo) {
	httpPeer.ID = pi.ID
	otherPeer.ID = pi.ID

	for _, addr := range pi.Addrs {
		if isHTTPAddress(addr) {
			httpPeer.Addrs = append(httpPeer.Addrs, addr)
		} else {
			otherPeer.Addrs = append(otherPeer.Addrs, addr)
		}
	}

	return
}

// isHTTPAddress checks if a multiaddress is an HTTP or HTTPS address.
func isHTTPAddress(ma multiaddr.Multiaddr) bool {
	protocols := ma.Protocols()
	for _, proto := range protocols {
		if proto.Name == "http" || proto.Name == "https" {
			return true
		}
	}
	return false
}
