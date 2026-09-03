package network

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// ParsedURL contains the result of parsing an "http" transport multiaddress.
// SNI is set when the multiaddress specifies an SNI value.
type ParsedURL struct {
	Multiaddress multiaddr.Multiaddr
	URL          *url.URL
	SNI          string
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

	// Default well-known ports when schema is set but port is missing.
	// This handles shorthand multiaddrs like /dns/example.com/https
	// where the /tcp/443 component is omitted.
	if port == "" {
		switch schema {
		case "https":
			port = "443"
		case "http":
			port = "80"
		}
	}

	if host == "" || port == "" || schema == "" {
		return ParsedURL{}, fmt.Errorf("multiaddress is missing required components (host/port/schema)")
	}

	// Construct the URL object.
	// Omit the port when it matches the schema default to produce
	// canonical URLs. This ensures the HTTP client sends a clean Host
	// header (e.g. "example.com" instead of "example.com:443") and
	// avoids sharding HTTP caches on providers that key on Host,
	// as well as issues with reverse proxies or redirects that fail
	// to match when an explicit default port is present.
	var address string
	if (schema == "https" && port == "443") || (schema == "http" && port == "80") {
		address = fmt.Sprintf("%s://%s", schema, hostInURL(host))
	} else {
		address = fmt.Sprintf("%s://%s", schema, net.JoinHostPort(host, port))
	}
	pURL, err := url.Parse(address)
	if err != nil {
		return ParsedURL{}, fmt.Errorf("failed to parse URL: %w", err)
	}

	parsedURL := ParsedURL{
		Multiaddress: ma,
		URL:          pURL,
		SNI:          sni,
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

// hostInURL renders host for use in a URL authority that carries no port. An
// IPv6 literal has to be bracketed there (RFC 3986, section 3.2.2), otherwise
// its colons read as a port separator: url.Parse rejects the result outright
// under Go 1.26+, and older parsers silently split the address at the last
// colon. net.JoinHostPort covers the case where a port is present.
func hostInURL(host string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]"
	}
	return host
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
