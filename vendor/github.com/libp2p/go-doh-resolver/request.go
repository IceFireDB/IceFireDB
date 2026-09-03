package doh

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/miekg/dns"

	logging "github.com/ipfs/go-log/v2"
)

const (
	dohMimeType = "application/dns-message"
)

var log = logging.Logger("doh")

func doRequest(ctx context.Context, url string, m *dns.Msg) (*dns.Msg, error) {
	data, err := m.Pack()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", dohMimeType)
	req.Header.Set("Accept", dohMimeType)

	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %q [%d] from %q", resp.Status, resp.StatusCode, url)
	}

	if ct := resp.Header.Get("Content-Type"); ct != dohMimeType {
		return nil, fmt.Errorf("unexpected Content-Type %q", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	r := new(dns.Msg)
	if err := r.Unpack(body); err != nil {
		return nil, err
	}

	return r, nil
}

func doRequestA(ctx context.Context, url string, domain string) ([]net.IPAddr, uint32, error) {
	fqdn := dns.Fqdn(domain)

	m := new(dns.Msg)
	m.SetQuestion(fqdn, dns.TypeA)

	r, err := doRequest(ctx, url, m)
	if err != nil {
		return nil, 0, err
	}

	var ttl uint32
	result := make([]net.IPAddr, 0, len(r.Answer))
	first := true
	for _, rr := range r.Answer {
		switch v := rr.(type) {
		case *dns.A:
			result = append(result, net.IPAddr{IP: v.A})
			// RFC 2181 wants differing TTLs in an RRset treated as the lowest
			// one; a genuine TTL of 0 must not be mistaken for unset.
			if first || v.Hdr.Ttl < ttl {
				ttl = v.Hdr.Ttl
				first = false
			}
		default:
			log.Warnf("unexpected DNS resource record %+v", rr)
		}
	}

	return result, ttl, nil
}

func doRequestAAAA(ctx context.Context, url string, domain string) ([]net.IPAddr, uint32, error) {
	fqdn := dns.Fqdn(domain)

	m := new(dns.Msg)
	m.SetQuestion(fqdn, dns.TypeAAAA)

	r, err := doRequest(ctx, url, m)
	if err != nil {
		return nil, 0, err
	}

	var ttl uint32
	result := make([]net.IPAddr, 0, len(r.Answer))
	first := true
	for _, rr := range r.Answer {
		switch v := rr.(type) {
		case *dns.AAAA:
			result = append(result, net.IPAddr{IP: v.AAAA})
			if first || v.Hdr.Ttl < ttl {
				ttl = v.Hdr.Ttl
				first = false
			}

		default:
			log.Warnf("unexpected DNS resource record %+v", rr)
		}
	}

	return result, ttl, nil
}

func doRequestTXT(ctx context.Context, url string, domain string) ([]string, uint32, error) {
	fqdn := dns.Fqdn(domain)

	m := new(dns.Msg)
	m.SetQuestion(fqdn, dns.TypeTXT)

	r, err := doRequest(ctx, url, m)
	if err != nil {
		return nil, 0, err
	}

	var ttl uint32
	var result []string
	first := true
	for _, rr := range r.Answer {
		switch v := rr.(type) {
		case *dns.TXT:
			result = append(result, v.Txt...)
			if first || v.Hdr.Ttl < ttl {
				ttl = v.Hdr.Ttl
				first = false
			}

		default:
			log.Warnf("unexpected DNS resource record %+v", rr)
		}
	}

	return result, ttl, nil
}
