package multiaddr

import (
	"encoding/binary"
	"fmt"
	"net/netip"

	"github.com/multiformats/go-multiaddr/x/meg"
)

func CaptureAddrPort(network *string, ipPort *netip.AddrPort) (capturePattern meg.Pattern) {
	var ipOnly netip.Addr
	capturePort := func(s meg.Matchable) error {
		switch s.Code() {
		case P_UDP:
			*network = "udp"
		case P_TCP:
			*network = "tcp"
		default:
			return fmt.Errorf("invalid network: %s", s.Value())
		}

		port := binary.BigEndian.Uint16(s.RawValue())
		*ipPort = netip.AddrPortFrom(ipOnly, port)
		return nil
	}

	pattern := meg.Cat(
		meg.Or(
			meg.CaptureWithF(P_IP4, func(s meg.Matchable) error {
				var ok bool
				ipOnly, ok = netip.AddrFromSlice(s.RawValue())
				if !ok {
					return fmt.Errorf("invalid ip4 address: %s", s.Value())
				}
				return nil
			}),
			meg.CaptureWithF(P_IP6, func(s meg.Matchable) error {
				var ok bool
				ipOnly, ok = netip.AddrFromSlice(s.RawValue())
				if !ok {
					return fmt.Errorf("invalid ip6 address: %s", s.Value())
				}
				return nil
			}),
		),
		meg.Or(
			meg.CaptureWithF(P_UDP, capturePort),
			meg.CaptureWithF(P_TCP, capturePort),
		),
	)

	return pattern
}
