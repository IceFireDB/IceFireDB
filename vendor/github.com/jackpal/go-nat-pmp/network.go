package natpmp

import (
	"fmt"
	"net"
	"time"
)

const nAT_PMP_PORT = 5351
const nAT_TRIES = 9
const nAT_INITIAL_MS = 250

// A caller that implements the NAT-PMP RPC protocol.
type network struct {
	gateway net.IP
	local   net.IP
	port    int
}

func (n *network) call(msg []byte, timeout time.Duration) (result []byte, err error) {
	var server net.UDPAddr
	server.IP = n.gateway
	port := n.port
	if port == 0 {
		port = nAT_PMP_PORT
	}
	server.Port = port

	var localAddr *net.UDPAddr
	if len(n.local) > 0 {
		localAddr = &net.UDPAddr{IP: n.local}
	}
	conn, err := net.DialUDP("udp", localAddr, &server)
	if err != nil {
		return
	}
	defer conn.Close()

	// 16 bytes is the maximum result size.
	result = make([]byte, 16)

	var finalTimeout time.Time
	if timeout != 0 {
		finalTimeout = time.Now().Add(timeout)
	}

	needNewDeadline := true

	var tries uint
	for tries = 0; (tries < nAT_TRIES && finalTimeout.IsZero()) || time.Now().Before(finalTimeout); {
		if needNewDeadline {
			shift := tries
			if shift > 8 {
				shift = 8
			}
			nextDeadline := time.Now().Add((nAT_INITIAL_MS << shift) * time.Millisecond)
			err = conn.SetDeadline(minTime(nextDeadline, finalTimeout))
			if err != nil {
				return
			}
			needNewDeadline = false
		}
		_, err = conn.Write(msg)
		if err != nil {
			return
		}
		var bytesRead int
		var remoteAddr *net.UDPAddr
		bytesRead, remoteAddr, err = conn.ReadFromUDP(result)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				tries++
				needNewDeadline = true
				continue
			}
			return
		}
		if !remoteAddr.IP.Equal(n.gateway) {
			// Ignore this packet.
			// Continue without increasing retransmission timeout or deadline.
			continue
		}
		// Trim result to actual number of bytes received
		if bytesRead < len(result) {
			result = result[:bytesRead]
		}
		return
	}
	err = fmt.Errorf("Timed out trying to contact gateway")
	return
}

func minTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	if a.Before(b) {
		return a
	}
	return b
}
