package webrtc

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

// WebRTCTransport implements the raft.StreamLayer interface for WebRTC communication
type WebRTCTransport struct {
	localAddr net.Addr
	peerConns map[string]*webrtc.PeerConnection
	conns     chan net.Conn
	closeCh   chan struct{}
	mu        sync.RWMutex
	closed    bool
}

// NewWebRTCTransport creates a new WebRTC transport
func NewWebRTCTransport(localAddr string) (*WebRTCTransport, error) {
	addr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve local address: %w", err)
	}

	transport := &WebRTCTransport{
		localAddr: addr,
		peerConns: make(map[string]*webrtc.PeerConnection),
		conns:     make(chan net.Conn, 100),
		closeCh:   make(chan struct{}),
	}

	return transport, nil
}

// Accept waits for and returns the next connection to the listener
func (t *WebRTCTransport) Accept() (net.Conn, error) {
	select {
	case conn := <-t.conns:
		return conn, nil
	case <-t.closeCh:
		return nil, io.EOF
	}
}

// Close closes the listener
func (t *WebRTCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true
	close(t.closeCh)

	// Close all peer connections
	for _, pc := range t.peerConns {
		pc.Close()
	}

	return nil
}

// Addr returns the listener's network address
func (t *WebRTCTransport) Addr() net.Addr {
	return t.localAddr
}

// Dial creates a new WebRTC connection to the target address
func (t *WebRTCTransport) Dial(address string, timeout time.Duration) (net.Conn, error) {

	// Create a new peer connection
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	}

	peerConnection, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create peer connection: %w", err)
	}

	// Create a data channel for Raft communication
	dataChannel, err := peerConnection.CreateDataChannel("raft", nil)
	if err != nil {
		peerConnection.Close()
		return nil, fmt.Errorf("failed to create data channel: %w", err)
	}

	// Create offer
	offer, err := peerConnection.CreateOffer(nil)
	if err != nil {
		peerConnection.Close()
		return nil, fmt.Errorf("failed to create offer: %w", err)
	}

	err = peerConnection.SetLocalDescription(offer)
	if err != nil {
		peerConnection.Close()
		return nil, fmt.Errorf("failed to set local description: %w", err)
	}

	// In a real implementation, we would exchange SDP with the remote peer
	// For now, we'll create a simple connection that can be used for testing

	conn := &WebRTCConn{
		dataChannel: dataChannel,
		peerConn:    peerConnection,
		localAddr:   t.localAddr,
		remoteAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 11001},
	}

	return conn, nil
}

// WebRTCConn implements net.Conn for WebRTC data channels
type WebRTCConn struct {
	dataChannel *webrtc.DataChannel
	peerConn    *webrtc.PeerConnection
	localAddr   net.Addr
	remoteAddr  net.Addr
	reader      *bufio.Reader
	writer      *bufio.Writer
	closeCh     chan struct{}
	mu          sync.RWMutex
	closed      bool
}

// Read reads data from the WebRTC data channel
func (c *WebRTCConn) Read(b []byte) (n int, err error) {
	// Implementation would handle reading from data channel
	// For now, return EOF as this is a stub implementation
	return 0, io.EOF
}

// Write writes data to the WebRTC data channel
func (c *WebRTCConn) Write(b []byte) (n int, err error) {
	// Implementation would handle writing to data channel
	// For now, return success as this is a stub implementation
	return len(b), nil
}

// Close closes the connection
func (c *WebRTCConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	close(c.closeCh)
	if c.peerConn != nil {
		c.peerConn.Close()
	}
	return nil
}

// LocalAddr returns the local network address
func (c *WebRTCConn) LocalAddr() net.Addr {
	return c.localAddr
}

// RemoteAddr returns the remote network address
func (c *WebRTCConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

// SetDeadline sets the read and write deadlines
func (c *WebRTCConn) SetDeadline(t time.Time) error {
	return nil // Not implemented for WebRTC
}

// SetReadDeadline sets the read deadline
func (c *WebRTCConn) SetReadDeadline(t time.Time) error {
	return nil // Not implemented for WebRTC
}

// SetWriteDeadline sets the write deadline
func (c *WebRTCConn) SetWriteDeadline(t time.Time) error {
	return nil // Not implemented for WebRTC
}
