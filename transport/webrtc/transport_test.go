package webrtc

import (
	"net"
	"testing"
	"time"
)

func TestWebRTCTransport_New(t *testing.T) {
	tests := []struct {
		name      string
		localAddr string
		wantErr   bool
	}{
		{
			name:      "valid UDP address",
			localAddr: "127.0.0.1:0",
			wantErr:   false,
		},
		{
			name:      "invalid address",
			localAddr: "invalid-address",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport, err := NewWebRTCTransport(tt.localAddr)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewWebRTCTransport() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && transport == nil {
				t.Error("NewWebRTCTransport() returned nil transport without error")
			}
		})
	}
}

func TestWebRTCTransport_Addr(t *testing.T) {
	transport, err := NewWebRTCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	addr := transport.Addr()
	if addr == nil {
		t.Error("Addr() returned nil")
	}

	// Should be a UDP address
	if _, ok := addr.(*net.UDPAddr); !ok {
		t.Errorf("Addr() returned %T, expected *net.UDPAddr", addr)
	}
}

func TestWebRTCTransport_Close(t *testing.T) {
	transport, err := NewWebRTCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// First close should succeed
	err = transport.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Subsequent closes should also succeed
	err = transport.Close()
	if err != nil {
		t.Errorf("Second Close() error = %v", err)
	}
}

func TestWebRTCTransport_Accept_AfterClose(t *testing.T) {
	transport, err := NewWebRTCTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// Close the transport
	err = transport.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Accept after close should return error
	conn, err := transport.Accept()
	if err == nil {
		conn.Close()
		t.Error("Accept() after close should return error")
	}
}

func TestWebRTCConn_Interface(t *testing.T) {
	// Test that WebRTCConn implements net.Conn interface
	var _ net.Conn = &WebRTCConn{}
}

func TestWebRTCConn_Methods(t *testing.T) {
	// Create a minimal WebRTCConn for testing
	conn := &WebRTCConn{
		localAddr:  &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0},
		remoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 11001},
		closeCh:    make(chan struct{}),
	}

	// Test LocalAddr
	localAddr := conn.LocalAddr()
	if localAddr == nil {
		t.Error("LocalAddr() returned nil")
	}

	// Test RemoteAddr
	remoteAddr := conn.RemoteAddr()
	if remoteAddr == nil {
		t.Error("RemoteAddr() returned nil")
	}

	// Test SetDeadline (should not panic)
	err := conn.SetDeadline(time.Now().Add(time.Second))
	if err != nil {
		t.Errorf("SetDeadline() error = %v", err)
	}

	// Test SetReadDeadline (should not panic)
	err = conn.SetReadDeadline(time.Now().Add(time.Second))
	if err != nil {
		t.Errorf("SetReadDeadline() error = %v", err)
	}

	// Test SetWriteDeadline (should not panic)
	err = conn.SetWriteDeadline(time.Now().Add(time.Second))
	if err != nil {
		t.Errorf("SetWriteDeadline() error = %v", err)
	}

	// Test Close
	err = conn.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Test double close
	err = conn.Close()
	if err != nil {
		t.Errorf("Second Close() error = %v", err)
	}
}
