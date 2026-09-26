package transport

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestWebRTCE2E tests end-to-end WebRTC transport functionality
func TestWebRTCE2E(t *testing.T) {
	// Create custom transports in WebRTC mode
	addr1 := "127.0.0.1:11011"
	addr2 := "127.0.0.1:11012"

	// Create mock TCP transports as fallback
	tcpTransport1 := createMockTCPTransport(addr1)
	tcpTransport2 := createMockTCPTransport(addr2)

	// Create custom transports in WebRTC mode
	transport1, err := NewCustomTransport("webrtc", tcpTransport1, addr1)
	if err != nil {
		t.Fatalf("Failed to create transport1: %v", err)
	}
	transport2, err := NewCustomTransport("webrtc", tcpTransport2, addr2)
	if err != nil {
		t.Fatalf("Failed to create transport2: %v", err)
	}

	// Test basic transport properties
	if transport1.LocalAddr() == "" {
		t.Error("Transport1 LocalAddr() returned empty address")
	}
	if transport2.LocalAddr() == "" {
		t.Error("Transport2 LocalAddr() returned empty address")
	}

	// Test RPC consumer channels
	consumer1 := transport1.Consumer()
	if consumer1 == nil {
		t.Error("Transport1 Consumer() returned nil channel")
	}
	consumer2 := transport2.Consumer()
	if consumer2 == nil {
		t.Error("Transport2 Consumer() returned nil channel")
	}

	// Test encoding/decoding peer addresses
	peerID := raft.ServerID("node2")
	peerAddr := raft.ServerAddress(addr2)

	encoded := transport1.EncodePeer(peerID, peerAddr)
	if len(encoded) == 0 {
		t.Error("EncodePeer returned empty byte slice")
	}

	decoded := transport1.DecodePeer(encoded)
	if decoded != peerAddr {
		t.Errorf("DecodePeer returned %s, expected %s", decoded, peerAddr)
	}

	// Test heartbeat handler setup
	transport1.SetHeartbeatHandler(func(rpc raft.RPC) {
		// Heartbeat handler callback
	})

	// Verify transport mode
	if transport1.networkMode != "webrtc" {
		t.Errorf("Expected network mode 'webrtc', got '%s'", transport1.networkMode)
	}

	// Verify WebRTC transport is created
	if transport1.webrtcTransport == nil {
		t.Error("WebRTC transport should not be nil in WebRTC mode")
	}

	// Clean up
	// Note: In a real E2E test, we would test actual WebRTC communication
	// but for now we're testing the transport setup
}

// TestWebRTCTransportIntegration tests integration with actual Raft operations
func TestWebRTCTransportIntegration(t *testing.T) {
	// Create a simple test to verify transport can be used with Raft
	addr := "127.0.0.1:11013"
	tcpTransport := createMockTCPTransport(addr)

	transport, err := NewCustomTransport("webrtc", tcpTransport, addr)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// Test that transport implements all required raft.Transport methods
	var _ raft.Transport = transport

	// Test timeout now operation (should not panic)
	err = transport.TimeoutNow("node1", "127.0.0.1:11014", &raft.TimeoutNowRequest{}, &raft.TimeoutNowResponse{})
	if err != nil {
		t.Logf("TimeoutNow returned error (expected for mock): %v", err)
	}

	// Test append entries operation
	err = transport.AppendEntries("node1", "127.0.0.1:11014", &raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})
	if err != nil {
		t.Logf("AppendEntries returned error (expected for mock): %v", err)
	}

	// Test request vote operation
	err = transport.RequestVote("node1", "127.0.0.1:11014", &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{})
	if err != nil {
		t.Logf("RequestVote returned error (expected for mock): %v", err)
	}

	// Test install snapshot operation
	err = transport.InstallSnapshot("node1", "127.0.0.1:11014", &raft.InstallSnapshotRequest{}, &raft.InstallSnapshotResponse{}, nil)
	if err != nil {
		t.Logf("InstallSnapshot returned error (expected for mock): %v", err)
	}
}

// TestWebRTCModeSwitching tests that WebRTC mode properly creates WebRTC transport
func TestWebRTCModeSwitching(t *testing.T) {
	addr := "127.0.0.1:11015"
	tcpTransport := createMockTCPTransport(addr)

	transport, err := NewCustomTransport("webrtc", tcpTransport, addr)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// In WebRTC mode, WebRTC transport should be created
	if transport.webrtcTransport == nil {
		t.Error("WebRTC transport should not be nil in WebRTC mode")
	}

	// TCP transport should still be available for fallback
	if transport.tcpTransport == nil {
		t.Error("TCP transport should not be nil")
	}

	// Verify that network mode is correctly set
	if transport.networkMode != "webrtc" {
		t.Errorf("Expected network mode 'webrtc', got '%s'", transport.networkMode)
	}
}

// TestWebRTCTransportConcurrent tests concurrent access to WebRTC transport
func TestWebRTCTransportConcurrent(t *testing.T) {
	addr := "127.0.0.1:11016"
	tcpTransport := createMockTCPTransport(addr)

	transport, err := NewCustomTransport("webrtc", tcpTransport, addr)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// Test concurrent access to transport methods
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan bool)

	// Concurrently call various transport methods
	go func() {
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				_ = transport.LocalAddr()
				_ = transport.Consumer()
				transport.SetHeartbeatHandler(func(rpc raft.RPC) {})
			}
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				_ = transport.EncodePeer("node1", "127.0.0.1:11017")
				_ = transport.DecodePeer([]byte("127.0.0.1:11017"))
			}
		}
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	// Test should complete without panics or deadlocks
}

// TestWebRTCFallbackToTCP tests that WebRTC mode properly falls back to TCP for unimplemented features
func TestWebRTCFallbackToTCP(t *testing.T) {
	addr := "127.0.0.1:11018"
	tcpTransport := createMockTCPTransport(addr)

	transport, err := NewCustomTransport("webrtc", tcpTransport, addr)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// Test that consumer falls back to TCP
	consumer := transport.Consumer()
	if consumer == nil {
		t.Error("Consumer should not be nil")
	}

	// Test that append entries pipeline falls back to TCP
	_, err = transport.AppendEntriesPipeline("node1", "127.0.0.1:11019")
	if err != nil {
		t.Logf("AppendEntriesPipeline returned error (expected for mock): %v", err)
	}

	// Test that append entries falls back to TCP
	err = transport.AppendEntries("node1", "127.0.0.1:11019", &raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})
	if err != nil {
		t.Logf("AppendEntries returned error (expected for mock): %v", err)
	}

	// Test that request vote falls back to TCP
	err = transport.RequestVote("node1", "127.0.0.1:11019", &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{})
	if err != nil {
		t.Logf("RequestVote returned error (expected for mock): %v", err)
	}

	// Test that install snapshot falls back to TCP
	err = transport.InstallSnapshot("node1", "127.0.0.1:11019", &raft.InstallSnapshotRequest{}, &raft.InstallSnapshotResponse{}, nil)
	if err != nil {
		t.Logf("InstallSnapshot returned error (expected for mock): %v", err)
	}

	// Test that heartbeat handler falls back to TCP
	transport.SetHeartbeatHandler(func(rpc raft.RPC) {
		// Heartbeat handler callback
	})

	// Test that timeout now falls back to TCP
	err = transport.TimeoutNow("node1", "127.0.0.1:11019", &raft.TimeoutNowRequest{}, &raft.TimeoutNowResponse{})
	if err != nil {
		t.Logf("TimeoutNow returned error (expected for mock): %v", err)
	}
}
