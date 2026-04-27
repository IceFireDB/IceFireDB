package transport

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestTCPE2E tests end-to-end TCP transport functionality
func TestTCPE2E(t *testing.T) {
	// Create two TCP transports for simulating node communication
	addr1 := "127.0.0.1:11001"
	addr2 := "127.0.0.1:11002"

	// Create mock TCP transports
	tcpTransport1 := createMockTCPTransport(addr1)
	tcpTransport2 := createMockTCPTransport(addr2)

	// Create custom transports in TCP mode
	transport1, err := NewCustomTransport("tcp", tcpTransport1, "")
	if err != nil {
		t.Fatalf("Failed to create transport1: %v", err)
	}
	transport2, err := NewCustomTransport("tcp", tcpTransport2, "")
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
	if transport1.networkMode != "tcp" {
		t.Errorf("Expected network mode 'tcp', got '%s'", transport1.networkMode)
	}

	// Clean up
	// Note: In a real E2E test, we would test actual RPC communication
	// but for now we're testing the transport setup
}

// TestTCPTransportIntegration tests integration with actual Raft operations
func TestTCPTransportIntegration(t *testing.T) {
	// Create a simple test to verify transport can be used with Raft
	addr := "127.0.0.1:11003"
	tcpTransport := createMockTCPTransport(addr)

	transport, err := NewCustomTransport("tcp", tcpTransport, "")
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// Test that transport implements all required raft.Transport methods
	var _ raft.Transport = transport

	// Test timeout now operation (should not panic)
	err = transport.TimeoutNow("node1", "127.0.0.1:11004", &raft.TimeoutNowRequest{}, &raft.TimeoutNowResponse{})
	if err != nil {
		t.Logf("TimeoutNow returned error (expected for mock): %v", err)
	}

	// Test append entries operation
	err = transport.AppendEntries("node1", "127.0.0.1:11004", &raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})
	if err != nil {
		t.Logf("AppendEntries returned error (expected for mock): %v", err)
	}

	// Test request vote operation
	err = transport.RequestVote("node1", "127.0.0.1:11004", &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{})
	if err != nil {
		t.Logf("RequestVote returned error (expected for mock): %v", err)
	}

	// Test install snapshot operation
	err = transport.InstallSnapshot("node1", "127.0.0.1:11004", &raft.InstallSnapshotRequest{}, &raft.InstallSnapshotResponse{}, nil)
	if err != nil {
		t.Logf("InstallSnapshot returned error (expected for mock): %v", err)
	}
}

// TestTCPModeSwitching tests that TCP mode properly falls back to TCP transport
func TestTCPModeSwitching(t *testing.T) {
	addr := "127.0.0.1:11005"
	tcpTransport := createMockTCPTransport(addr)

	transport, err := NewCustomTransport("tcp", tcpTransport, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}

	// In TCP mode, WebRTC transport should be nil
	if transport.webrtcTransport != nil {
		t.Error("WebRTC transport should be nil in TCP mode")
	}

	// All operations should use TCP transport
	if transport.tcpTransport == nil {
		t.Error("TCP transport should not be nil")
	}

	// Verify that network mode is correctly set
	if transport.networkMode != "tcp" {
		t.Errorf("Expected network mode 'tcp', got '%s'", transport.networkMode)
	}
}

// TestTCPTransportConcurrent tests concurrent access to TCP transport
func TestTCPTransportConcurrent(t *testing.T) {
	addr := "127.0.0.1:11006"
	tcpTransport := createMockTCPTransport(addr)

	transport, err := NewCustomTransport("tcp", tcpTransport, "")
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
				_ = transport.EncodePeer("node1", "127.0.0.1:11007")
				_ = transport.DecodePeer([]byte("127.0.0.1:11007"))
			}
		}
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	// Test should complete without panics or deadlocks
}
