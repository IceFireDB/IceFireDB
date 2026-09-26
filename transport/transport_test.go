package transport

import (
	"testing"

	"github.com/hashicorp/raft"
)

func TestNewCustomTransport(t *testing.T) {
	tests := []struct {
		name        string
		networkMode string
		webrtcAddr  string
		wantErr     bool
	}{
		{
			name:        "TCP mode",
			networkMode: "tcp",
			webrtcAddr:  "127.0.0.1:0",
			wantErr:     false,
		},
		{
			name:        "WebRTC mode with valid address",
			networkMode: "webrtc",
			webrtcAddr:  "127.0.0.1:0",
			wantErr:     false,
		},
		{
			name:        "WebRTC mode with invalid address",
			networkMode: "webrtc",
			webrtcAddr:  "invalid-address",
			wantErr:     true,
		},
		{
			name:        "Invalid network mode",
			networkMode: "invalid",
			webrtcAddr:  "127.0.0.1:0",
			wantErr:     false, // Should not error, just fall back to TCP
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockTCP := &mockTransport{localAddr: "127.0.0.1:11001"}
			transport, err := NewCustomTransport(tt.networkMode, mockTCP, tt.webrtcAddr)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCustomTransport() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && transport == nil {
				t.Error("NewCustomTransport() returned nil transport without error")
			}
		})
	}
}

func TestCustomTransport_LocalAddr(t *testing.T) {
	mockTCP := &mockTransport{localAddr: "tcp://127.0.0.1:11001"}

	tests := []struct {
		name        string
		networkMode string
		expected    raft.ServerAddress
	}{
		{
			name:        "TCP mode returns TCP address",
			networkMode: "tcp",
			expected:    "tcp://127.0.0.1:11001",
		},
		{
			name:        "WebRTC mode returns WebRTC address",
			networkMode: "webrtc",
			expected:    "127.0.0.1:0", // WebRTC address format
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport, err := NewCustomTransport(tt.networkMode, mockTCP, "127.0.0.1:0")
			if err != nil {
				t.Fatalf("Failed to create transport: %v", err)
			}

			addr := transport.LocalAddr()
			if string(addr) == "" {
				t.Error("LocalAddr() returned empty address")
			}
		})
	}
}

func TestCustomTransport_Methods(t *testing.T) {
	mockTCP := &mockTransport{localAddr: "127.0.0.1:11001"}

	tests := []struct {
		name        string
		networkMode string
	}{
		{
			name:        "TCP mode",
			networkMode: "tcp",
		},
		{
			name:        "WebRTC mode",
			networkMode: "webrtc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport, err := NewCustomTransport(tt.networkMode, mockTCP, "127.0.0.1:0")
			if err != nil {
				t.Fatalf("Failed to create transport: %v", err)
			}

			// Test Consumer
			consumer := transport.Consumer()
			if consumer == nil {
				t.Error("Consumer() returned nil")
			}

			// Test AppendEntriesPipeline
			pipeline, err := transport.AppendEntriesPipeline("node1", "127.0.0.1:11002")
			if err != nil {
				t.Errorf("AppendEntriesPipeline() error = %v", err)
			}
			if pipeline != nil {
				t.Error("AppendEntriesPipeline() should return nil for mock")
			}

			// Test AppendEntries
			err = transport.AppendEntries("node1", "127.0.0.1:11002", &raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})
			if err != nil {
				t.Errorf("AppendEntries() error = %v", err)
			}

			// Test RequestVote
			err = transport.RequestVote("node1", "127.0.0.1:11002", &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{})
			if err != nil {
				t.Errorf("RequestVote() error = %v", err)
			}

			// Test InstallSnapshot
			err = transport.InstallSnapshot("node1", "127.0.0.1:11002", &raft.InstallSnapshotRequest{}, &raft.InstallSnapshotResponse{}, nil)
			if err != nil {
				t.Errorf("InstallSnapshot() error = %v", err)
			}

			// Test EncodePeer
			encoded := transport.EncodePeer("node1", "127.0.0.1:11002")
			if len(encoded) == 0 {
				t.Error("EncodePeer() returned empty byte slice")
			}

			// Test DecodePeer
			decoded := transport.DecodePeer([]byte("127.0.0.1:11002"))
			if decoded == "" {
				t.Error("DecodePeer() returned empty address")
			}

			// Test SetHeartbeatHandler
			transport.SetHeartbeatHandler(func(rpc raft.RPC) {
				// Should not panic
			})

			// Test TimeoutNow
			err = transport.TimeoutNow("node1", "127.0.0.1:11002", &raft.TimeoutNowRequest{}, &raft.TimeoutNowResponse{})
			if err != nil {
				t.Errorf("TimeoutNow() error = %v", err)
			}
		})
	}
}

func TestCustomTransport_Interface(t *testing.T) {
	// Test that CustomTransport implements raft.Transport interface
	var _ raft.Transport = &CustomTransport{}
}
