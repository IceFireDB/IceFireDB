package transport

import (
	"io"
	"time"

	"github.com/hashicorp/raft"
)

// mockTransport implements raft.Transport for testing
// This is a simplified mock that provides basic functionality for testing
type mockTransport struct {
	localAddr raft.ServerAddress
}

// Consumer returns a channel that can be used to consume and respond to RPC requests
func (m *mockTransport) Consumer() <-chan raft.RPC {
	// Return an empty channel for testing
	return make(chan raft.RPC)
}

// LocalAddr is used to return our local address to distinguish from our peers
func (m *mockTransport) LocalAddr() raft.ServerAddress {
	return m.localAddr
}

// AppendEntriesPipeline returns an interface that can be used to pipeline AppendEntries requests
func (m *mockTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, nil
}

// AppendEntries sends the appropriate RPC to the target node
func (m *mockTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return nil
}

// RequestVote sends the appropriate RPC to the target node
func (m *mockTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return nil
}

// InstallSnapshot is used to push a snapshot down to a follower
func (m *mockTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return nil
}

// EncodePeer is used to serialize a peer's address
func (m *mockTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

// DecodePeer is used to deserialize a peer's address
func (m *mockTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return raft.ServerAddress(buf)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
func (m *mockTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	// No-op for mock
}

// TimeoutNow is used to start a leadership transfer to the target node
func (m *mockTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return nil
}

// Test utilities for network testing

// createMockTCPTransport creates a mock TCP transport for testing
func createMockTCPTransport(addr string) raft.Transport {
	return &mockTransport{
		localAddr: raft.ServerAddress(addr),
	}
}

// TestConfig holds configuration for transport tests
type TestConfig struct {
	NetworkMode     string
	TCPAddr         string
	WebRTCAddr      string
	Timeout         time.Duration
	ConcurrentTests int
}

// DefaultTestConfig returns a default test configuration
func DefaultTestConfig() TestConfig {
	return TestConfig{
		NetworkMode:     "tcp",
		TCPAddr:         "127.0.0.1:11001",
		WebRTCAddr:      "127.0.0.1:11002",
		Timeout:         5 * time.Second,
		ConcurrentTests: 10,
	}
}

// TCPTestConfig returns a test configuration for TCP mode
func TCPTestConfig() TestConfig {
	config := DefaultTestConfig()
	config.NetworkMode = "tcp"
	return config
}

// WebRTCTestConfig returns a test configuration for WebRTC mode
func WebRTCTestConfig() TestConfig {
	config := DefaultTestConfig()
	config.NetworkMode = "webrtc"
	return config
}

// TestTransportPair represents a pair of transports for testing communication
type TestTransportPair struct {
	Transport1 *CustomTransport
	Transport2 *CustomTransport
	Config     TestConfig
}

// CreateTestTransportPair creates a pair of transports for testing
func CreateTestTransportPair(config TestConfig) (*TestTransportPair, error) {
	tcpTransport1 := createMockTCPTransport(config.TCPAddr)
	tcpTransport2 := createMockTCPTransport(config.WebRTCAddr)

	transport1, err := NewCustomTransport(config.NetworkMode, tcpTransport1, config.WebRTCAddr)
	if err != nil {
		return nil, err
	}

	transport2, err := NewCustomTransport(config.NetworkMode, tcpTransport2, config.TCPAddr)
	if err != nil {
		return nil, err
	}

	return &TestTransportPair{
		Transport1: transport1,
		Transport2: transport2,
		Config:     config,
	}, nil
}

// VerifyTransportInterface verifies that a transport implements all required raft.Transport methods
func VerifyTransportInterface(transport raft.Transport) bool {
	// Test that all required methods exist by calling them
	// This will cause compilation errors if any methods are missing

	_ = transport.Consumer()
	_ = transport.LocalAddr()
	_, _ = transport.AppendEntriesPipeline("test", "127.0.0.1:11001")
	_ = transport.AppendEntries("test", "127.0.0.1:11001", &raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})
	_ = transport.RequestVote("test", "127.0.0.1:11001", &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{})
	_ = transport.InstallSnapshot("test", "127.0.0.1:11001", &raft.InstallSnapshotRequest{}, &raft.InstallSnapshotResponse{}, nil)
	_ = transport.EncodePeer("test", "127.0.0.1:11001")
	_ = transport.DecodePeer([]byte("127.0.0.1:11001"))
	transport.SetHeartbeatHandler(func(rpc raft.RPC) {})
	_ = transport.TimeoutNow("test", "127.0.0.1:11001", &raft.TimeoutNowRequest{}, &raft.TimeoutNowResponse{})

	return true
}
