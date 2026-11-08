package transport

import (
	"fmt"
	"io"

	"github.com/IceFireDB/IceFireDB/transport/webrtc"
	"github.com/hashicorp/raft"
)

// CustomTransport implements raft.Transport and can switch between TCP and WebRTC
type CustomTransport struct {
	networkMode     string
	tcpTransport    raft.Transport
	webrtcTransport *webrtc.WebRTCTransport
}

// NewCustomTransport creates a new custom transport that can use TCP or WebRTC
func NewCustomTransport(networkMode string, tcpTransport raft.Transport, webrtcAddr string) (*CustomTransport, error) {
	var webrtcTransport *webrtc.WebRTCTransport
	var err error

	if networkMode == "webrtc" {
		webrtcTransport, err = webrtc.NewWebRTCTransport(webrtcAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to create WebRTC transport: %w", err)
		}
	}

	return &CustomTransport{
		networkMode:     networkMode,
		tcpTransport:    tcpTransport,
		webrtcTransport: webrtcTransport,
	}, nil
}

// Consumer returns a channel that can be used to consume and respond to RPC requests
func (t *CustomTransport) Consumer() <-chan raft.RPC {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		// WebRTC transport doesn't implement RPC consumer yet
		// For now, fall back to TCP
		return t.tcpTransport.Consumer()
	}
	return t.tcpTransport.Consumer()
}

// LocalAddr is used to return our local address to distinguish from our peers
func (t *CustomTransport) LocalAddr() raft.ServerAddress {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		return raft.ServerAddress(t.webrtcTransport.Addr().String())
	}
	return t.tcpTransport.LocalAddr()
}

// AppendEntriesPipeline returns an interface that can be used to pipeline AppendEntries requests
func (t *CustomTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		// WebRTC transport doesn't implement pipeline yet
		// For now, fall back to TCP
		return t.tcpTransport.AppendEntriesPipeline(id, target)
	}
	return t.tcpTransport.AppendEntriesPipeline(id, target)
}

// AppendEntries sends the appropriate RPC to the target node
func (t *CustomTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		// WebRTC transport doesn't implement RPC yet
		// For now, fall back to TCP
		return t.tcpTransport.AppendEntries(id, target, args, resp)
	}
	return t.tcpTransport.AppendEntries(id, target, args, resp)
}

// RequestVote sends the appropriate RPC to the target node
func (t *CustomTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		// WebRTC transport doesn't implement RPC yet
		// For now, fall back to TCP
		return t.tcpTransport.RequestVote(id, target, args, resp)
	}
	return t.tcpTransport.RequestVote(id, target, args, resp)
}

// InstallSnapshot is used to push a snapshot down to a follower
func (t *CustomTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		// WebRTC transport doesn't implement snapshot yet
		// For now, fall back to TCP
		return t.tcpTransport.InstallSnapshot(id, target, args, resp, data)
	}
	return t.tcpTransport.InstallSnapshot(id, target, args, resp, data)
}

// EncodePeer is used to serialize a peer's address
func (t *CustomTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		// For WebRTC, we might need different encoding
		// For now, use TCP encoding
		return t.tcpTransport.EncodePeer(id, addr)
	}
	return t.tcpTransport.EncodePeer(id, addr)
}

// DecodePeer is used to deserialize a peer's address
func (t *CustomTransport) DecodePeer(buf []byte) raft.ServerAddress {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		// For WebRTC, we might need different decoding
		// For now, use TCP decoding
		return t.tcpTransport.DecodePeer(buf)
	}
	return t.tcpTransport.DecodePeer(buf)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
func (t *CustomTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		// WebRTC transport doesn't implement heartbeat handler yet
		// For now, fall back to TCP
		t.tcpTransport.SetHeartbeatHandler(cb)
		return
	}
	t.tcpTransport.SetHeartbeatHandler(cb)
}

// TimeoutNow is used to start a leadership transfer to the target node
func (t *CustomTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	if t.networkMode == "webrtc" && t.webrtcTransport != nil {
		// WebRTC transport doesn't implement timeout yet
		// For now, fall back to TCP
		return t.tcpTransport.TimeoutNow(id, target, args, resp)
	}
	return t.tcpTransport.TimeoutNow(id, target, args, resp)
}
