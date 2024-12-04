package libp2pwebtransport

import (
	"context"

	"github.com/libp2p/go-libp2p/core/network"
	tpt "github.com/libp2p/go-libp2p/core/transport"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
)

type connSecurityMultiaddrs struct {
	network.ConnSecurity
	network.ConnMultiaddrs
}

type connMultiaddrs struct {
	local, remote ma.Multiaddr
}

var _ network.ConnMultiaddrs = &connMultiaddrs{}

func (c *connMultiaddrs) LocalMultiaddr() ma.Multiaddr  { return c.local }
func (c *connMultiaddrs) RemoteMultiaddr() ma.Multiaddr { return c.remote }

type conn struct {
	*connSecurityMultiaddrs

	transport *transport
	session   *webtransport.Session

	scope network.ConnManagementScope
	qconn quic.Connection
}

var _ tpt.CapableConn = &conn{}

func newConn(tr *transport, sess *webtransport.Session, sconn *connSecurityMultiaddrs, scope network.ConnManagementScope, qconn quic.Connection) *conn {
	return &conn{
		connSecurityMultiaddrs: sconn,
		transport:              tr,
		session:                sess,
		scope:                  scope,
		qconn:                  qconn,
	}
}

func (c *conn) OpenStream(ctx context.Context) (network.MuxedStream, error) {
	str, err := c.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &stream{str}, nil
}

func (c *conn) AcceptStream() (network.MuxedStream, error) {
	str, err := c.session.AcceptStream(context.Background())
	if err != nil {
		return nil, err
	}
	return &stream{str}, nil
}

func (c *conn) allowWindowIncrease(size uint64) bool {
	return c.scope.ReserveMemory(int(size), network.ReservationPriorityMedium) == nil
}

// Close closes the connection.
// It must be called even if the peer closed the connection in order for
// garbage collection to properly work in this package.
func (c *conn) Close() error {
	defer c.scope.Done()
	c.transport.removeConn(c.session)
	err := c.session.CloseWithError(0, "")
	_ = c.qconn.CloseWithError(1, "")
	return err
}

func (c *conn) IsClosed() bool           { return c.session.Context().Err() != nil }
func (c *conn) Scope() network.ConnScope { return c.scope }
func (c *conn) Transport() tpt.Transport { return c.transport }

func (c *conn) ConnState() network.ConnectionState {
	return network.ConnectionState{Transport: "webtransport"}
}
