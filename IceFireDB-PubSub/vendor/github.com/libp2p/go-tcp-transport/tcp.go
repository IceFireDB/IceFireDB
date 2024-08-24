package tcp

import (
	"context"
	"errors"
	"net"
	"os"
	"runtime"
	"time"

	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/transport"
	tptu "github.com/libp2p/go-libp2p-transport-upgrader"
	rtpt "github.com/libp2p/go-reuseport-transport"

	ma "github.com/multiformats/go-multiaddr"
	mafmt "github.com/multiformats/go-multiaddr-fmt"
	manet "github.com/multiformats/go-multiaddr/net"
)

// DefaultConnectTimeout is the (default) maximum amount of time the TCP
// transport will spend on the initial TCP connect before giving up.
var DefaultConnectTimeout = 5 * time.Second

var log = logging.Logger("tcp-tpt")

const keepAlivePeriod = 30 * time.Second

type canKeepAlive interface {
	SetKeepAlive(bool) error
	SetKeepAlivePeriod(time.Duration) error
}

var _ canKeepAlive = &net.TCPConn{}

func tryKeepAlive(conn net.Conn, keepAlive bool) {
	keepAliveConn, ok := conn.(canKeepAlive)
	if !ok {
		log.Errorf("Can't set TCP keepalives.")
		return
	}
	if err := keepAliveConn.SetKeepAlive(keepAlive); err != nil {
		// Sometimes we seem to get "invalid argument" results from this function on Darwin.
		// This might be due to a closed connection, but I can't reproduce that on Linux.
		//
		// But there's nothing we can do about invalid arguments, so we'll drop this to a
		// debug.
		if errors.Is(err, os.ErrInvalid) {
			log.Debugw("failed to enable TCP keepalive", "error", err)
		} else {
			log.Errorw("failed to enable TCP keepalive", "error", err)
		}
		return
	}

	if runtime.GOOS != "openbsd" {
		if err := keepAliveConn.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
			log.Errorw("failed set keepalive period", "error", err)
		}
	}
}

// try to set linger on the connection, if possible.
func tryLinger(conn net.Conn, sec int) {
	type canLinger interface {
		SetLinger(int) error
	}

	if lingerConn, ok := conn.(canLinger); ok {
		_ = lingerConn.SetLinger(sec)
	}
}

type tcpListener struct {
	manet.Listener
	sec int
}

func (ll *tcpListener) Accept() (manet.Conn, error) {
	c, err := ll.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tryLinger(c, ll.sec)
	tryKeepAlive(c, true)
	return c, nil
}

// TcpTransport is the TCP transport.
type TcpTransport struct {
	// Connection upgrader for upgrading insecure stream connections to
	// secure multiplex connections.
	Upgrader *tptu.Upgrader

	// Explicitly disable reuseport.
	DisableReuseport bool

	// TCP connect timeout
	ConnectTimeout time.Duration

	reuse rtpt.Transport
}

var _ transport.Transport = &TcpTransport{}

// NewTCPTransport creates a tcp transport object that tracks dialers and listeners
// created. It represents an entire tcp stack (though it might not necessarily be)
func NewTCPTransport(upgrader *tptu.Upgrader) *TcpTransport {
	return &TcpTransport{Upgrader: upgrader, ConnectTimeout: DefaultConnectTimeout}
}

var dialMatcher = mafmt.And(mafmt.IP, mafmt.Base(ma.P_TCP))

// CanDial returns true if this transport believes it can dial the given
// multiaddr.
func (t *TcpTransport) CanDial(addr ma.Multiaddr) bool {
	return dialMatcher.Matches(addr)
}

func (t *TcpTransport) maDial(ctx context.Context, raddr ma.Multiaddr) (manet.Conn, error) {
	// Apply the deadline iff applicable
	if t.ConnectTimeout > 0 {
		deadline := time.Now().Add(t.ConnectTimeout)
		if d, ok := ctx.Deadline(); !ok || deadline.Before(d) {
			var cancel func()
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
	}

	if t.UseReuseport() {
		return t.reuse.DialContext(ctx, raddr)
	}
	var d manet.Dialer
	return d.DialContext(ctx, raddr)
}

// Dial dials the peer at the remote address.
func (t *TcpTransport) Dial(ctx context.Context, raddr ma.Multiaddr, p peer.ID) (transport.CapableConn, error) {
	conn, err := t.maDial(ctx, raddr)
	if err != nil {
		return nil, err
	}
	// Set linger to 0 so we never get stuck in the TIME-WAIT state. When
	// linger is 0, connections are _reset_ instead of closed with a FIN.
	// This means we can immediately reuse the 5-tuple and reconnect.
	tryLinger(conn, 0)
	tryKeepAlive(conn, true)
	c, err := newTracingConn(conn, true)
	if err != nil {
		return nil, err
	}
	return t.Upgrader.UpgradeOutbound(ctx, t, c, p)
}

// UseReuseport returns true if reuseport is enabled and available.
func (t *TcpTransport) UseReuseport() bool {
	return !t.DisableReuseport && ReuseportIsAvailable()
}

func (t *TcpTransport) maListen(laddr ma.Multiaddr) (manet.Listener, error) {
	if t.UseReuseport() {
		return t.reuse.Listen(laddr)
	}
	return manet.Listen(laddr)
}

// Listen listens on the given multiaddr.
func (t *TcpTransport) Listen(laddr ma.Multiaddr) (transport.Listener, error) {
	list, err := t.maListen(laddr)
	if err != nil {
		return nil, err
	}
	list = &tracingListener{&tcpListener{list, 0}}
	return t.Upgrader.UpgradeListener(t, list), nil
}

// Protocols returns the list of terminal protocols this transport can dial.
func (t *TcpTransport) Protocols() []int {
	return []int{ma.P_TCP}
}

// Proxy always returns false for the TCP transport.
func (t *TcpTransport) Proxy() bool {
	return false
}

func (t *TcpTransport) String() string {
	return "TCP"
}
