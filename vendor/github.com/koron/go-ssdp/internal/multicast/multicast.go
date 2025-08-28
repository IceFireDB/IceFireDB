package multicast

import (
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/koron/go-ssdp/internal/ssdplog"
	"golang.org/x/net/ipv4"
)

// Conn is multicast connection.
type Conn struct {
	laddr *net.UDPAddr
	pconn *ipv4.PacketConn

	// ifps stores pointers of multicast interface.
	ifps []*net.Interface
}

type connConfig struct {
	ttl   int
	sysIf bool
}

// Listen starts to receiving multicast messages.
func Listen(r *AddrResolver, opts ...ConnOption) (*Conn, error) {
	// prepare parameters.
	laddr, err := r.resolve()
	if err != nil {
		return nil, err
	}
	// configure connection
	var cfg connConfig
	for _, o := range opts {
		o.apply(&cfg)
	}
	// connect.
	conn, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		return nil, err
	}
	// configure socket to use with multicast.
	pconn, ifplist, err := newIPv4MulticastConn(conn, cfg.sysIf)
	if err != nil {
		conn.Close()
		return nil, err
	}
	// set TTL
	if cfg.ttl > 0 {
		err := pconn.SetTTL(cfg.ttl)
		if err != nil {
			pconn.Close()
			return nil, err
		}
	}
	return &Conn{
		laddr: laddr,
		pconn: pconn,
		ifps:  ifplist,
	}, nil
}

// newIPv4MulticastConn create a new multicast connection.
// 2nd return parameter will be nil when sysIf is true.
func newIPv4MulticastConn(conn *net.UDPConn, sysIf bool) (*ipv4.PacketConn, []*net.Interface, error) {
	// sysIf: use system assigned multicast interface.
	// the empty iflist indicate it.
	var ifplist []*net.Interface
	if !sysIf {
		list, err := interfaces()
		if err != nil {
			return nil, nil, err
		}
		ifplist = make([]*net.Interface, 0, len(list))
		for i := range list {
			ifplist = append(ifplist, &list[i])
		}
	}
	addr, err := SendAddr()
	if err != nil {
		return nil, nil, err
	}
	pconn, err := joinGroupIPv4(conn, ifplist, addr)
	if err != nil {
		return nil, nil, err
	}
	return pconn, ifplist, nil
}

// joinGroupIPv4 makes the connection join to a group on interfaces.
// This trys to use system assigned when iflist is nil or empty.
func joinGroupIPv4(conn *net.UDPConn, ifplist []*net.Interface, gaddr net.Addr) (*ipv4.PacketConn, error) {
	wrap := ipv4.NewPacketConn(conn)
	wrap.SetMulticastLoopback(true)

	// try to use the system assigned multicast interface when iflist is empty.
	if len(ifplist) == 0 {
		if err := wrap.JoinGroup(nil, gaddr); err != nil {
			ssdplog.Printf("failed to join group %s on system assigned multicast interface: %s", gaddr.String(), err)
			return nil, errors.New("no system assigned multicast interfaces had joined to group")
		}
		ssdplog.Printf("joined group %s on system assigned multicast interface", gaddr.String())
		return wrap, nil
	}

	// add interfaces to multicast group.
	joined := 0
	for _, ifi := range ifplist {
		if err := wrap.JoinGroup(ifi, gaddr); err != nil {
			ssdplog.Printf("failed to join group %s on %s: %s", gaddr.String(), ifi.Name, err)
			continue
		}
		joined++
		ssdplog.Printf("joined group %s on %s (#%d)", gaddr.String(), ifi.Name, ifi.Index)
	}
	if joined == 0 {
		return nil, errors.New("no interfaces had joined to group")
	}
	return wrap, nil
}

// Close closes a multicast connection.
func (mc *Conn) Close() error {
	// based net.UDPConn will be closed by mc.pconn.Close()
	return mc.pconn.Close()
}

// DataProvider provides a body of multicast message to send.
type DataProvider interface {
	Bytes(*net.Interface) []byte
}

type BytesDataProvider []byte

func (b BytesDataProvider) Bytes(ifi *net.Interface) []byte {
	return []byte(b)
}

// WriteTo sends a multicast message to interfaces.
func (mc *Conn) WriteTo(dataProv DataProvider, to net.Addr) (int, error) {
	// Send a multicast message directory when recipient "to" address is not multicast.
	if uaddr, ok := to.(*net.UDPAddr); ok && !uaddr.IP.IsMulticast() {
		return mc.writeToIfi(dataProv, to, nil)
	}
	// Send a multicast message to all interfaces (iflist).
	sum := 0
	for _, ifi := range mc.ifps {
		ssdplog.Printf("WriteTo: ifi=%+v", ifi)
		n, err := mc.writeToIfi(dataProv, to, ifi)
		if err != nil {
			return 0, err
		}
		sum += n
	}
	return sum, nil
}

func (mc *Conn) writeToIfi(dataProv DataProvider, to net.Addr, ifi *net.Interface) (int, error) {
	if err := mc.pconn.SetMulticastInterface(ifi); err != nil {
		return 0, err
	}
	return mc.pconn.WriteTo(dataProv.Bytes(ifi), nil, to)
}

// LocalAddr returns local address to listen multicast packets.
func (mc *Conn) LocalAddr() net.Addr {
	return mc.laddr
}

// ReadPackets reads multicast packets.
func (mc *Conn) ReadPackets(timeout time.Duration, h PacketHandler) error {
	buf := make([]byte, 65535)
	if timeout > 0 {
		mc.pconn.SetReadDeadline(time.Now().Add(timeout))
	}
	for {
		n, _, addr, err := mc.pconn.ReadFrom(buf)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				return nil
			}
			if strings.Contains(err.Error(), "use of closed network connection") {
				return io.EOF
			}
			return err
		}
		if err := h(addr, buf[:n]); err != nil {
			return err
		}
	}
}

// ConnOption is option for Listen()
type ConnOption interface {
	apply(cfg *connConfig)
}

type connOptFunc func(*connConfig)

func (f connOptFunc) apply(cfg *connConfig) {
	f(cfg)
}

// ConnTTL returns as ConnOption that set default TTL to the connection.
func ConnTTL(ttl int) ConnOption {
	return connOptFunc(func(cfg *connConfig) {
		cfg.ttl = ttl
	})
}

func ConnSystemAssginedInterface() ConnOption {
	return connOptFunc(func(cfg *connConfig) {
		cfg.sysIf = true
	})
}
