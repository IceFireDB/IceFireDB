package ssdp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/koron/go-ssdp/internal/multicast"
	"github.com/koron/go-ssdp/internal/ssdplog"
)

type message struct {
	to   net.Addr
	data multicast.DataProvider
}

// Advertiser is a server to advertise a service.
type Advertiser struct {
	st      string
	usn     string
	locProv LocationProvider
	server  string
	maxAge  int

	mu   sync.Mutex
	conn *multicast.Conn
	wg   sync.WaitGroup

	// addHost is an optional flag to add HOST header for M-SEARCH response.
	// It is to support SmartThings.
	// See https://github.com/koron/go-ssdp/issues/30 for details
	addHost bool
}

// Advertise starts advertisement of service.
// location should be a string or a ssdp.LocationProvider.
func Advertise(st, usn string, location any, server string, maxAge int, opts ...Option) (*Advertiser, error) {
	locProv, err := toLocationProvider(location)
	if err != nil {
		return nil, err
	}
	cfg, err := opts2config(opts)
	if err != nil {
		return nil, err
	}
	conn, err := multicast.Listen(multicast.RecvAddrResolver, cfg.multicastConfig.options()...)
	if err != nil {
		return nil, err
	}
	ssdplog.Printf("SSDP advertise on: %s", conn.LocalAddr().String())
	a := &Advertiser{
		st:      st,
		usn:     usn,
		locProv: locProv,
		server:  server,
		maxAge:  maxAge,
		conn:    conn,
		addHost: cfg.advertiseConfig.addHost,
	}
	a.wg.Add(1)
	go func() {
		a.recvMain()
		a.wg.Done()
	}()
	return a, nil
}

func (a *Advertiser) recvMain() error {
	// TODO: update listening interfaces of a.conn
	err := a.conn.ReadPackets(0, func(addr net.Addr, data []byte) error {
		if err := a.handleRaw(addr, data); err != nil {
			ssdplog.Printf("failed to handle message: %s", err)
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

func (a *Advertiser) handleRaw(from net.Addr, raw []byte) error {
	if !bytes.HasPrefix(raw, []byte("M-SEARCH ")) {
		// unexpected method.
		return nil
	}
	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(raw)))
	if err != nil {
		return err
	}
	var (
		man = req.Header.Get("MAN")
		st  = req.Header.Get("ST")
	)
	if man != `"ssdp:discover"` {
		return fmt.Errorf("unexpected MAN: %s", man)
	}
	if st != All && st != RootDevice && st != a.st {
		// skip when ST is not matched/expected.
		return nil
	}
	ssdplog.Printf("received M-SEARCH MAN=%s ST=%s from %s", man, st, from.String())
	// build and send a response.
	var host string
	if a.addHost {
		addr, err := multicast.SendAddr()
		if err != nil {
			return err
		}
		host = addr.String()
	}
	msg := buildOK(a.st, a.usn, a.locProv.Location(from, nil), a.server, a.maxAge, host)
	_, err = a.conn.WriteTo(multicast.BytesDataProvider(msg), from)
	return err
}

func buildOK(st, usn, location, server string, maxAge int, host string) []byte {
	// bytes.Buffer#Write() is never fail, so we can omit error checks.
	b := new(bytes.Buffer)
	b.WriteString("HTTP/1.1 200 OK\r\n")
	fmt.Fprintf(b, "EXT: \r\n")
	fmt.Fprintf(b, "ST: %s\r\n", st)
	fmt.Fprintf(b, "USN: %s\r\n", usn)
	if location != "" {
		fmt.Fprintf(b, "LOCATION: %s\r\n", location)
	}
	if server != "" {
		fmt.Fprintf(b, "SERVER: %s\r\n", server)
	}
	fmt.Fprintf(b, "CACHE-CONTROL: max-age=%d\r\n", maxAge)
	if host != "" {
		fmt.Fprintf(b, "HOST: %s\r\n", host)
	}
	b.WriteString("\r\n")
	return b.Bytes()
}

var ErrAdvertiserClosedAlready = errors.New("advertiser closed already")

func (a *Advertiser) connGuard(fn func() error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.conn != nil {
		return fn()
	}
	return ErrAdvertiserClosedAlready
}

// Close stops advertisement.
func (a *Advertiser) Close() error {
	return a.connGuard(func() error {
		a.conn.Close() // 1. Interrupt ReadPackets in recvMain
		a.wg.Wait()    // 2. Wait for termination of recvMain
		a.conn = nil
		return nil
	})
}

// Alive announces ssdp:alive message.
func (a *Advertiser) Alive() error {
	return a.connGuard(func() error {
		addr, err := multicast.SendAddr()
		if err != nil {
			return err
		}
		msg := &aliveDataProvider{
			host:     addr,
			nt:       a.st,
			usn:      a.usn,
			location: a.locProv,
			server:   a.server,
			maxAge:   a.maxAge,
		}
		_, err = a.conn.WriteTo(msg, addr)
		ssdplog.Printf("sent alive")
		return err
	})
}

// Bye announces ssdp:byebye message.
func (a *Advertiser) Bye() error {
	return a.connGuard(func() error {
		addr, err := multicast.SendAddr()
		if err != nil {
			return err
		}
		msg, err := buildBye(addr, a.st, a.usn)
		if err != nil {
			return err
		}
		_, err = a.conn.WriteTo(multicast.BytesDataProvider(msg), addr)
		ssdplog.Printf("sent bye")
		return err
	})
}
