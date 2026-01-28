package webtransport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"

	"github.com/dunglas/httpsfv"
)

const (
	wtAvailableProtocolsHeader = "WT-Available-Protocols"
	wtProtocolHeader           = "WT-Protocol"
)

const (
	webTransportFrameType     = 0x41
	webTransportUniStreamType = 0x54
)

type quicConnKeyType struct{}

var quicConnKey = quicConnKeyType{}

func ConfigureHTTP3Server(s *http3.Server) {
	if s.AdditionalSettings == nil {
		s.AdditionalSettings = make(map[uint64]uint64, 1)
	}
	s.AdditionalSettings[settingsEnableWebtransport] = 1
	s.EnableDatagrams = true
	origConnContext := s.ConnContext
	s.ConnContext = func(ctx context.Context, conn *quic.Conn) context.Context {
		if origConnContext != nil {
			ctx = origConnContext(ctx, conn)
		}
		ctx = context.WithValue(ctx, quicConnKey, conn)
		return ctx
	}
}

type Server struct {
	H3 *http3.Server

	// ApplicationProtocols is a list of application protocols that can be negotiated,
	// see section 3.3 of https://www.ietf.org/archive/id/draft-ietf-webtrans-http3-14 for details.
	ApplicationProtocols []string

	// ReorderingTimeout is the maximum time an incoming WebTransport stream that cannot be associated
	// with a session is buffered. It is also the maximum time a WebTransport connection request is
	// blocked waiting for the client's SETTINGS are received.
	// This can happen if the CONNECT request (that creates a new session) is reordered, and arrives
	// after the first WebTransport stream(s) for that session.
	// Defaults to 5 seconds.
	ReorderingTimeout time.Duration

	// CheckOrigin is used to validate the request origin, thereby preventing cross-site request forgery.
	// CheckOrigin returns true if the request Origin header is acceptable.
	// If unset, a safe default is used: If the Origin header is set, it is checked that it
	// matches the request's Host header.
	CheckOrigin func(r *http.Request) bool

	ctx       context.Context // is closed when Close is called
	ctxCancel context.CancelFunc
	refCount  sync.WaitGroup

	initOnce sync.Once
	initErr  error

	connsMx sync.Mutex
	conns   map[*quic.Conn]*sessionManager
}

func (s *Server) initialize() error {
	s.initOnce.Do(func() {
		s.initErr = s.init()
	})
	return s.initErr
}

func (s *Server) timeout() time.Duration {
	timeout := s.ReorderingTimeout
	if timeout == 0 {
		return 5 * time.Second
	}
	return timeout
}

func (s *Server) init() error {
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	s.conns = make(map[*quic.Conn]*sessionManager)
	if s.CheckOrigin == nil {
		s.CheckOrigin = checkSameOrigin
	}
	return nil
}

func (s *Server) Serve(conn net.PacketConn) error {
	if err := s.initialize(); err != nil {
		return err
	}
	var quicConf *quic.Config
	if s.H3.QUICConfig != nil {
		quicConf = s.H3.QUICConfig.Clone()
	} else {
		quicConf = &quic.Config{}
	}
	quicConf.EnableDatagrams = true
	quicConf.EnableStreamResetPartialDelivery = true
	ln, err := quic.ListenEarly(conn, s.H3.TLSConfig, quicConf)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		qconn, err := ln.Accept(s.ctx)
		if err != nil {
			return err
		}
		s.refCount.Add(1)
		go func() {
			defer s.refCount.Done()

			if err := s.ServeQUICConn(qconn); err != nil {
				log.Printf("http3: error serving QUIC connection: %v", err)
			}
		}()
	}
}

// ServeQUICConn serves a single QUIC connection.
func (s *Server) ServeQUICConn(conn *quic.Conn) error {
	connState := conn.ConnectionState()
	if !connState.SupportsDatagrams.Local {
		return errors.New("webtransport: QUIC DATAGRAM support required, enable it via QUICConfig.EnableDatagrams")
	}
	if !connState.SupportsStreamResetPartialDelivery.Local {
		return errors.New("webtransport: QUIC Stream Resets with Partial Delivery required, enable it via QUICConfig.EnableStreamResetPartialDelivery")
	}
	if err := s.initialize(); err != nil {
		return err
	}

	s.connsMx.Lock()
	sessMgr, ok := s.conns[conn]
	if !ok {
		sessMgr = newSessionManager(s.timeout())
		s.conns[conn] = sessMgr
	}
	s.connsMx.Unlock()

	// Clean up when connection closes
	context.AfterFunc(conn.Context(), func() {
		s.connsMx.Lock()
		delete(s.conns, conn)
		s.connsMx.Unlock()
		sessMgr.Close()
	})

	http3Conn, err := s.H3.NewRawServerConn(conn)
	if err != nil {
		return err
	}

	// slose the connection when the server context is cancelled.
	go func() {
		select {
		case <-s.ctx.Done():
			conn.CloseWithError(0, "")
		case <-conn.Context().Done():
			// connection already closed
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()

		for {
			str, err := conn.AcceptStream(s.ctx)
			if err != nil {
				return
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				typ, err := quicvarint.Peek(str)
				if err != nil {
					return
				}
				if typ != webTransportFrameType {
					http3Conn.HandleRequestStream(str)
					return
				}
				// read the frame type (already peeked)
				if _, err := quicvarint.Read(quicvarint.NewReader(str)); err != nil {
					return
				}
				// read the session ID
				id, err := quicvarint.Read(quicvarint.NewReader(str))
				if err != nil {
					str.CancelRead(quic.StreamErrorCode(http3.ErrCodeGeneralProtocolError))
					str.CancelWrite(quic.StreamErrorCode(http3.ErrCodeGeneralProtocolError))
					return
				}
				sessMgr.AddStream(str, sessionID(id))
			}()
		}
	}()

	go func() {
		defer wg.Done()

		for {
			str, err := conn.AcceptUniStream(s.ctx)
			if err != nil {
				return
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				typ, err := quicvarint.Peek(str)
				if err != nil {
					return
				}
				if typ != webTransportUniStreamType {
					http3Conn.HandleUnidirectionalStream(str)
					return
				}
				// read the stream type (already peeked) before passing to AddUniStream
				r := quicvarint.NewReader(str)
				if _, err := quicvarint.Read(r); err != nil {
					return
				}
				// read the session ID
				id, err := quicvarint.Read(r)
				if err != nil {
					str.CancelRead(quic.StreamErrorCode(http3.ErrCodeGeneralProtocolError))
					return
				}
				sessMgr.AddUniStream(str, sessionID(id))
			}()
		}
	}()

	wg.Wait()
	return nil
}

func (s *Server) ListenAndServe() error {
	addr := s.H3.Addr
	if addr == "" {
		addr = ":https"
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	return s.Serve(conn)
}

func (s *Server) ListenAndServeTLS(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	if s.H3.TLSConfig == nil {
		s.H3.TLSConfig = &tls.Config{}
	}
	s.H3.TLSConfig.Certificates = []tls.Certificate{cert}
	return s.ListenAndServe()
}

func (s *Server) Close() error {
	// Make sure that ctxCancel is defined.
	// This is expected to be uncommon.
	// It only happens if the server is closed without Serve / ListenAndServe having been called.
	s.initOnce.Do(func() {})

	if s.ctxCancel != nil {
		s.ctxCancel()
	}
	s.connsMx.Lock()
	if s.conns != nil {
		for _, mgr := range s.conns {
			mgr.Close()
		}
		s.conns = nil
	}
	s.connsMx.Unlock()

	err := s.H3.Close()
	s.refCount.Wait()
	return err
}

func (s *Server) Upgrade(w http.ResponseWriter, r *http.Request) (*Session, error) {
	if err := s.initialize(); err != nil {
		return nil, err
	}
	if r.Method != http.MethodConnect {
		return nil, fmt.Errorf("expected CONNECT request, got %s", r.Method)
	}
	if r.Proto != protocolHeader {
		return nil, fmt.Errorf("unexpected protocol: %s", r.Proto)
	}
	if !s.CheckOrigin(r) {
		return nil, errors.New("webtransport: request origin not allowed")
	}

	id := r.Context().Value(quicConnKey)
	if id == nil {
		return nil, errors.New("webtransport: missing QUIC connection")
	}
	conn := id.(*quic.Conn)

	selectedProtocol := s.selectProtocol(r.Header[http.CanonicalHeaderKey(wtAvailableProtocolsHeader)])

	// Wait for SETTINGS
	settingser := w.(http3.Settingser)
	timer := time.NewTimer(s.timeout())
	defer timer.Stop()
	select {
	case <-settingser.ReceivedSettings():
	case <-timer.C:
		return nil, errors.New("webtransport: didn't receive the client's SETTINGS on time")
	}
	settings := settingser.Settings()
	if !settings.EnableDatagrams {
		return nil, errors.New("webtransport: missing datagram support")
	}

	if selectedProtocol != "" {
		v, err := httpsfv.Marshal(httpsfv.NewItem(selectedProtocol))
		if err != nil {
			return nil, fmt.Errorf("failed to marshal selected protocol: %w", err)
		}
		w.Header().Add(wtProtocolHeader, v)
	}
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	str := w.(http3.HTTPStreamer).HTTPStream()
	sessID := sessionID(str.StreamID())

	// The session manager should already exist because ServeQUICConn creates it
	// before any HTTP requests can be processed on this connection.
	s.connsMx.Lock()
	defer s.connsMx.Unlock()

	sessMgr, ok := s.conns[conn]
	if !ok {
		return nil, errors.New("webtransport: connection session manager not found")
	}

	sess := newSession(context.WithoutCancel(r.Context()), sessID, conn, str, selectedProtocol)
	sessMgr.AddSession(sessID, sess)
	return sess, nil
}

func (s *Server) selectProtocol(theirs []string) string {
	list, err := httpsfv.UnmarshalList(theirs)
	if err != nil {
		return ""
	}
	offered := make([]string, 0, len(list))
	for _, item := range list {
		i, ok := item.(httpsfv.Item)
		if !ok {
			return ""
		}
		protocol, ok := i.Value.(string)
		if !ok {
			return ""
		}
		offered = append(offered, protocol)
	}
	var selectedProtocol string
	for _, p := range offered {
		if slices.Contains(s.ApplicationProtocols, p) {
			selectedProtocol = p
			break
		}
	}
	return selectedProtocol
}

// copied from https://github.com/gorilla/websocket
func checkSameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return equalASCIIFold(u.Host, r.Host)
}

// copied from https://github.com/gorilla/websocket
func equalASCIIFold(s, t string) bool {
	for s != "" && t != "" {
		sr, size := utf8.DecodeRuneInString(s)
		s = s[size:]
		tr, size := utf8.DecodeRuneInString(t)
		t = t[size:]
		if sr == tr {
			continue
		}
		if 'A' <= sr && sr <= 'Z' {
			sr = sr + 'a' - 'A'
		}
		if 'A' <= tr && tr <= 'Z' {
			tr = tr + 'a' - 'A'
		}
		if sr != tr {
			return false
		}
	}
	return s == t
}
