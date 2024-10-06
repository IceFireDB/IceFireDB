package webtransport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

const (
	webTransportDraftOfferHeaderKey = "Sec-Webtransport-Http3-Draft02"
	webTransportDraftHeaderKey      = "Sec-Webtransport-Http3-Draft"
	webTransportDraftHeaderValue    = "draft02"
)

const (
	webTransportFrameType     = 0x41
	webTransportUniStreamType = 0x54
)

type Server struct {
	H3 http3.Server

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

	conns *sessionManager
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

	s.conns = newSessionManager(s.timeout())
	if s.CheckOrigin == nil {
		s.CheckOrigin = checkSameOrigin
	}

	// configure the http3.Server
	if s.H3.AdditionalSettings == nil {
		s.H3.AdditionalSettings = make(map[uint64]uint64, 1)
	}
	s.H3.AdditionalSettings[settingsEnableWebtransport] = 1
	s.H3.EnableDatagrams = true
	if s.H3.StreamHijacker != nil {
		return errors.New("StreamHijacker already set")
	}
	s.H3.StreamHijacker = func(ft http3.FrameType, connTracingID quic.ConnectionTracingID, str quic.Stream, err error) (bool /* hijacked */, error) {
		if isWebTransportError(err) {
			return true, nil
		}
		if ft != webTransportFrameType {
			return false, nil
		}
		// Reading the varint might block if the peer sends really small frames, but this is fine.
		// This function is called from the HTTP/3 request handler, which runs in its own Go routine.
		id, err := quicvarint.Read(quicvarint.NewReader(str))
		if err != nil {
			if isWebTransportError(err) {
				return true, nil
			}
			return false, err
		}
		s.conns.AddStream(connTracingID, str, sessionID(id))
		return true, nil
	}
	s.H3.UniStreamHijacker = func(st http3.StreamType, connTracingID quic.ConnectionTracingID, str quic.ReceiveStream, err error) (hijacked bool) {
		if st != webTransportUniStreamType && !isWebTransportError(err) {
			return false
		}
		s.conns.AddUniStream(connTracingID, str)
		return true
	}
	return nil
}

func (s *Server) Serve(conn net.PacketConn) error {
	if err := s.initialize(); err != nil {
		return err
	}
	return s.H3.Serve(conn)
}

// ServeQUICConn serves a single QUIC connection.
func (s *Server) ServeQUICConn(conn quic.Connection) error {
	if err := s.initialize(); err != nil {
		return err
	}
	return s.H3.ServeQUICConn(conn)
}

func (s *Server) ListenAndServe() error {
	if err := s.initialize(); err != nil {
		return err
	}
	return s.H3.ListenAndServe()
}

func (s *Server) ListenAndServeTLS(certFile, keyFile string) error {
	if err := s.initialize(); err != nil {
		return err
	}
	return s.H3.ListenAndServeTLS(certFile, keyFile)
}

func (s *Server) Close() error {
	// Make sure that ctxCancel is defined.
	// This is expected to be uncommon.
	// It only happens if the server is closed without Serve / ListenAndServe having been called.
	s.initOnce.Do(func() {})

	if s.ctxCancel != nil {
		s.ctxCancel()
	}
	if s.conns != nil {
		s.conns.Close()
	}
	err := s.H3.Close()
	s.refCount.Wait()
	return err
}

func (s *Server) Upgrade(w http.ResponseWriter, r *http.Request) (*Session, error) {
	if r.Method != http.MethodConnect {
		return nil, fmt.Errorf("expected CONNECT request, got %s", r.Method)
	}
	if r.Proto != protocolHeader {
		return nil, fmt.Errorf("unexpected protocol: %s", r.Proto)
	}
	if v, ok := r.Header[webTransportDraftOfferHeaderKey]; !ok || len(v) != 1 || v[0] != "1" {
		return nil, fmt.Errorf("missing or invalid %s header", webTransportDraftOfferHeaderKey)
	}
	if !s.CheckOrigin(r) {
		return nil, errors.New("webtransport: request origin not allowed")
	}

	// Wait for SETTINGS
	conn := w.(http3.Hijacker).Connection()
	timer := time.NewTimer(s.timeout())
	defer timer.Stop()
	select {
	case <-conn.ReceivedSettings():
	case <-timer.C:
		return nil, errors.New("webtransport: didn't receive the client's SETTINGS on time")
	}
	settings := conn.Settings()
	if !settings.EnableDatagrams {
		return nil, errors.New("webtransport: missing datagram support")
	}

	w.Header().Add(webTransportDraftHeaderKey, webTransportDraftHeaderValue)
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	str := w.(http3.HTTPStreamer).HTTPStream()
	sessID := sessionID(str.StreamID())
	return s.conns.AddSession(conn, sessID, str), nil
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
