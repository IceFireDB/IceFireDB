package webtransport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

var errNoWebTransport = errors.New("server didn't enable WebTransport")

type Dialer struct {
	// TLSClientConfig is the TLS client config used when dialing the QUIC connection.
	// It must set the h3 ALPN.
	TLSClientConfig *tls.Config

	// QUICConfig is the QUIC config used when dialing the QUIC connection.
	QUICConfig *quic.Config

	// StreamReorderingTime is the time an incoming WebTransport stream that cannot be associated
	// with a session is buffered.
	// This can happen if the response to a CONNECT request (that creates a new session) is reordered,
	// and arrives after the first WebTransport stream(s) for that session.
	// Defaults to 5 seconds.
	StreamReorderingTimeout time.Duration

	// DialAddr is the function used to dial the underlying QUIC connection.
	// If unset, quic.DialAddrEarly will be used.
	DialAddr func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (quic.EarlyConnection, error)

	ctx       context.Context
	ctxCancel context.CancelFunc

	initOnce sync.Once

	conns sessionManager
}

func (d *Dialer) init() {
	timeout := d.StreamReorderingTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	d.conns = *newSessionManager(timeout)
	d.ctx, d.ctxCancel = context.WithCancel(context.Background())
}

func (d *Dialer) Dial(ctx context.Context, urlStr string, reqHdr http.Header) (*http.Response, *Session, error) {
	d.initOnce.Do(func() { d.init() })

	// Technically, this is not true. DATAGRAMs could be sent using the Capsule protocol.
	// However, quic-go currently enforces QUIC datagram support if HTTP/3 datagrams are enabled.
	quicConf := d.QUICConfig
	if quicConf == nil {
		quicConf = &quic.Config{EnableDatagrams: true}
	} else if !d.QUICConfig.EnableDatagrams {
		return nil, nil, errors.New("webtransport: DATAGRAM support required, enable it via QUICConfig.EnableDatagrams")
	}

	tlsConf := d.TLSClientConfig
	if tlsConf == nil {
		tlsConf = &tls.Config{}
	} else {
		tlsConf = tlsConf.Clone()
	}
	if len(tlsConf.NextProtos) == 0 {
		tlsConf.NextProtos = []string{http3.NextProtoH3}
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, nil, err
	}
	if reqHdr == nil {
		reqHdr = http.Header{}
	}
	reqHdr.Set(webTransportDraftOfferHeaderKey, "1")
	req := &http.Request{
		Method: http.MethodConnect,
		Header: reqHdr,
		Proto:  "webtransport",
		Host:   u.Host,
		URL:    u,
	}
	req = req.WithContext(ctx)

	dialAddr := d.DialAddr
	if dialAddr == nil {
		dialAddr = quic.DialAddrEarly
	}
	qconn, err := dialAddr(ctx, u.Host, tlsConf, quicConf)
	if err != nil {
		return nil, nil, err
	}
	tr := &http3.Transport{
		EnableDatagrams: true,
		StreamHijacker: func(ft http3.FrameType, connTracingID quic.ConnectionTracingID, str quic.Stream, e error) (hijacked bool, err error) {
			if isWebTransportError(e) {
				return true, nil
			}
			if ft != webTransportFrameType {
				return false, nil
			}
			id, err := quicvarint.Read(quicvarint.NewReader(str))
			if err != nil {
				if isWebTransportError(err) {
					return true, nil
				}
				return false, err
			}
			d.conns.AddStream(connTracingID, str, sessionID(id))
			return true, nil
		},
		UniStreamHijacker: func(st http3.StreamType, connTracingID quic.ConnectionTracingID, str quic.ReceiveStream, err error) (hijacked bool) {
			if st != webTransportUniStreamType && !isWebTransportError(err) {
				return false
			}
			d.conns.AddUniStream(connTracingID, str)
			return true
		},
	}

	conn := tr.NewClientConn(qconn)
	select {
	case <-conn.ReceivedSettings():
	case <-d.ctx.Done():
		return nil, nil, context.Cause(d.ctx)
	}
	settings := conn.Settings()
	if !settings.EnableExtendedConnect {
		return nil, nil, errors.New("server didn't enable Extended CONNECT")
	}
	if !settings.EnableDatagrams {
		return nil, nil, errors.New("server didn't enable HTTP/3 datagram support")
	}
	if settings.Other == nil {
		return nil, nil, errNoWebTransport
	}
	s, ok := settings.Other[settingsEnableWebtransport]
	if !ok || s != 1 {
		return nil, nil, errNoWebTransport
	}

	requestStr, err := conn.OpenRequestStream(ctx) // TODO: put this on the Connection (maybe introduce a ClientConnection?)
	if err != nil {
		return nil, nil, err
	}
	if err := requestStr.SendRequestHeader(req); err != nil {
		return nil, nil, err
	}
	// TODO(#136): create the session to allow optimistic opening of streams and sending of datagrams
	rsp, err := requestStr.ReadResponse()
	if err != nil {
		return nil, nil, err
	}
	if rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		return rsp, nil, fmt.Errorf("received status %d", rsp.StatusCode)
	}
	return rsp, d.conns.AddSession(conn, sessionID(requestStr.StreamID()), requestStr), nil
}

func (d *Dialer) Close() error {
	d.ctxCancel()
	return nil
}
