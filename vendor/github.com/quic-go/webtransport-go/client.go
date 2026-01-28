package webtransport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"

	"github.com/dunglas/httpsfv"
)

var errNoWebTransport = errors.New("server didn't enable WebTransport")

type Dialer struct {
	// TLSClientConfig is the TLS client config used when dialing the QUIC connection.
	// It must set the h3 ALPN.
	TLSClientConfig *tls.Config

	// QUICConfig is the QUIC config used when dialing the QUIC connection.
	QUICConfig *quic.Config

	// ApplicationProtocols is a list of application protocols that can be negotiated,
	// see section 3.3 of https://www.ietf.org/archive/id/draft-ietf-webtrans-http3-14 for details.
	ApplicationProtocols []string

	// StreamReorderingTime is the time an incoming WebTransport stream that cannot be associated
	// with a session is buffered.
	// This can happen if the response to a CONNECT request (that creates a new session) is reordered,
	// and arrives after the first WebTransport stream(s) for that session.
	// Defaults to 5 seconds.
	StreamReorderingTimeout time.Duration

	// DialAddr is the function used to dial the underlying QUIC connection.
	// If unset, quic.DialAddrEarly will be used.
	DialAddr func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error)

	ctx       context.Context
	ctxCancel context.CancelFunc

	initOnce sync.Once
}

func (d *Dialer) init() {
	d.ctx, d.ctxCancel = context.WithCancel(context.Background())
}

func (d *Dialer) Dial(ctx context.Context, urlStr string, reqHdr http.Header) (*http.Response, *Session, error) {
	d.initOnce.Do(func() { d.init() })

	quicConf := d.QUICConfig
	if quicConf == nil {
		quicConf = &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		}
	} else {
		if !d.QUICConfig.EnableDatagrams {
			return nil, nil, errors.New("webtransport: DATAGRAM support required, enable it via QUICConfig.EnableDatagrams")
		}
		if !d.QUICConfig.EnableStreamResetPartialDelivery {
			return nil, nil, errors.New("webtransport: stream reset partial delivery required, enable it via QUICConfig.EnableStreamResetPartialDelivery")
		}
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
	if len(d.ApplicationProtocols) > 0 && reqHdr.Get(wtAvailableProtocolsHeader) == "" {
		list := httpsfv.List{}
		for _, protocol := range d.ApplicationProtocols {
			list = append(list, httpsfv.NewItem(protocol))
		}
		protocols, err := httpsfv.Marshal(list)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal application protocols: %w", err)
		}
		reqHdr.Set(wtAvailableProtocolsHeader, protocols)
	}

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

	tr := &http3.Transport{EnableDatagrams: true}
	rsp, sess, err := d.handleConn(ctx, tr, qconn, req)
	if err != nil {
		// TODO: use a more specific error code
		// see https://github.com/ietf-wg-webtrans/draft-ietf-webtrans-http3/issues/245
		qconn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
		tr.Close()
		return rsp, nil, err
	}
	context.AfterFunc(sess.Context(), func() {
		qconn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
		tr.Close()
	})
	return rsp, sess, nil
}

func (d *Dialer) handleConn(ctx context.Context, tr *http3.Transport, qconn *quic.Conn, req *http.Request) (*http.Response, *Session, error) {
	timeout := d.StreamReorderingTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	sessMgr := newSessionManager(timeout)
	context.AfterFunc(qconn.Context(), sessMgr.Close)

	conn := tr.NewRawClientConn(qconn)

	go func() {
		for {
			str, err := qconn.AcceptStream(context.Background())
			if err != nil {
				return
			}

			go func() {
				typ, err := quicvarint.Peek(str)
				if err != nil {
					return
				}
				if typ != webTransportFrameType {
					conn.HandleBidirectionalStream(str)
					return
				}
				// read the frame type (already peeked above)
				if _, err := quicvarint.Read(quicvarint.NewReader(str)); err != nil {
					return
				}
				// read the session ID
				id, err := quicvarint.Read(quicvarint.NewReader(str))
				if err != nil {
					return
				}
				sessMgr.AddStream(str, sessionID(id))
			}()
		}
	}()

	go func() {
		for {
			str, err := qconn.AcceptUniStream(context.Background())
			if err != nil {
				return
			}

			go func() {
				typ, err := quicvarint.Peek(str)
				if err != nil {
					return
				}
				if typ != webTransportUniStreamType {
					conn.HandleUnidirectionalStream(str)
					return
				}
				// read the stream type (already peeked above)
				if _, err := quicvarint.Read(quicvarint.NewReader(str)); err != nil {
					return
				}
				// read the session ID
				id, err := quicvarint.Read(quicvarint.NewReader(str))
				if err != nil {
					str.CancelRead(quic.StreamErrorCode(http3.ErrCodeGeneralProtocolError))
					return
				}
				sessMgr.AddUniStream(str, sessionID(id))
			}()
		}
	}()

	select {
	case <-conn.ReceivedSettings():
	case <-ctx.Done():
		return nil, nil, fmt.Errorf("error waiting for HTTP/3 settings: %w", context.Cause(ctx))
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

	requestStr, err := conn.OpenRequestStream(ctx)
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
	var protocol string
	if protocolHeader, ok := rsp.Header[http.CanonicalHeaderKey(wtProtocolHeader)]; ok {
		protocol = d.negotiateProtocol(protocolHeader)
	}
	sessID := sessionID(requestStr.StreamID())
	sess := newSession(context.WithoutCancel(ctx), sessID, qconn, requestStr, protocol)
	sessMgr.AddSession(sessID, sess)
	return rsp, sess, nil
}

func (d *Dialer) negotiateProtocol(theirs []string) string {
	negotiatedProtocolItem, err := httpsfv.UnmarshalItem(theirs)
	if err != nil {
		return ""
	}
	negotiatedProtocol, ok := negotiatedProtocolItem.Value.(string)
	if !ok {
		return ""
	}
	if !slices.Contains(d.ApplicationProtocols, negotiatedProtocol) {
		return ""
	}
	return negotiatedProtocol
}

func (d *Dialer) Close() error {
	d.ctxCancel()
	return nil
}
