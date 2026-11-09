package webtransport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"

	"github.com/dunglas/httpsfv"
)

// A ClientConn is a WebTransport client connection.
// Multiple sessions can be established concurrently on a ClientConn.
type ClientConn struct {
	conn                 *quic.Conn
	clientConn           *http3.RawClientConn
	sessMgr              *sessionManager
	config               Config
	applicationProtocols []string
	transportCtx         context.Context
}

var _ http.RoundTripper = &ClientConn{}

// NewClientConn creates a WebTransport client connection on an existing QUIC connection.
// The QUIC connection must have datagrams and stream reset partial delivery enabled on both endpoints.
// It must only be called once per QUIC connection.
// The caller owns the QUIC connection and closes it when done.
func (d *Transport) NewClientConn(qconn *quic.Conn) (*ClientConn, error) {
	d.initOnce.Do(func() { d.init() })
	state := qconn.ConnectionState()
	if !state.SupportsDatagrams.Local {
		return nil, errors.New("webtransport: DATAGRAM support required, enable it via QUICConfig.EnableDatagrams")
	}
	if !state.SupportsStreamResetPartialDelivery.Local {
		return nil, errors.New("webtransport: stream reset partial delivery required, enable it via QUICConfig.EnableStreamResetPartialDelivery")
	}

	var config Config
	if d.Config != nil {
		config = *d.Config
	}
	additionalSettings := map[uint64]uint64{settingsWebTransportEnabled: 1}
	config.addSettings(additionalSettings)
	tr := &http3.Transport{EnableDatagrams: true, AdditionalSettings: additionalSettings}

	timeout := d.StreamReorderingTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	sessMgr := newSessionManager(timeout)
	context.AfterFunc(qconn.Context(), sessMgr.Close)

	c := &ClientConn{
		conn:                 qconn,
		clientConn:           tr.NewRawClientConn(qconn),
		sessMgr:              sessMgr,
		config:               config,
		applicationProtocols: slices.Clone(d.ApplicationProtocols),
		transportCtx:         d.ctx,
	}

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
					c.clientConn.HandleBidirectionalStream(str)
					return
				}
				r := &byteCountingReader{ByteReader: quicvarint.NewReader(str)}
				// read the frame type (already peeked above)
				if _, err := quicvarint.Read(r); err != nil {
					return
				}
				// read the session ID
				id, err := quicvarint.Read(r)
				if err != nil {
					return
				}
				if !isValidSessionID(id) {
					qconn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeIDError), "")
					return
				}
				c.sessMgr.AddStream(str, sessionID(id), r.BytesRead)
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
					c.clientConn.HandleUnidirectionalStream(str)
					return
				}
				r := &byteCountingReader{ByteReader: quicvarint.NewReader(str)}
				// read the stream type (already peeked above)
				if _, err := quicvarint.Read(r); err != nil {
					return
				}
				// read the session ID
				id, err := quicvarint.Read(r)
				if err != nil {
					str.CancelRead(quic.StreamErrorCode(http3.ErrCodeGeneralProtocolError))
					return
				}
				if !isValidSessionID(id) {
					qconn.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeIDError), "")
					return
				}
				c.sessMgr.AddUniStream(str, sessionID(id), r.BytesRead)
			}()
		}
	}()

	return c, nil
}

// Dial establishes a new WebTransport session on the client connection.
// Closing the session doesn't close the underlying QUIC connection or other sessions.
func (c *ClientConn) Dial(ctx context.Context, urlStr string, reqHdr http.Header) (*http.Response, *Session, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, nil, err
	}
	return c.dial(ctx, u, reqHdr)
}

func (c *ClientConn) dial(ctx context.Context, u *url.URL, reqHdr http.Header) (*http.Response, *Session, error) {
	if reqHdr == nil {
		reqHdr = make(http.Header)
	} else {
		reqHdr = reqHdr.Clone()
	}
	if len(c.applicationProtocols) > 0 && reqHdr.Get(wtAvailableProtocolsHeader) == "" {
		list := make(httpsfv.List, 0, len(c.applicationProtocols))
		for _, protocol := range c.applicationProtocols {
			list = append(list, httpsfv.NewItem(protocol))
		}
		protocols, err := httpsfv.Marshal(list)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal application protocols: %w", err)
		}
		reqHdr.Set(wtAvailableProtocolsHeader, protocols)
	}
	req := (&http.Request{
		Method: http.MethodConnect,
		Header: reqHdr,
		Proto:  protocolHeader,
		Host:   u.Host,
		URL:    u,
	}).WithContext(ctx)

	// wait for the QUIC handshake to complete
	select {
	case <-c.conn.HandshakeComplete():
	case <-ctx.Done():
		return nil, nil, fmt.Errorf("error waiting for QUIC handshake: %w", context.Cause(ctx))
	case <-c.conn.Context().Done():
		return nil, nil, context.Cause(c.conn.Context())
	case <-c.transportCtx.Done():
		return nil, nil, context.Cause(c.transportCtx)
	}

	state := c.conn.ConnectionState()
	if !state.SupportsDatagrams.Remote {
		return nil, nil, &RequirementsNotMetError{Message: "server didn't enable QUIC datagram support"}
	}
	if !state.SupportsStreamResetPartialDelivery.Remote {
		return nil, nil, &RequirementsNotMetError{Message: "server didn't enable QUIC stream reset partial delivery"}
	}

	select {
	case <-c.clientConn.ReceivedSettings():
	case <-ctx.Done():
		return nil, nil, fmt.Errorf("error waiting for HTTP/3 settings: %w", context.Cause(ctx))
	case <-c.conn.Context().Done():
		return nil, nil, context.Cause(c.conn.Context())
	case <-c.transportCtx.Done():
		return nil, nil, context.Cause(c.transportCtx)
	}
	settings := c.clientConn.Settings()
	if !settings.EnableExtendedConnect {
		return nil, nil, &RequirementsNotMetError{Message: "server didn't enable Extended CONNECT"}
	}
	if !settings.EnableDatagrams {
		return nil, nil, &RequirementsNotMetError{Message: "server didn't enable HTTP/3 datagram support"}
	}
	if settings.Other == nil {
		return nil, nil, &RequirementsNotMetError{Message: "server didn't enable WebTransport"}
	}
	// any non-zero value for SETTINGS_WT_ENABLED means that WebTransport is enabled
	s, ok := settings.Other[settingsWebTransportEnabled]
	if !ok || s == 0 {
		return nil, nil, &RequirementsNotMetError{Message: "server didn't enable WebTransport"}
	}

	requestStr, err := c.clientConn.OpenRequestStream(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := requestStr.SendRequestHeader(req); err != nil {
		requestStr.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		requestStr.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		return nil, nil, err
	}
	// TODO(#136): create the session to allow optimistic opening of streams and sending of datagrams
	rsp, err := requestStr.ReadResponse()
	if err != nil {
		requestStr.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		requestStr.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		return nil, nil, err
	}
	if rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		_ = requestStr.Close()
		return rsp, nil, fmt.Errorf("received status %d", rsp.StatusCode)
	}
	sessID := sessionID(requestStr.StreamID())
	var protocol string
	// Don't send WT_ALPN_ERROR if WT-Protocol is absent: the server didn't
	// negotiate a protocol. Send it only when WT-Protocol is present but invalid.
	if protocolHeader, ok := rsp.Header[http.CanonicalHeaderKey(wtProtocolHeader)]; ok {
		var err error
		protocol, err = c.negotiateProtocol(protocolHeader)
		if err != nil {
			sessErr := &SessionError{ErrorCode: WTALPNErrorCode, Message: err.Error()}
			_ = closeSessionStream(
				requestStr,
				closeSessionCapsule{ErrorCode: sessErr.ErrorCode, Message: sessErr.Message},
			)
			return rsp, nil, sessErr
		}
	}
	sess := newSession(
		context.WithoutCancel(ctx),
		sessID,
		c.conn,
		requestStr,
		protocol,
		c.config.sessionFlowControl(settings),
	)
	c.sessMgr.AddSession(sessID, sess)
	return rsp, sess, nil
}

// RoundTrip executes an HTTP request on the underlying HTTP/3 connection.
func (c *ClientConn) RoundTrip(req *http.Request) (*http.Response, error) {
	return c.clientConn.RoundTrip(req)
}

func (c *ClientConn) negotiateProtocol(theirs []string) (string, error) {
	negotiatedProtocolItem, err := httpsfv.UnmarshalItem(theirs)
	if err != nil {
		return "", fmt.Errorf("webtransport: invalid WT-Protocol header: %w", err)
	}
	negotiatedProtocol, ok := negotiatedProtocolItem.Value.(string)
	if !ok {
		return "", errors.New("webtransport: invalid WT-Protocol header: value is not a string")
	}
	if !slices.Contains(c.applicationProtocols, negotiatedProtocol) {
		return "", fmt.Errorf("webtransport: server selected application protocol %q that wasn't offered", negotiatedProtocol)
	}
	return negotiatedProtocol, nil
}
