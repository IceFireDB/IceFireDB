// Package httpnet implements an Exchange network that sends and receives
// Exchange messages from peers' HTTP endpoints.
package httpnet

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/ipfs/boxo/bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("httpnet")

var (
	ErrNoHTTPAddresses = errors.New("AddrInfo does not contain any valid HTTP addresses")
	ErrNoSuccess       = errors.New("none of the peer HTTP endpoints responded successfully to request")
	ErrNotConnected    = errors.New("no HTTP connection has been setup to this peer")
)

var _ network.BitSwapNetwork = (*Network)(nil)

// DefaultUserAgent is sent as a header in all requests.
var DefaultUserAgent = defaultUserAgent() // Usually will result in a "boxo@commitID"

// Defaults for the configurable options.
const (
	DefaultMaxBlockSize            int64 = 2 << 20 // 2MiB: https://specs.ipfs.tech/bitswap-protocol/#block-sizes
	DefaultDialTimeout                   = 5 * time.Second
	DefaultIdleConnTimeout               = 30 * time.Second
	DefaultResponseHeaderTimeout         = 10 * time.Second
	DefaultMaxIdleConns                  = 50
	DefaultInsecureSkipVerify            = false
	DefaultMaxBackoff                    = time.Minute
	DefaultMaxHTTPAddressesPerPeer       = 10
	DefaultMaxDontHaveErrors             = 100
	DefaultHTTPWorkers                   = 64
	// DefaultConnectFailureBackoff is how long Connect waits before
	// re-probing an HTTP endpoint after a failed probe, unless the endpoint
	// requested a different wait with a Retry-After header.
	DefaultConnectFailureBackoff = time.Minute
)

var pingCid = "bafkqaaa" // identity CID

const http2proto = "HTTP/2.0"

const peerstoreSupportsHeadKey = "http-retrieval-head-support"

// Option allows to configure the Network.
type Option func(net *Network)

// WithUserAgent sets the user agent when making requests.
func WithUserAgent(agent string) Option {
	return func(net *Network) {
		net.userAgent = agent
	}
}

// WithMaxBlockSize sets the maximum size of an HTTP response (block).
func WithMaxBlockSize(size int64) Option {
	return func(net *Network) {
		net.maxBlockSize = size
	}
}

// WithDialTimeout sets the maximum time to wait for a connection to be set up.
func WithDialTimeout(t time.Duration) Option {
	return func(net *Network) {
		net.dialTimeout = t
	}
}

// WithIdleConnTimeout sets how long to keep connections alive before closing
// them when no requests happen.
func WithIdleConnTimeout(t time.Duration) Option {
	return func(net *Network) {
		net.idleConnTimeout = t
	}
}

// WithResponseHeaderTimeout sets how long to wait for a response to start
// arriving. It is the time given to the provider to find and start sending
// the block. It does not affect the time it takes to download the request body.
func WithResponseHeaderTimeout(t time.Duration) Option {
	return func(net *Network) {
		net.responseHeaderTimeout = t
	}
}

// WithMaxIdleConns sets how many keep-alive connections we can have where no
// requests are happening.
func WithMaxIdleConns(n int) Option {
	return func(net *Network) {
		net.maxIdleConns = n
	}
}

// WithInsecureSkipVerify allows making HTTPS connections to test servers.
// Use for testing.
func WithInsecureSkipVerify(b bool) Option {
	return func(net *Network) {
		net.insecureSkipVerify = b
	}
}

// WithAllowlist sets the hostnames that we are allowed to connect to via
// HTTP.
func WithAllowlist(hosts []string) Option {
	return func(net *Network) {
		log.Infof("HTTP retrieval allowlist: %s", strings.Join(hosts, ", "))
		net.allowlist = make(map[string]struct{})
		for _, h := range hosts {
			h = strings.TrimSpace(h)
			if h == "" {
				log.Error("empty string in allowlist. Ignoring...")
				continue
			}
			if strings.Contains(h, " ") {
				log.Errorf("allowlist item '%s' contains a whitespace. Ignoring...")
				continue
			}

			net.allowlist[h] = struct{}{}
		}
	}
}

// WithDenylist sets the hostnames that we are prohibited to connect to via
// HTTP.
func WithDenylist(hosts []string) Option {
	return func(net *Network) {
		log.Infof("HTTP retrieval denylist: %s", strings.Join(hosts, ", "))
		net.denylist = make(map[string]struct{})
		for _, h := range hosts {
			h = strings.TrimSpace(h)
			if h == "" {
				log.Error("empty string in denylist. Ignoring...")
				continue
			}
			if strings.Contains(h, " ") {
				log.Errorf("denylist item '%s' contains a whitespace. Ignoring...")
				continue
			}

			net.denylist[h] = struct{}{}
		}
	}
}

// WithMaxHTTPAddressesPerPeer limits how many http addresses we attempt to
// connect to per peer.
func WithMaxHTTPAddressesPerPeer(max int) Option {
	return func(net *Network) {
		net.maxHTTPAddressesPerPeer = max
	}
}

// WithHTTPWorkers controls how many HTTP requests can be done concurrently.
func WithHTTPWorkers(n int) Option {
	return func(net *Network) {
		net.httpWorkers = n
	}
}

// WithMaxDontHaveErrors sets the maximum number of client errors that a peer
// can cause in a row before we disconnect. For example, if set to 50, and a
// peer returns 404 to 50 requests in a row, we will disconnect and signal the
// upper layers to stop making requests to this peer and its endpoints. It may
// be that pending requests will still happen. The HTTP connection might be
// kept until it times-out per the IdleConnTimeout. Requests will resume if a
// provider record is found causing us to "reconnect" to the peer.
func WithMaxDontHaveErrors(threshold int) Option {
	return func(net *Network) {
		net.maxDontHaveErrors = threshold
	}
}

// WithMetricsLabelsForHosts allows to label some metrics that support it
// with the endpoint name that they relate to. For example, this allows
// tracking respose statuses by endpoint. Using '*' means that all endpoints
// are tracked. By default, no endpoints are tracked. Endpoints that are not
// tracked are assigned the label "other". In a scenario where we are making
// requests to many different endpoints, logging all of them with '*' can
// cause the metric cardinality to grow accordingly, and end up affecting
// the performance of the metrics collector (i.e. Prometheus).
func WithMetricsLabelsForEndpoints(hosts []string) Option {
	return func(net *Network) {
		net.trackedEndpoints = make(map[string]struct{})
		for _, h := range hosts {
			net.trackedEndpoints[h] = struct{}{}
		}
	}
}

// WithConnectEventManager allows to set the ConnectEventManager. Upon
// Start(), we will run SetListeners(). If not provided, an event manager will
// be created internally. This allows re-using the event manager among several
// Network instances.
func WithConnectEventManager(evm *network.ConnectEventManager) Option {
	return func(net *Network) {
		net.connEvtMgr = evm
	}
}

// WithCooldownTracker replaces the process-wide cooldown registry with a
// private one made with NewCooldownTracker. Without this option, the Network
// uses SharedCooldownTracker, and it should stay that way for anything that
// talks to endpoints it does not operate: the shared registry is what keeps
// backoff deadlines (Retry-After, HTTP 429 and similar) working across
// short-lived Network instances. Use a private registry only in tests or
// when isolating traffic on purpose.
func WithCooldownTracker(ct *CooldownTracker) Option {
	return func(net *Network) {
		net.cooldownTracker = ct
	}
}

type Network struct {
	// NOTE: Stats must be at the top of the heap allocation to ensure 64bit
	// alignment.
	stats network.Stats

	host   host.Host
	client *http.Client

	closeOnce       sync.Once
	closing         chan struct{}
	receivers       []network.Receiver
	connEvtMgr      *network.ConnectEventManager
	pinger          *pinger
	errorTracker    *errorTracker
	requestTracker  *requestTracker
	cooldownTracker *CooldownTracker

	ongoingConnsLock sync.RWMutex
	ongoingConns     map[peer.ID]struct{}

	// options
	userAgent               string
	maxBlockSize            int64
	dialTimeout             time.Duration
	idleConnTimeout         time.Duration
	responseHeaderTimeout   time.Duration
	maxIdleConns            int
	insecureSkipVerify      bool
	maxHTTPAddressesPerPeer int
	maxDontHaveErrors       int
	httpWorkers             int
	allowlist               map[string]struct{}
	denylist                map[string]struct{}
	trackedEndpoints        map[string]struct{}

	metrics      *metrics
	httpRequests chan httpRequestInfo
}

type httpRequestInfo struct {
	ctx       context.Context
	sender    *httpMsgSender
	entry     bsmsg.Entry
	result    chan<- httpResult
	startTime time.Time
}

type httpResult struct {
	info  httpRequestInfo
	block blocks.Block
	err   *senderError
}

// New returns a BitSwapNetwork supported by underlying IPFS host.
func New(host host.Host, opts ...Option) network.BitSwapNetwork {
	htnet := &Network{
		host:                    host,
		closing:                 make(chan struct{}),
		ongoingConns:            make(map[peer.ID]struct{}),
		userAgent:               defaultUserAgent(),
		maxBlockSize:            DefaultMaxBlockSize,
		dialTimeout:             DefaultDialTimeout,
		idleConnTimeout:         DefaultIdleConnTimeout,
		responseHeaderTimeout:   DefaultResponseHeaderTimeout,
		maxIdleConns:            DefaultMaxIdleConns,
		insecureSkipVerify:      DefaultInsecureSkipVerify,
		maxHTTPAddressesPerPeer: DefaultMaxHTTPAddressesPerPeer,
		maxDontHaveErrors:       DefaultMaxDontHaveErrors,
		httpWorkers:             DefaultHTTPWorkers,
		httpRequests:            make(chan httpRequestInfo),
	}

	for _, opt := range opts {
		opt(htnet)
	}

	htnet.metrics = newMetrics(htnet.trackedEndpoints)

	reqTracker := newRequestTracker()
	htnet.requestTracker = reqTracker

	// Default to the process-wide registry so backoff deadlines survive
	// this Network instance. See SharedCooldownTracker.
	if htnet.cooldownTracker == nil {
		htnet.cooldownTracker = SharedCooldownTracker()
	}

	netdialer := &net.Dialer{
		// Timeout for connects to complete.
		Timeout:   htnet.dialTimeout,
		KeepAlive: 15 * time.Second,
		// TODO for go1.23
		// // KeepAlive config for sending probes for an active
		// // connection.
		// KeepAliveConfig: net.KeepAliveConfig{
		// 	Enable:   true,
		// 	Idle:     15 * time.Second, // default
		// 	Interval: 15 * time.Second, // default
		// 	Count:    2,                // default would be 9
		// },
	}

	// Re: wasm: see
	// https://cs.opensource.google/go/go/+/266626211e40d1f2c3a34fa4cd2023f5310cbd7d
	// In wasm builds custom Dialer gets ignored. DefaultTransport makes
	// sure it sets DialContext to nil for wasm builds as to not break the
	// "contract". Probably makes no difference in the end, but we do the
	// same, just in case.
	dialCtx := netdialer.DialContext
	if http.DefaultTransport.(*http.Transport).DialContext == nil {
		dialCtx = nil
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: htnet.insecureSkipVerify,
	}

	t := &http.Transport{
		TLSClientConfig:   tlsCfg,
		Proxy:             http.ProxyFromEnvironment,
		DialContext:       dialCtx,
		ForceAttemptHTTP2: true,
		// MaxIdleConns: how many keep-alive conns can we have without
		// requests.
		MaxIdleConns: htnet.maxIdleConns,
		// IdleConnTimeout: how long can a keep-alive connection stay
		// around without requests.
		IdleConnTimeout:        htnet.idleConnTimeout,
		ResponseHeaderTimeout:  htnet.responseHeaderTimeout,
		ExpectContinueTimeout:  1 * time.Second,
		MaxResponseHeaderBytes: 2 << 10,  // 2KiB
		ReadBufferSize:         16 << 10, // 16KiB. Default is 4KiB. 16KiB is max TLS buffer size.
	}

	c := &http.Client{
		Transport: t,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// we do not follow redirects. Providers should keep
			// announcements up to
			// date. https://github.com/boxo/issues/862.
			return http.ErrUseLastResponse
		},
	}
	htnet.client = c

	pinger := newPinger(htnet)
	htnet.pinger = pinger

	et := newErrorTracker(htnet)
	htnet.errorTracker = et

	for i := 0; i < htnet.httpWorkers; i++ {
		go htnet.httpWorker(i)
	}

	return htnet
}

// Start sets up the given receivers to be notified when message responses are
// received. It also starts the connection event manager. Start must be called
// before using the Network.
func (ht *Network) Start(receivers ...network.Receiver) {
	log.Info("HTTP raw block retrieval system started")
	ht.receivers = receivers
	connectionListeners := make([]network.ConnectionListener, len(receivers))
	for i, v := range receivers {
		connectionListeners[i] = v
	}

	if ht.connEvtMgr == nil {
		ht.connEvtMgr = network.NewConnectEventManager(connectionListeners...)
	} else {
		ht.connEvtMgr.SetListeners(connectionListeners...)
	}

	ht.connEvtMgr.Start()
}

// Stop stops the connect event manager associated with this network.
// Other methods should no longer be used after calling Stop().
//
// The cooldown registry is deliberately left alone: by default it is the
// process-wide SharedCooldownTracker, which must outlive this instance so
// backoff deadlines survive Network churn. It holds no resources that need
// shutting down.
func (ht *Network) Stop() {
	ht.connEvtMgr.Stop()
	ht.closeOnce.Do(func() {
		close(ht.closing)
	})
}

// Ping sends a probe to the peer's endpoints and returns the measured
// latency. Endpoints in cooldown are not probed; when all of them are cooling
// down, no request is made and the result carries an error.
func (ht *Network) Ping(ctx context.Context, p peer.ID) ping.Result {
	return ht.pinger.ping(ctx, p)
}

// Latency returns the EWMA latency for the given peer. The estimate is seeded
// from the Connect probe and updated with response times of real retrieval
// requests.
func (ht *Network) Latency(p peer.ID) time.Duration {
	return ht.pinger.latency(p)
}

func (ht *Network) Host() host.Host {
	return ht.host
}

func (ht *Network) senderURLs(p peer.ID) []*senderURL {
	pi := ht.host.Peerstore().PeerInfo(p)
	urls := network.ExtractURLsFromPeer(pi)
	if len(urls) == 0 {
		return nil
	}
	return ht.cooldownTracker.fillSenderURLs(urls)
}

// IsConnectedToPeer returns true if the peer is in the connected registry,
// which means Connect() found a working HTTP endpoint for it and
// DisconnectFrom() has not been called since.
func (ht *Network) IsConnectedToPeer(ctx context.Context, p peer.ID) bool {
	// only answer this question while no one is connecting or
	// disconnecting.
	ht.ongoingConnsLock.RLock()
	defer ht.ongoingConnsLock.RUnlock()
	return ht.pinger.isConnected(p)
}

// SendMessage sends the given message to the given peer. It uses
// NewMessageSender under the hood, with default options.
func (ht *Network) SendMessage(ctx context.Context, p peer.ID, msg bsmsg.BitSwapMessage) error {
	if len(msg.Wantlist()) == 0 {
		return nil
	}

	log.Debugf("SendMessage: %s", p)

	// Note: SendMessage seems to only be used to send cancellations.
	// So default options are fine.
	sender, err := ht.NewMessageSender(ctx, p, nil)
	if err != nil {
		return err
	}
	return sender.SendMsg(ctx, msg)
}

// Self returns the local peer ID.
func (ht *Network) Self() peer.ID {
	return ht.host.ID()
}

// lockConnectingPeer locks code around connecting/disconnecting to avoid
// answering questions about connection state while a connect/disconnect
// operation is ongoing. Also avoid doing them twice, or simultaneously.
func (ht *Network) lockConnectingPeer(p peer.ID) {
	ht.ongoingConnsLock.Lock()
	ht.ongoingConns[p] = struct{}{}
	ht.ongoingConnsLock.Unlock()
}

// unlockConnectingPeer unlocks code around connecting/disconnecting to avoid
// answering questions about connection state while a connect/disconnect
// operation is ongoing. Also avoid doing them twice, or simultaneously.
func (ht *Network) unlockConnectingPeer(p peer.ID) {
	ht.ongoingConnsLock.Lock()
	delete(ht.ongoingConns, p)
	ht.ongoingConnsLock.Unlock()
}

// Connect attempts setting up an HTTP connection to the given peer. The given
// AddrInfo must include at least one HTTP endpoint for the peer. HTTP URLs in
// AddrInfo will be tried by making an HTTP request to "/ipfs/bafkqaaa", which
// is the CID for an empty raw block (inlined). A URL works when the endpoint
// answers in a way that shows it understood the request. On success the peer
// is marked as "connected", setting it up to handle messages and make
// requests, and its latency estimate is seeded from the probe round trip.
// Endpoints whose probe failed are not probed again until their cooldown
// expires: the Retry-After deadline when the endpoint sent one, otherwise
// DefaultConnectFailureBackoff.
func (ht *Network) Connect(ctx context.Context, pi peer.AddrInfo) error {
	// Connect is called when finding provider records. We should avoid
	// reconnecting all the time. We should avoid re-testing broken
	// addresses all the time as well. For this reason we assume that if
	// we are connected to an HTTP, the working urls we added the first
	// time are correct, and we will only re-do the effort when we are not
	// connected.
	p := pi.ID
	connected := ht.IsConnectedToPeer(ctx, p)
	if connected {
		ht.connEvtMgr.Connected(p)
		log.Debugf("skipping connect, already connected to %s", p)
		return nil
	}

	ht.lockConnectingPeer(p)
	defer ht.unlockConnectingPeer(p)

	urls := network.ExtractURLsFromPeer(pi)

	errs := []error{ErrNoSuccess}

	// Filter addresses based on allow and denylists
	var filteredURLs []network.ParsedURL
	for _, u := range urls {
		host := u.URL.Hostname()

		// Filter out if allowlist is enabled and it is not allowed,
		// OR if host is in denylist.
		_, inAllowlist := ht.allowlist[host]
		allowed := (len(ht.allowlist) == 0) || inAllowlist
		_, denied := ht.denylist[host]
		if allowed && !denied {
			filteredURLs = append(filteredURLs, u)
		} else {
			errs = append(errs, fmt.Errorf("%s: address not allowed per allow/denylist", u.Multiaddress.String()))
		}
	}
	urls = filteredURLs
	if len(urls) == 0 {
		return errors.Join(errs...)
	}
	if len(urls) > ht.maxHTTPAddressesPerPeer {
		urls = urls[0:ht.maxHTTPAddressesPerPeer]
	}

	// Try to talk to the peer by making HTTP requests to its urls and
	// recording which ones work. This allows re-using the connections
	// that we are about to open next time with the client. We call
	// peer.Connected() on success.
	var workingAddrs []multiaddr.Multiaddr
	var cooledAddrs []multiaddr.Multiaddr
	var probeRTT time.Duration
	supportsHead := true
	for _, u := range urls {
		// Respect an ongoing cooldown for this host without making any
		// request. The address is kept as a failover target below:
		// senders skip it while the cooldown lasts and use it again
		// once it lapses, instead of losing it for the lifetime of the
		// connection.
		if dl, cooling := ht.cooldownTracker.inCooldown(u.URL.Host); cooling {
			errs = append(errs, fmt.Errorf("%s: host in cooldown until %s", u.Multiaddress.String(), dl))
			cooledAddrs = append(cooledAddrs, u.Multiaddress)
			continue
		}

		// If head works we assume GET works too.
		start := time.Now()
		status, retryAfter, err := ht.connectToURL(ctx, pi.ID, u, "HEAD")
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %s", u.Multiaddress.String(), err))
			// abort if context cancelled
			if ctxErr := ctx.Err(); ctxErr != nil {
				return errors.Join(errs...)
			}

			if status == http.StatusTooManyRequests {
				// The endpoint is throttling the probe itself:
				// back off from the host, do not try GET.
				ht.startCooldown(u.URL.Host, retryAfter)
				continue
			}
		} else {
			if probeRTT == 0 {
				probeRTT = time.Since(start)
			}
			workingAddrs = append(workingAddrs, u.Multiaddress)
			continue
		}

		// HEAD did not work. Try GET.
		supportsHead = false

		start = time.Now()
		_, retryAfter, err = ht.connectToURL(ctx, pi.ID, u, "GET")
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %s", u.Multiaddress.String(), err))
			if ctx.Err() != nil {
				return errors.Join(errs...)
			}
			// Both methods failed: back off from the host.
			ht.startCooldown(u.URL.Host, retryAfter)
			continue
		}
		if probeRTT == 0 {
			probeRTT = time.Since(start)
		}
		workingAddrs = append(workingAddrs, u.Multiaddress)
	}

	// Bail out if no working urls found.
	if len(workingAddrs) == 0 {
		err := errors.Join(errs...)
		log.Debug(err)
		return err
	}

	// Cooled addresses were never probed, so HEAD support cannot be
	// assumed for the peer.
	if len(cooledAddrs) > 0 {
		supportsHead = false
	}

	// We have some working urls, keep the bitswap providers in case we fail over.
	// Add the working and the cooled-but-untested addresses to the
	// peerstore. Clean the others.
	ps := ht.host.Peerstore()
	ps.ClearAddrs(p)
	ps.AddAddrs(p, append(workingAddrs, cooledAddrs...), peerstore.PermanentAddrTTL)
	// Record whether HEAD test passed for all urls - ignoring error
	_ = ps.Put(pi.ID, peerstoreSupportsHeadKey, supportsHead)

	ht.pinger.markConnected(p)
	// Seed the latency estimate from the probe, so consumers see a real
	// value without any extra request. Real retrieval responses update it
	// from here on. Recorded unconditionally: this point is only reached
	// after a probe succeeded, and a coarse clock may have measured that
	// probe as zero; the recording floor turns it into a valid sample.
	ht.pinger.recordLatencyIfConnected(p, probeRTT)
	ht.connEvtMgr.Connected(p)

	log.Debugf("connect success to %s (supports HEAD: %t)", p, supportsHead)
	// We "connected"
	return nil
}

// startCooldown registers a failed-probe cooldown for the host, honoring the
// Retry-After deadline when the endpoint provided one.
func (ht *Network) startCooldown(host string, retryAfter time.Time) {
	if retryAfter.IsZero() {
		ht.cooldownTracker.setByDuration(host, DefaultConnectFailureBackoff)
		return
	}
	ht.cooldownTracker.setByDate(host, retryAfter)
}

// connectToURL perform a pingCid request  against the given URL using the given method. An error is returned if we interprete that the server does not understand the request (not a valid IPFS gateway that we can use for HTTP requests).  That happens if the client.Do fails, if the remote endpoint does not support HTTP/2, or if the remote endpoint errors in a way that suggests it is not a gateway. Some requests are considered successful even if they return an http error code if we can assume that the server has understood the request. The response status code is returned in any case, along with the Retry-After deadline when a throttling response carried one.
func (ht *Network) connectToURL(ctx context.Context, p peer.ID, u network.ParsedURL, method string) (int, time.Time, error) {
	req, err := buildRequest(ctx, u, method, pingCid, ht.userAgent)
	if err != nil {
		log.Debug(err)
		return 0, time.Time{}, err
	}

	log.Debugf("connect/ping request to %s %s %q", p, method, req.URL)
	resp, err := ht.client.Do(req)
	if err != nil {
		return 0, time.Time{}, err
	}
	defer resp.Body.Close()

	// For HTTP, the address can only be a LAN IP as otherwise it would have
	// been filtered out before.
	// So IF it is HTTPS and not http2, we abort because we don't want
	// requests to non-local hosts without http2.
	if u.URL.Scheme == "https" && resp.Proto != http2proto {
		err = fmt.Errorf("%s://%q is not using HTTP/2 (%s)", req.URL.Scheme, req.URL.Host, resp.Proto)
		log.Warn(err)
		return resp.StatusCode, time.Time{}, err
	}

	// probe success.
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		log.Debugf("connect/ping request to %s %s succeeded: %d", p, req.URL, resp.StatusCode)
		io.Copy(io.Discard, resp.Body) // read all body data so that connection can be reused
		return resp.StatusCode, time.Time{}, nil
	}

	if resp.StatusCode == http.StatusInternalServerError {
		limReader := &io.LimitedReader{
			R: resp.Body,
			N: 512,
		}

		body, err := io.ReadAll(limReader)
		if err != nil {
			return resp.StatusCode, time.Time{}, err
		}

		// The endpoint understands ipld.
		if isKnownNotFoundError(string(body)) {
			log.Debugf("connect/ping request to %s %s succeeded despite status code (known error): %d / %s", p, req.URL, resp.StatusCode, string(body))
			return resp.StatusCode, time.Time{}, nil
		}
	}

	// Per the path-gateway spec, throttling responses should carry
	// Retry-After. Surface it so the caller can size the cooldown.
	var retryAfter time.Time
	switch resp.StatusCode {
	case http.StatusTooManyRequests,
		http.StatusServiceUnavailable,
		http.StatusBadGateway,
		http.StatusGatewayTimeout:
		if ra, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			retryAfter = ra
		}
	}

	log.Debugf("connect error: %d <- %q (%s)", resp.StatusCode, req.URL, p)
	// We made a proper request and got a 5xx back.
	// We cannot consider this a working connection.
	return resp.StatusCode, retryAfter, errors.New("testCid request not understood by the server")
}

// DisconnectFrom marks this peer as Disconnected in the connection event
// manager, removes it from the connected registry and forgets its recorded
// latency.
func (ht *Network) DisconnectFrom(ctx context.Context, p peer.ID) error {
	ht.lockConnectingPeer(p)
	defer ht.unlockConnectingPeer(p)

	log.Debugf("disconnecting from %s", p)
	ht.connEvtMgr.Disconnected(p) // notify everywhere that we are going offline

	ht.pinger.markDisconnected(p)
	ht.errorTracker.stopTracking(p)

	// coolDownTracker: we leave untouched. We want to keep
	// ongoing cooldowns there in case we reconnect to this peer.

	return nil
}

// TagPeer uses the host's ConnManager to tag a peer.
func (ht *Network) TagPeer(p peer.ID, tag string, w int) {
	ht.host.ConnManager().TagPeer(p, tag, w)
}

// UntagPeer uses the host's ConnManager to untag a peer.
func (ht *Network) UntagPeer(p peer.ID, tag string) {
	ht.host.ConnManager().UntagPeer(p, tag)
}

// Protect does nothing. The purpose of Protect is to mantain connections as
// long as they are used. But our connections are already maintained as long
// as they are, and closed when not.
func (ht *Network) Protect(p peer.ID, tag string) {
}

// Unprotect does nothing. The purpose of Unprotect is to be able to close
// connections when they are no longer relevant. Our connections are already
// closed when they are not used. It returns always true as technically our
// connections are potentially still protected as long as they are used.
func (ht *Network) Unprotect(p peer.ID, tag string) bool {
	return true
}

// Stats returns message counts for this peer. Each message sent is an HTTP
// requests. Each message received is an HTTP response.
func (ht *Network) Stats() network.Stats {
	return network.Stats{
		MessagesRecvd: atomic.LoadUint64(&ht.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&ht.stats.MessagesSent),
	}
}

func (ht *Network) httpWorker(i int) {
	for {
		select {
		case <-ht.closing:
			return
		case reqInfo := <-ht.httpRequests:
			retryLaterErrors := 0
			var urlIgnore []*senderURL
			for {
				// bestURL
				u, err := reqInfo.sender.bestURL(urlIgnore)
				if err != nil {
					reqInfo.result <- httpResult{
						info: reqInfo,
						err: &senderError{
							Type: typeFatal,
							Err:  err,
						},
					}
					break // stop retry loop
				}

				// no urls to retry left.
				if u == nil {
					reqInfo.result <- httpResult{
						info: reqInfo,
						err: &senderError{
							Type: typeClient,
							Err:  nil,
						},
					}
					break // stop retry loop
				}

				b, serr := reqInfo.sender.tryURL(
					reqInfo.ctx,
					u,
					reqInfo.entry,
				)

				result := httpResult{
					info:  reqInfo,
					block: b,
					err:   serr,
				}

				if serr != nil {
					switch serr.Type {
					case typeRetryLater:
						// This error signals that we
						// should retry but if things
						// keep failing we consider it
						// a serverError. When
						// multiple urls, retries may
						// happen on a different url.
						retryLaterErrors++
						if retryLaterErrors%2 == 0 {
							// we retried same CID 2 times. No luck.
							// Increase server errors.
							// Start ignoring urls.
							result.err.Type = typeServer
							urlIgnore = append(urlIgnore, u)
							u.serverErrors.Add(1)
						}
						continue // retry request again
					case typeClient:
						urlIgnore = append(urlIgnore, u)
						continue // retry again ignoring current url
					case typeContext:
					case typeFatal:
						// noop. Return result. tryURL
						// never returns
						// typeFatal. Error logging
						// happens in the result
						// collector
					case typeServer:
						u.serverErrors.Add(1)
						continue // retry until bestURL forces abort

					default:
						panic("unknown sender error type")
					}
				}

				reqInfo.result <- result
				break // exit retry loop
			}
		}
	}
}

// buildRequests sets up common settings for making a requests.
func buildRequest(ctx context.Context, u network.ParsedURL, method string, cid string, userAgent string) (*http.Request, error) {
	// copy url
	sendURL, _ := url.Parse(u.URL.String())
	sendURL.RawQuery = "format=raw"
	sendURL.Path += "/ipfs/" + cid

	req, err := http.NewRequestWithContext(ctx,
		method,
		sendURL.String(),
		nil,
	)
	if err != nil {
		log.Error("error building request:", err)
		return nil, err
	}

	headers := make(http.Header)
	headers.Add("Accept", "application/vnd.ipld.raw")
	headers.Add("User-Agent", userAgent)
	if u.SNI != "" {
		headers.Add("Host", u.SNI)
	}
	req.Header = headers
	return req, nil
}

// NewMessageSender returns a MessageSender implementation which sends the
// given message to the given peer over HTTP.
// An error is returned of the peer has no known HTTP endpoints.
func (ht *Network) NewMessageSender(ctx context.Context, p peer.ID, opts *network.MessageSenderOpts) (network.MessageSender, error) {
	log.Debugf("NewMessageSender: %s", p)

	// cooldowns made by other senders between now and SendMsg will not be
	// taken into account since we access that info here only. From that
	// point, we only react to cooldowns/errors received by this message
	// sender and not others. This is mostly fine given how MessageSender
	// is used as part of MessageQueue:
	//
	// * We expect peers to be associated with single urls so there will
	// not be multiple message sender for the same url normally.
	// * We remember cooldowns between message senders (i.e. when a queue
	// dies and a new one is created).
	// * We track cooldowns in the urls for the lifetime of this sender.
	//
	// This way we minimize lock contention around the cooldown map, with
	// one read access per message sender only.

	// Error when we have not called Connect() for this peer or we
	// Disconnected and this is a late item.  We cannot simply
	// re-connect: perhaps we disconnected based on client-errors and this
	// is a left-over request for that endpoint. If we reconnect here we
	// start from scratch and the disconnection was for nothing because it
	// gets overridden.
	if !ht.IsConnectedToPeer(ctx, p) {
		log.Debugf("NewMessageSender: cannot send message over HTTP: not connected to %s", p)
		return nil, ErrNotConnected
	}

	// Check that we have HTTP urls.
	urls := ht.senderURLs(p)
	if len(urls) == 0 {
		log.Debugf("NewMessageSender: aborting: no HTTPAddresses for %s", p)
		return nil, ErrNoHTTPAddresses
	}

	senderOpts := setSenderOpts(opts)

	return &httpMsgSender{
		// ctx ??
		ht:      ht,
		peer:    p,
		urls:    urls,
		closing: make(chan struct{}, 1),
		opts:    senderOpts,
	}, nil
}

// defaultUserAgent returns a useful user agent version string allowing us to
// identify requests coming from official releases of this module vs forks.
func defaultUserAgent() (ua string) {
	p := reflect.ValueOf(Network{}).Type().PkgPath()
	// we have monorepo, so stripping the remainder
	importPath := strings.TrimSuffix(p, "/bitswap/network/httpnet")

	ua = importPath
	var module *debug.Module
	if bi, ok := debug.ReadBuildInfo(); ok {
		// If debug.ReadBuildInfo was successful, we can read Version by finding
		// this client in the dependency list of the app that has it in go.mod
		for _, dep := range bi.Deps {
			if dep.Path == importPath {
				module = dep
				break
			}
		}
		if module != nil {
			ua += "@" + module.Version
			return
		}
		ua += "@unknown"
	}
	return
}
