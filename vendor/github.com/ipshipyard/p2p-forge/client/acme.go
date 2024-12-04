package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"

	httppeeridauth "github.com/libp2p/go-libp2p/p2p/http/auth"
	"go.uber.org/zap"

	"github.com/caddyserver/certmagic"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/config"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mholt/acmez/v2"
	"github.com/mholt/acmez/v2/acme"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multibase"
)

type P2PForgeCertMgr struct {
	ctx                        context.Context
	cancel                     func()
	forgeDomain                string
	forgeRegistrationEndpoint  string
	ProvideHost                func(host.Host)
	hostFn                     func() host.Host
	hasHost                    func() bool
	cfg                        *certmagic.Config
	log                        *zap.SugaredLogger
	allowPrivateForgeAddresses bool

	hasCert     bool // tracking if we've received a certificate
	certCheckMx sync.RWMutex
}

var (
	defaultCertCache   *certmagic.Cache
	defaultCertCacheMu sync.Mutex
)

func isRelayAddr(a multiaddr.Multiaddr) bool {
	found := false
	multiaddr.ForEach(a, func(c multiaddr.Component) bool {
		found = c.Protocol().Code == multiaddr.P_CIRCUIT
		return !found
	})
	return found
}

// isPublicAddr follows the logic of manet.IsPublicAddr, except it uses
// a stricter definition of "public" for ipv6 by excluding nat64 addresses.
func isPublicAddr(a multiaddr.Multiaddr) bool {
	ip, err := manet.ToIP(a)
	if err != nil {
		return false
	}
	if ip.To4() != nil {
		return manet.IsPublicAddr(a)
	}

	return manet.IsPublicAddr(a) && !manet.IsNAT64IPv4ConvertedIPv6Addr(a)
}

type P2PForgeCertMgrConfig struct {
	forgeDomain                string
	forgeRegistrationEndpoint  string
	forgeAuth                  string
	caEndpoint                 string
	userEmail                  string
	userAgent                  string
	trustedRoots               *x509.CertPool
	storage                    certmagic.Storage
	modifyForgeRequest         func(r *http.Request) error
	onCertLoaded               func()
	log                        *zap.SugaredLogger
	allowPrivateForgeAddresses bool
}

type P2PForgeCertMgrOptions func(*P2PForgeCertMgrConfig) error

func WithOnCertLoaded(fn func()) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.onCertLoaded = fn
		return nil
	}
}

func WithForgeDomain(domain string) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.forgeDomain = domain
		return nil
	}
}

func WithForgeRegistrationEndpoint(endpoint string) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.forgeRegistrationEndpoint = endpoint
		return nil
	}
}

func WithCAEndpoint(caEndpoint string) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.caEndpoint = caEndpoint
		return nil
	}
}

func WithCertificateStorage(storage certmagic.Storage) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.storage = storage
		return nil
	}
}

func WithUserEmail(email string) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.userEmail = email
		return nil
	}
}

// WithForgeAuth sets optional secret be sent with requests to the forge
// registration endpoint.
func WithForgeAuth(forgeAuth string) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.forgeAuth = forgeAuth
		return nil
	}
}

// WithUserAgent sets custom User-Agent sent to the forge.
func WithUserAgent(userAgent string) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.userAgent = userAgent
		return nil
	}
}

/*
// WithHTTPClient sets a custom HTTP Client to be used when talking to registration endpoint.
func WithHTTPClient(h httpClient) error {
	return func(config *P2PForgeCertMgrConfig) error {
		return nil
	}
}
*/

// WithModifiedForgeRequest enables modifying how the ACME DNS challenges are sent to the forge, such as to enable
// custom HTTP headers, etc.
func WithModifiedForgeRequest(fn func(req *http.Request) error) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.modifyForgeRequest = fn
		return nil
	}
}

// WithTrustedRoots is meant for testing
func WithTrustedRoots(trustedRoots *x509.CertPool) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.trustedRoots = trustedRoots
		return nil
	}
}

// WithAllowPrivateForgeAddrs is meant for testing or skipping all the
// connectivity checks libp2p node needs to pass before it can request domain
// and start ACME DNS-01 challenge.
func WithAllowPrivateForgeAddrs() P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.allowPrivateForgeAddresses = true
		return nil
	}
}

func WithLogger(log *zap.SugaredLogger) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.log = log
		return nil
	}
}

// newCertmagicConfig is p2p-forge/client-specific version of
// certmagic.NewDefault() that ensures we have our own cert cache. This is
// necessary to ensure cert maintenance spawned by NewCache does not share
// global certmagic.Default.Storage, and certmagic.Default.Logger and uses
// storage path specific to p2p-forge, and no other instance of certmagic in
// golang application.
func newCertmagicConfig(mgrCfg *P2PForgeCertMgrConfig) *certmagic.Config {
	clog := mgrCfg.log.Desugar()

	defaultCertCacheMu.Lock()
	if defaultCertCache == nil {
		defaultCertCache = certmagic.NewCache(certmagic.CacheOptions{
			GetConfigForCert: func(certmagic.Certificate) (*certmagic.Config, error) {
				// default getter that does not depend on certmagic defaults
				// and respects Config.Storage path
				return newCertmagicConfig(mgrCfg), nil
			},
			Logger: clog,
		})
	}
	certCache := defaultCertCache
	defaultCertCacheMu.Unlock()

	return certmagic.New(certCache, certmagic.Config{
		Storage: mgrCfg.storage,
		Logger:  clog,
	})
}

// NewP2PForgeCertMgr handles the creation and management of certificates that are automatically granted by a forge
// to a libp2p host.
//
// Calling this function signifies your acceptance to
// the CA's Subscriber Agreement and/or Terms of Service. Let's Encrypt is the default CA.
func NewP2PForgeCertMgr(opts ...P2PForgeCertMgrOptions) (*P2PForgeCertMgr, error) {
	mgrCfg := &P2PForgeCertMgrConfig{}
	for _, opt := range opts {
		if err := opt(mgrCfg); err != nil {
			return nil, err
		}
	}

	if mgrCfg.log == nil {
		mgrCfg.log = logging.Logger("p2p-forge/client").Desugar().Sugar()
	}
	if mgrCfg.forgeDomain == "" {
		mgrCfg.forgeDomain = DefaultForgeDomain
	}
	if mgrCfg.caEndpoint == "" {
		mgrCfg.caEndpoint = DefaultCAEndpoint
	}
	if mgrCfg.forgeRegistrationEndpoint == "" {
		if mgrCfg.forgeDomain == DefaultForgeDomain {
			mgrCfg.forgeRegistrationEndpoint = DefaultForgeEndpoint
		} else {
			return nil, fmt.Errorf("must specify the forge registration endpoint if using a non-default forge")
		}
	}

	const defaultStorageLocation = "p2p-forge-certs"
	if mgrCfg.storage == nil {
		mgrCfg.storage = &certmagic.FileStorage{Path: defaultStorageLocation}
	}

	certCfg := newCertmagicConfig(mgrCfg)

	hostChan := make(chan host.Host, 1)
	provideHost := func(host host.Host) { hostChan <- host }
	hasHostChan := make(chan struct{})
	hasHostFn := func() bool {
		select {
		case <-hasHostChan:
			return true
		default:
			return false
		}
	}
	hostFn := sync.OnceValue(func() host.Host {
		defer close(hasHostChan)
		return <-hostChan
	})

	myACME := certmagic.NewACMEIssuer(certCfg, certmagic.ACMEIssuer{ // TODO: UX around user passed emails + agreement
		CA:     mgrCfg.caEndpoint,
		Email:  mgrCfg.userEmail,
		Agreed: true,
		DNS01Solver: &dns01P2PForgeSolver{
			forge:                      mgrCfg.forgeRegistrationEndpoint,
			forgeAuth:                  mgrCfg.forgeAuth,
			hostFn:                     hostFn,
			modifyForgeRequest:         mgrCfg.modifyForgeRequest,
			userAgent:                  mgrCfg.userAgent,
			allowPrivateForgeAddresses: mgrCfg.allowPrivateForgeAddresses,
		},
		TrustedRoots: mgrCfg.trustedRoots,
		Logger:       certCfg.Logger,
	})

	certCfg.Issuers = []certmagic.Issuer{myACME}

	mgr := &P2PForgeCertMgr{
		forgeDomain:                mgrCfg.forgeDomain,
		forgeRegistrationEndpoint:  mgrCfg.forgeRegistrationEndpoint,
		ProvideHost:                provideHost,
		hostFn:                     hostFn,
		hasHost:                    hasHostFn,
		cfg:                        certCfg,
		log:                        mgrCfg.log,
		allowPrivateForgeAddresses: mgrCfg.allowPrivateForgeAddresses,
	}

	certCfg.OnEvent = func(ctx context.Context, event string, data map[string]any) error {
		if event == "cached_managed_cert" {
			sans, ok := data["sans"]
			if !ok {
				return nil
			}
			sanList, ok := sans.([]string)
			if !ok {
				return nil
			}

			name := certName(hostFn().ID(), mgrCfg.forgeDomain)
			for _, san := range sanList {
				if san == name {
					// When the certificate is loaded mark that it has been so we know we are good to use the domain name
					// TODO: This won't handle if the cert expires and cannot get renewed
					mgr.certCheckMx.Lock()
					mgr.hasCert = true
					mgr.certCheckMx.Unlock()
					// Execute user function for on certificate load
					if mgrCfg.onCertLoaded != nil {
						mgrCfg.onCertLoaded()
					}
				}
			}
			return nil
		}
		return nil
	}

	return mgr, nil
}

func (m *P2PForgeCertMgr) Start() error {
	if m.cfg == nil || m.hostFn == nil {
		return errors.New("unable to start without a certmagic and libp2p host")
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	go func() {
		log := m.log.Named("start")
		h := m.hostFn()
		name := certName(h.ID(), m.forgeDomain)
		certExists := localCertExists(m.ctx, m.cfg, name)
		startCertManagement := func() {
			if err := m.cfg.ManageAsync(m.ctx, []string{name}); err != nil {
				log.Error(err)
			}
		}

		if certExists {
			log.Infof("found preexisting cert for %q in local storage", name)
		} else {
			log.Infof("no cert found for %q", name)
		}

		// Start immediatelly if either:
		// (A) preexisting certificate is found in certmagic storage
		// (B) allowPrivateForgeAddresses flag is set
		if certExists || m.allowPrivateForgeAddresses {
			startCertManagement()
		} else {
			// No preexisting cert found.
			// We will get a new one, but don't want to ask for one
			// if our node is not publicly diallable.
			// To avoid ERROR(s) in log and unnecessary retries we wait for libp2p
			// confirmation that node is publicly reachable before sending
			// multiaddrs to p2p-forge's registration endpoint.
			withHostConnectivity(m.ctx, log, h, startCertManagement)
		}
	}()
	return nil
}

// withHostConnectivity executes callback func only after certain libp2p connectivity checks / criteria against passed host are fullfilled.
// The main purpose is to not bother CA ACME endpoint or p2p-forge registration endpoint if we know the peer is not
// ready to use TLS cert.
func withHostConnectivity(ctx context.Context, log *zap.SugaredLogger, h host.Host, callback func()) {
	log.Infof("waiting until libp2p reports event network.ReachabilityPublic")
	sub, err := h.EventBus().Subscribe(new(event.EvtLocalReachabilityChanged))
	if err != nil {
		log.Error(err)
		return
	}
	defer sub.Close()
	select {
	case e := <-sub.Out():
		evt := e.(event.EvtLocalReachabilityChanged) // guaranteed safe
		log.Infof("libp2p reachability status changed to %s", evt.Reachability)
		if evt.Reachability == network.ReachabilityPublic {
			callback()
			return
		} else if evt.Reachability == network.ReachabilityPrivate {
			log.Infof("certificate will not be requested while libp2p reachability status is %s", evt.Reachability)
		}
	case <-ctx.Done():
		if ctx.Err() != context.Canceled {
			log.Error(fmt.Errorf("aborted while waiting for libp2p reachability status discovery: %w", ctx.Err()))
		}
		return
	}
}

func (m *P2PForgeCertMgr) Stop() {
	m.cancel()
}

// TLSConfig returns a tls.Config that managed by the P2PForgeCertMgr
func (m *P2PForgeCertMgr) TLSConfig() *tls.Config {
	tlsCfg := m.cfg.TLSConfig()
	tlsCfg.NextProtos = nil // remove the ACME ALPN
	return tlsCfg
}

func (m *P2PForgeCertMgr) AddrStrings() []string {
	return []string{fmt.Sprintf("/ip4/0.0.0.0/tcp/0/tls/sni/*.%s/ws", m.forgeDomain),
		fmt.Sprintf("/ip6/::/tcp/0/tls/sni/*.%s/ws", m.forgeDomain),
	}
}

// AddressFactory returns a function that rewrites a set of forge managed multiaddresses.
// This should be used with the libp2p.AddrsFactory option to ensure that a libp2p host with forge managed addresses
// only announces those that are active and valid.
func (m *P2PForgeCertMgr) AddressFactory() config.AddrsFactory {
	tlsCfg := m.cfg.TLSConfig()
	tlsCfg.NextProtos = []string{"h2", "http/1.1"} // remove the ACME ALPN and set the HTTP 1.1 and 2 ALPNs

	return m.createAddrsFactory(m.allowPrivateForgeAddresses)
}

// localCertExists returns true if a certificate matching passed name is already present in certmagic.Storage
func localCertExists(ctx context.Context, cfg *certmagic.Config, name string) bool {
	if cfg == nil || cfg.Storage == nil || len(cfg.Issuers) == 0 {
		return false
	}
	acmeIssuer := cfg.Issuers[0].(*certmagic.ACMEIssuer)
	certKey := certmagic.StorageKeys.SiteCert(acmeIssuer.IssuerKey(), name)
	return cfg.Storage.Exists(ctx, certKey)
}

// certName returns a string with DNS wildcard for use in TLS cert ("*.peerid.forgeDomain")
func certName(id peer.ID, suffixDomain string) string {
	pb36 := peer.ToCid(id).Encode(multibase.MustNewEncoder(multibase.Base36))
	return fmt.Sprintf("*.%s.%s", pb36, suffixDomain)
}

func (m *P2PForgeCertMgr) createAddrsFactory(allowPrivateForgeAddrs bool) config.AddrsFactory {
	var p2pForgeWssComponent = multiaddr.StringCast(fmt.Sprintf("/tls/sni/*.%s/ws", m.forgeDomain))

	return func(multiaddrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
		var skipForgeAddrs bool
		if !m.hasHost() {
			skipForgeAddrs = true
		}
		m.certCheckMx.RLock()
		if !m.hasCert {
			skipForgeAddrs = true
		}
		m.certCheckMx.RUnlock()

		return addrFactoryFn(skipForgeAddrs, func() peer.ID { return m.hostFn().ID() }, m.forgeDomain, allowPrivateForgeAddrs, p2pForgeWssComponent, multiaddrs, m.log)
	}
}

type dns01P2PForgeSolver struct {
	forge                      string
	forgeAuth                  string
	hostFn                     func() host.Host
	modifyForgeRequest         func(r *http.Request) error
	userAgent                  string
	allowPrivateForgeAddresses bool
}

func (d *dns01P2PForgeSolver) Wait(ctx context.Context, challenge acme.Challenge) error {
	// TODO: query the authoritative DNS
	time.Sleep(time.Second * 5)
	return nil
}

func (d *dns01P2PForgeSolver) Present(ctx context.Context, challenge acme.Challenge) error {
	h := d.hostFn()
	addrs := h.Addrs()

	var advertisedAddrs []multiaddr.Multiaddr

	if !d.allowPrivateForgeAddresses {
		var publicAddrs []multiaddr.Multiaddr
		for _, addr := range addrs {
			if isPublicAddr(addr) {
				publicAddrs = append(publicAddrs, addr)
			}
		}

		if len(publicAddrs) == 0 {
			return fmt.Errorf("no public address found")
		}
		advertisedAddrs = publicAddrs
	} else {
		advertisedAddrs = addrs
	}

	req, err := ChallengeRequest(ctx, d.forge, challenge.DNS01KeyAuthorization(), advertisedAddrs)
	if err != nil {
		return err
	}

	// Add forge auth header if set
	if d.forgeAuth != "" {
		req.Header.Set(ForgeAuthHeader, d.forgeAuth)
	}

	// Always include User-Agent header
	if d.userAgent == "" {
		d.userAgent = defaultUserAgent
	}
	req.Header.Set("User-Agent", d.userAgent)

	if d.modifyForgeRequest != nil {
		if err := d.modifyForgeRequest(req); err != nil {
			return err
		}
	}

	client := &httppeeridauth.ClientPeerIDAuth{PrivKey: h.Peerstore().PrivKey(h.ID())}
	_, resp, err := client.AuthenticatedDo(http.DefaultClient, req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s : %s", resp.Status, respBody)
	}
	return nil
}

func (d *dns01P2PForgeSolver) CleanUp(ctx context.Context, challenge acme.Challenge) error {
	//TODO: Should we implement this, or is doing delete and Last-Writer-Wins enough?
	return nil
}

var _ acmez.Solver = (*dns01P2PForgeSolver)(nil)
var _ acmez.Waiter = (*dns01P2PForgeSolver)(nil)

func addrFactoryFn(skipForgeAddrs bool, peerIDFn func() peer.ID, forgeDomain string, allowPrivateForgeAddrs bool, p2pForgeWssComponent multiaddr.Multiaddr, multiaddrs []multiaddr.Multiaddr, log *zap.SugaredLogger) []multiaddr.Multiaddr {
	retAddrs := make([]multiaddr.Multiaddr, 0, len(multiaddrs))
	for _, a := range multiaddrs {
		if isRelayAddr(a) {
			retAddrs = append(retAddrs, a)
			continue
		}

		// We expect the address to be of the form: /ipX/<IP address>/tcp/<Port>/tls/sni/*.<forge-domain>/ws
		// We'll then replace the * with the IP address
		withoutForgeWSS := a.Decapsulate(p2pForgeWssComponent)
		if withoutForgeWSS.Equal(a) {
			retAddrs = append(retAddrs, a)
			continue
		}

		index := 0
		var escapedIPStr string
		var ipMaStr string
		var tcpPortStr string
		multiaddr.ForEach(withoutForgeWSS, func(c multiaddr.Component) bool {
			switch index {
			case 0:
				switch c.Protocol().Code {
				case multiaddr.P_IP4:
					ipMaStr = c.String()
					ipAddr := c.Value()
					escapedIPStr = strings.ReplaceAll(ipAddr, ".", "-")
				case multiaddr.P_IP6:
					ipMaStr = c.String()
					ipAddr := c.Value()
					escapedIPStr = strings.ReplaceAll(ipAddr, ":", "-")
					if escapedIPStr[0] == '-' {
						escapedIPStr = "0" + escapedIPStr
					}
					if escapedIPStr[len(escapedIPStr)-1] == '-' {
						escapedIPStr = escapedIPStr + "0"
					}
				default:
					return false
				}
			case 1:
				if c.Protocol().Code != multiaddr.P_TCP {
					return false
				}
				tcpPortStr = c.Value()
			default:
				index++
				return false
			}
			index++
			return true
		})
		if index != 2 || escapedIPStr == "" || tcpPortStr == "" {
			retAddrs = append(retAddrs, a)
			continue
		}

		// It looks like it's a valid forge address, now figure out if we skip these forge addresses
		if skipForgeAddrs {
			continue
		}

		// don't return non-public forge addresses unless explicitly opted in
		if !allowPrivateForgeAddrs && !isPublicAddr(a) {
			continue
		}

		pidStr := peer.ToCid(peerIDFn()).Encode(multibase.MustNewEncoder(multibase.Base36))

		newMaStr := fmt.Sprintf("%s/tcp/%s/tls/sni/%s.%s.%s/ws", ipMaStr, tcpPortStr, escapedIPStr, pidStr, forgeDomain)
		newMA, err := multiaddr.NewMultiaddr(newMaStr)
		if err != nil {
			log.Errorf("error creating new multiaddr from %q: %s", newMaStr, err.Error())
			retAddrs = append(retAddrs, a)
			continue
		}
		retAddrs = append(retAddrs, newMA)
	}
	return retAddrs
}
