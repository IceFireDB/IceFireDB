package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"

	"go.uber.org/zap"

	"github.com/caddyserver/certmagic"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/config"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mholt/acmez/v3"
	"github.com/mholt/acmez/v3/acme"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multibase"
)

type P2PForgeCertMgr struct {
	ctx                        context.Context
	cancel                     func()
	forgeDomain                string
	forgeRegistrationEndpoint  string
	registrationDelay          time.Duration
	ProvideHost                func(host.Host)
	hostFn                     func() host.Host
	hasHost                    func() bool
	certmagic                  *certmagic.Config
	log                        *zap.SugaredLogger
	allowPrivateForgeAddresses bool
	produceShortAddrs          bool

	hasCert     bool // tracking if we've received a certificate
	certCheckMx sync.RWMutex
}

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
	onCertRenewed              func()
	log                        *zap.SugaredLogger
	resolver                   *net.Resolver
	allowPrivateForgeAddresses bool
	produceShortAddrs          bool
	renewCheckInterval         time.Duration
	registrationDelay          time.Duration
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

// WithOnCertRenewed is optional callback executed on cert renewal event
func WithOnCertRenewed(fn func()) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.onCertRenewed = fn
		return nil
	}
}

// WithRenewCheckInterval is meant for testing
func WithRenewCheckInterval(renewCheckInterval time.Duration) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.renewCheckInterval = renewCheckInterval
		return nil
	}
}

// WithRegistrationDelay allows delaying initial registration to ensure node was online for a while before requesting TLS cert.
func WithRegistrationDelay(registrationDelay time.Duration) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.registrationDelay = registrationDelay
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

// WithShortForgeAddrs controls if final addresses produced by p2p-forge addr
// factory are short and start with /dnsX or are longer and the DNS name is
// fully resolved into /ipX /sni components.
//
// Using /dnsX may be beneficial when interop with older libp2p clients is
// required, or when shorter addresses are preferred.
//
// Example multiaddr formats:
//   - When true: /dnsX/<escaped-ip>.<peer-id>.<forge-domain>/tcp/<port>/tls/ws
//   - When false:  /ipX/<ip>/tcp/<port>/tls/sni/<escaped-ip>.<peer-id>.<forge-domain>/ws
func WithShortForgeAddrs(produceShortAddrs bool) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.produceShortAddrs = produceShortAddrs
		return nil
	}
}

func WithLogger(log *zap.SugaredLogger) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.log = log
		return nil
	}
}

// WithResolver allows passing custom DNS resolver to be used for DNS-01 checks.
// By default [net.DefaultResolver] is used.
func WithResolver(resolver *net.Resolver) P2PForgeCertMgrOptions {
	return func(config *P2PForgeCertMgrConfig) error {
		config.resolver = resolver
		return nil
	}
}

// NewP2PForgeCertMgr handles the creation and management of certificates that are automatically granted by a forge
// to a libp2p host.
//
// Calling this function signifies your acceptance to
// the CA's Subscriber Agreement and/or Terms of Service. Let's Encrypt is the default CA.
func NewP2PForgeCertMgr(opts ...P2PForgeCertMgrOptions) (*P2PForgeCertMgr, error) {
	// Init config + apply optional user settings
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
	} else if mgrCfg.caEndpoint == DefaultCATestEndpoint {
		mgrCfg.log.Errorf("initialized with staging endpoint (%s): certificate won't work correctly in web browser; make sure to change to WithCAEndpoint(DefaultCAEndpoint) (%s) before deploying to production or testing in web browser", DefaultCATestEndpoint, DefaultCAEndpoint)
	}
	if mgrCfg.forgeRegistrationEndpoint == "" {
		if mgrCfg.forgeDomain == DefaultForgeDomain {
			mgrCfg.forgeRegistrationEndpoint = DefaultForgeEndpoint
		} else {
			return nil, fmt.Errorf("must specify the forge registration endpoint if using a non-default forge")
		}
	}
	if mgrCfg.storage == nil {
		mgrCfg.storage = &certmagic.FileStorage{Path: DefaultStorageLocation}
	}

	// Wire up resolver for verifying DNS-01 TXT record got published correctly
	if mgrCfg.resolver == nil {
		mgrCfg.resolver = net.DefaultResolver
	}

	// Wire up p2p-forge manager instance
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
	mgr := &P2PForgeCertMgr{
		forgeDomain:                mgrCfg.forgeDomain,
		forgeRegistrationEndpoint:  mgrCfg.forgeRegistrationEndpoint,
		ProvideHost:                provideHost,
		hostFn:                     hostFn,
		hasHost:                    hasHostFn,
		log:                        mgrCfg.log,
		allowPrivateForgeAddresses: mgrCfg.allowPrivateForgeAddresses,
		produceShortAddrs:          mgrCfg.produceShortAddrs,
		registrationDelay:          mgrCfg.registrationDelay,
	}

	// NOTE: callback getter is necessary to avoid circular dependency
	// but also structure code to avoid issues like https://github.com/ipshipyard/p2p-forge/issues/28
	configGetter := func(cert certmagic.Certificate) (*certmagic.Config, error) {
		if mgr.certmagic == nil {
			return nil, errors.New("P2PForgeCertmgr.certmagic is not set")
		}
		return mgr.certmagic, nil
	}

	magicCache := certmagic.NewCache(certmagic.CacheOptions{
		GetConfigForCert:   configGetter,
		RenewCheckInterval: mgrCfg.renewCheckInterval,
		Logger:             mgrCfg.log.Desugar(),
	})

	// Wire up final certmagic config by calling upstream New with sanity checks
	mgr.certmagic = certmagic.New(magicCache, certmagic.Config{
		Storage: mgrCfg.storage,
		Logger:  mgrCfg.log.Desugar(),
	})

	// Wire up Issuer that does brokered DNS-01 ACME challenge
	acmeLog := mgrCfg.log.Named("acme-broker")
	brokeredDNS01Issuer := certmagic.NewACMEIssuer(mgr.certmagic, certmagic.ACMEIssuer{
		CA:     mgrCfg.caEndpoint,
		Email:  mgrCfg.userEmail,
		Agreed: true,
		DNS01Solver: &dns01P2PForgeSolver{
			forgeRegistrationEndpoint:  mgrCfg.forgeRegistrationEndpoint,
			forgeAuth:                  mgrCfg.forgeAuth,
			hostFn:                     mgr.hostFn,
			modifyForgeRequest:         mgrCfg.modifyForgeRequest,
			userAgent:                  mgrCfg.userAgent,
			allowPrivateForgeAddresses: mgrCfg.allowPrivateForgeAddresses,
			log:                        acmeLog.Named("dns01solver"),
			resolver:                   mgrCfg.resolver,
		},
		TrustedRoots: mgrCfg.trustedRoots,
		Logger:       acmeLog.Desugar(),
	})
	mgr.certmagic.Issuers = []certmagic.Issuer{brokeredDNS01Issuer}

	// Wire up onCertLoaded callback
	mgr.certmagic.OnEvent = func(ctx context.Context, event string, data map[string]any) error {
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

		// Execute user function for on certificate cert renewal
		if event == "cert_obtained" && mgrCfg.onCertRenewed != nil {
			if renewal, ok := data["renewal"].(bool); ok && renewal {
				name := certName(hostFn().ID(), mgrCfg.forgeDomain)
				if id, ok := data["identifier"].(string); ok && id == name {
					mgrCfg.onCertRenewed()
				}
			}
			return nil
		}

		return nil
	}

	return mgr, nil
}

func (m *P2PForgeCertMgr) Start() error {
	if m.certmagic == nil || m.hostFn == nil {
		return errors.New("unable to start without a certmagic and libp2p host")
	}
	if m.certmagic.Storage == nil {
		return errors.New("unable to start without a certmagic Cache and Storage set up")
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	go func() {
		start := time.Now()
		log := m.log.Named("start")
		h := m.hostFn()
		name := certName(h.ID(), m.forgeDomain)
		certExists := localCertExists(m.ctx, m.certmagic, name)
		startCertManagement := func() {
			// respect WithRegistrationDelay if no cert exists
			if !certExists && m.registrationDelay != 0 {
				remainingDelay := m.registrationDelay - time.Since(start)
				if remainingDelay > 0 {
					log.Infof("registration delay set to %s, sleeping for remaining %s", m.registrationDelay, remainingDelay)
					time.Sleep(remainingDelay)
				}
			}
			// start internal certmagic instance
			if err := m.certmagic.ManageAsync(m.ctx, []string{name}); err != nil {
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
// It will also delay registration to ensure user-set registrationDelay is respected.
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
	tlsCfg := m.certmagic.TLSConfig()
	tlsCfg.NextProtos = nil // remove the ACME ALPN
	tlsCfg.GetCertificate = m.certmagic.GetCertificate
	return tlsCfg
}

func (m *P2PForgeCertMgr) AddrStrings() []string {
	return []string{
		fmt.Sprintf("/ip4/0.0.0.0/tcp/0/tls/sni/*.%s/ws", m.forgeDomain),
		fmt.Sprintf("/ip6/::/tcp/0/tls/sni/*.%s/ws", m.forgeDomain),
	}
}

// AddressFactory returns a function that rewrites a set of forge managed multiaddresses.
// This should be used with the libp2p.AddrsFactory option to ensure that a libp2p host with forge managed addresses
// only announces those that are active and valid.
func (m *P2PForgeCertMgr) AddressFactory() config.AddrsFactory {
	tlsCfg := m.certmagic.TLSConfig()
	tlsCfg.NextProtos = []string{"h2", "http/1.1"} // remove the ACME ALPN and set the HTTP 1.1 and 2 ALPNs

	return m.createAddrsFactory(m.allowPrivateForgeAddresses, m.produceShortAddrs)
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

func (m *P2PForgeCertMgr) createAddrsFactory(allowPrivateForgeAddrs bool, produceShortAddrs bool) config.AddrsFactory {
	p2pForgeWssComponent := multiaddr.StringCast(fmt.Sprintf("/tls/sni/*.%s/ws", m.forgeDomain))

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

		return addrFactoryFn(skipForgeAddrs, func() peer.ID { return m.hostFn().ID() }, m.forgeDomain, allowPrivateForgeAddrs, produceShortAddrs, p2pForgeWssComponent, multiaddrs, m.log)
	}
}

type dns01P2PForgeSolver struct {
	forgeRegistrationEndpoint  string
	forgeAuth                  string
	hostFn                     func() host.Host
	modifyForgeRequest         func(r *http.Request) error
	userAgent                  string
	allowPrivateForgeAddresses bool
	log                        *zap.SugaredLogger
	resolver                   *net.Resolver
}

func (d *dns01P2PForgeSolver) Wait(ctx context.Context, challenge acme.Challenge) error {
	// Try as long the challenge remains valid.
	// This acts both as sensible timeout and as a way to rate-limit clients using this library.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	// Extract the domain and expected TXT record value from the challenge
	domain := fmt.Sprintf("_acme-challenge.%s", challenge.Identifier.Value)
	expectedTXT := challenge.DNS01KeyAuthorization()
	d.log.Infow("waiting for DNS-01 TXT record to be set", "domain", domain)

	// Check if DNS-01 TXT record is correctly published by the p2p-forge
	// backend. This step ensures we are good citizens: we don't want to move
	// further and bother ACME endpoint with work if we are not confident
	// DNS-01 chalelnge will be successful.
	// We check fast, with backoff to avoid spamming DNS.
	pollInterval := 1 * time.Second
	maxPollInterval := 1 * time.Minute
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for DNS-01 TXT record to be set at %q: %v", domain, ctx.Err())
		case <-ticker.C:
			pollInterval *= 2
			if pollInterval > maxPollInterval {
				pollInterval = maxPollInterval
			}
			ticker.Reset(pollInterval)
			txtRecords, err := d.resolver.LookupTXT(ctx, domain)
			if err != nil {
				d.log.Debugw("dns lookup error", "domain", domain, "error", err)
				continue
			}
			for _, txt := range txtRecords {
				if txt == expectedTXT {
					d.log.Infow("confirmed TXT record for DNS-01 challenge is set", "domain", domain)
					return nil
				}
			}
			d.log.Debugw("no matching TXT record found yet, sleeping", "domain", domain)
		}
	}
}

func (d *dns01P2PForgeSolver) Present(ctx context.Context, challenge acme.Challenge) error {
	d.log.Debugw("getting DNS-01 challenge value from CA", "acme_challenge", challenge)
	dns01value := challenge.DNS01KeyAuthorization()
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
	d.log.Debugw("advertised libp2p addrs for p2p-forge broker to try", "addrs", advertisedAddrs)

	d.log.Debugw("asking p2p-forge broker to set DNS-01 TXT record", "url", d.forgeRegistrationEndpoint, "dns01_value", dns01value)
	err := SendChallenge(ctx,
		d.forgeRegistrationEndpoint,
		h.Peerstore().PrivKey(h.ID()),
		dns01value,
		advertisedAddrs,
		d.forgeAuth,
		d.userAgent,
		d.modifyForgeRequest,
	)
	if err != nil {
		return fmt.Errorf("p2p-forge broker registration error: %w", err)
	}

	return nil
}

func (d *dns01P2PForgeSolver) CleanUp(ctx context.Context, challenge acme.Challenge) error {
	// TODO: Should we implement this, or is doing delete and Last-Writer-Wins enough?
	return nil
}

var (
	_ acmez.Solver = (*dns01P2PForgeSolver)(nil)
	_ acmez.Waiter = (*dns01P2PForgeSolver)(nil)
)

func addrFactoryFn(skipForgeAddrs bool, peerIDFn func() peer.ID, forgeDomain string, allowPrivateForgeAddrs bool, produceShortAddrs bool, p2pForgeWssComponent multiaddr.Multiaddr, multiaddrs []multiaddr.Multiaddr, log *zap.SugaredLogger) []multiaddr.Multiaddr {
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
		var ipVersion string
		var ipMaStr string
		var tcpPortStr string
		multiaddr.ForEach(withoutForgeWSS, func(c multiaddr.Component) bool {
			switch index {
			case 0:
				switch c.Protocol().Code {
				case multiaddr.P_IP4:
					ipVersion = "4"
					ipMaStr = c.String()
					ipAddr := c.Value()
					escapedIPStr = strings.ReplaceAll(ipAddr, ".", "-")
				case multiaddr.P_IP6:
					ipVersion = "6"
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

		b36PidStr := peer.ToCid(peerIDFn()).Encode(multibase.MustNewEncoder(multibase.Base36))

		var newMaStr string
		if produceShortAddrs {
			newMaStr = fmt.Sprintf("/dns%s/%s.%s.%s/tcp/%s/tls/ws", ipVersion, escapedIPStr, b36PidStr, forgeDomain, tcpPortStr)
		} else {
			newMaStr = fmt.Sprintf("%s/tcp/%s/tls/sni/%s.%s.%s/ws", ipMaStr, tcpPortStr, escapedIPStr, b36PidStr, forgeDomain)
		}
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
