package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	httppeeridauth "github.com/libp2p/go-libp2p/p2p/http/auth"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multiaddr"
)

// SendChallengeOption configures a SendChallenge call. Options are applied
// in order; later options override earlier ones for the same field.
type SendChallengeOption func(*sendChallengeOptions) error

type sendChallengeOptions struct {
	httpClient *http.Client
}

// WithChallengeHTTPClient sets the *http.Client used to issue the registration
// POST. The default is http.DefaultClient.
//
// Callers can supply a client with a custom Transport (custom resolver,
// rewritten dial address, alternate root CAs for the registration endpoint
// itself, etc.). Useful for test harnesses that run an in-process forge on a
// loopback address while the PeerID-auth signature must still be scoped to
// the production registration hostname.
//
// The client's Timeout, Transport, CheckRedirect, and Jar are honored as-is;
// PeerID auth is layered on top via httppeeridauth.ClientPeerIDAuth.
func WithChallengeHTTPClient(c *http.Client) SendChallengeOption {
	return func(o *sendChallengeOptions) error {
		if c == nil {
			return fmt.Errorf("WithChallengeHTTPClient: client must not be nil")
		}
		o.httpClient = c
		return nil
	}
}

// SendChallenge submits value for DNS-01 challenge to the p2p-forge HTTP server for the given peerID.
// It requires the corresponding private key and a list of multiaddresses that the peerID is listening on using
// publicly reachable IP addresses.
//
// Optional SendChallengeOption values configure transport-level behavior such
// as the *http.Client used for the registration POST (see WithChallengeHTTPClient).
func SendChallenge(ctx context.Context, baseURL string, privKey crypto.PrivKey, challenge string, addrs []multiaddr.Multiaddr, forgeAuth string, userAgent string, modifyForgeRequest func(r *http.Request) error, opts ...SendChallengeOption) error {
	o := sendChallengeOptions{}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}

	// Create request
	registrationURL := fmt.Sprintf("%s/v1/_acme-challenge", baseURL)
	req, err := ChallengeRequest(ctx, registrationURL, challenge, addrs)
	if err != nil {
		return err
	}

	// Adjust headers if needed
	if forgeAuth != "" {
		req.Header.Set(ForgeAuthHeader, forgeAuth)
	}
	if userAgent == "" {
		userAgent = defaultUserAgent
	}
	req.Header.Set("User-Agent", userAgent)
	if modifyForgeRequest != nil {
		if err := modifyForgeRequest(req); err != nil {
			return err
		}
	}

	httpClient := o.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	// Execute request wrapped in ClientPeerIDAuth
	authClient := &httppeeridauth.ClientPeerIDAuth{PrivKey: privKey}
	_, resp, err := authClient.AuthenticatedDo(httpClient, req)
	if err != nil {
		return fmt.Errorf("libp2p HTTP ClientPeerIDAuth error at %s: %w", registrationURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s error from %s: %q", resp.Status, registrationURL, respBody)
	}
	return nil
}

// ChallengeRequest creates an HTTP Request object for submitting an ACME challenge to the p2p-forge HTTP server for a given peerID.
// Construction of the request requires a list of multiaddresses that the peerID is listening on using
// publicly reachable IP addresses.
//
// Sending the request to the DNS server requires performing HTTP PeerID Authentication for the corresponding peerID
func ChallengeRequest(ctx context.Context, registrationURL string, challenge string, addrs []multiaddr.Multiaddr) (*http.Request, error) {
	maStrs := make([]string, len(addrs))
	for i, addr := range addrs {
		maStrs[i] = addr.String()
	}

	body, err := json.Marshal(&struct {
		Value     string   `json:"value"`
		Addresses []string `json:"addresses"`
	}{
		Value:     challenge,
		Addresses: maStrs,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", registrationURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed while creating a request to %s: %w", registrationURL, err)
	}

	return req, nil
}
