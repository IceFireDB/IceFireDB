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

// SendChallenge submits a challenge to the DNS server for the given peerID.
// It requires the corresponding private key and a list of multiaddresses that the peerID is listening on using
// publicly reachable IP addresses.
func SendChallenge(ctx context.Context, baseURL string, privKey crypto.PrivKey, challenge string, addrs []multiaddr.Multiaddr) error {
	req, err := ChallengeRequest(ctx, baseURL, challenge, addrs)
	if err != nil {
		return err
	}

	client := &httppeeridauth.ClientPeerIDAuth{PrivKey: privKey}
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

// ChallengeRequest creates an HTTP Request object for submitting an ACME challenge to the DNS server for a given peerID.
// Construction of the request requires a list of multiaddresses that the peerID is listening on using
// publicly reachable IP addresses.
//
// Sending the request to the DNS server requires performing HTTP PeerID Authentication for the corresponding peerID
func ChallengeRequest(ctx context.Context, baseURL string, challenge string, addrs []multiaddr.Multiaddr) (*http.Request, error) {
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

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/v1/_acme-challenge", baseURL), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	return req, nil
}
