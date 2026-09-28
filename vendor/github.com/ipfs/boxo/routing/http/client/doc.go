// Package client implements an HTTP client for the [Delegated Routing V1] API
// (IPIP-337).
//
// The client supports finding content providers, peers, IPNS records, and
// closest DHT peers. Responses can be streamed as NDJSON or returned as plain
// JSON.
//
// # Basic Usage
//
//	c, err := client.New("https://delegated-ipfs.dev")
//	if err != nil {
//	    // handle error
//	}
//
//	providers, err := c.FindProviders(ctx, cid)
//
// # Options
//
// The client can be customized with functional options:
//
//   - [WithHTTPClient]: Use a custom [http.Client]
//   - [WithUserAgent]: Set the User-Agent header
//   - [WithIdentity]: Set a private key for authenticated requests
//   - [WithProtocolFilter]: Filter results by transport protocol
//   - [WithAddrFilter]: Filter results by multiaddr pattern
//   - [WithProviderInfo]: Set peer ID and addresses for provide requests
//   - [WithDisabledLocalFiltering]: Skip client-side filtering of results
//
// [Delegated Routing V1]: https://specs.ipfs.tech/routing/http-routing-v1/
package client
