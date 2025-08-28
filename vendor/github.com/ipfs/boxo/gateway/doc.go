// Package gateway provides an HTTP gateway for serving IPFS content over HTTP.
//
// The gateway implements the [IPFS HTTP Gateway] specifications, including:
//
//   - [Path Gateway]: Serves IPFS content from URL paths like /ipfs/ and /ipns/
//   - [Subdomain Gateway]: Serves IPFS content from subdomains for origin isolation
//   - [Trustless Gateway]: Provides verifiable responses for light clients ([application/vnd.ipld.raw], [application/vnd.ipld.car], and [application/vnd.ipfs.ipns-record])
//   - [DNSLink Gateway]: Resolves DNSLink names to IPFS content
//   - [Web Redirects File]: Supports _redirects file for URL redirection (when deserialized responses are enabled)
//
// It supports both trustless and deserialized response formats with features for:
//
//   - Serving UnixFS files and directories
//   - Streaming UnixFS directories as TAR archives (when deserialized responses are enabled)
//   - Content type detection and handling
//   - Raw block responses ([application/vnd.ipld.raw])
//   - CAR streaming ([application/vnd.ipld.car])
//   - IPNS record responses ([application/vnd.ipfs.ipns-record])
//   - DNSLink resolution
//   - IPNS (InterPlanetary Name System) resolution
//   - Rate limiting and resource protection
//   - Prometheus metrics
//
// # Basic Usage
//
// To create a gateway handler, you need an [IPFSBackend] implementation and a [Config]:
//
//	backend := ... // Your IPFSBackend implementation
//	config := gateway.Config{
//	    DeserializedResponses: true,
//	    MaxConcurrentRequests: 4096,
//	    RetrievalTimeout:      30 * time.Second,
//	}
//	handler := gateway.NewHandler(config, backend)
//
// The handler can then be used with any standard Go HTTP server:
//
//	http.ListenAndServe(":8080", handler)
//
// # Configuration
//
// The gateway behavior can be customized through the [Config] structure:
//
//   - [Config.DeserializedResponses]: Enable/disable deserialized responses (HTML, images, etc.)
//   - [Config.MaxConcurrentRequests]: Limit concurrent HTTP requests (default: 4096)
//   - [Config.RetrievalTimeout]: Maximum time between data writes (default: 30s)
//   - [Config.PublicGateways]: Configure behavior for specific hostnames
//   - [Config.NoDNSLink]: Disable DNSLink resolution
//
// # Resource Protection
//
// The gateway includes built-in resource protection mechanisms:
//
//   - Concurrent request limiting to prevent overload
//   - Retrieval timeouts to prevent stuck requests
//   - Prometheus metrics for monitoring
//
// When limits are exceeded, the gateway returns appropriate HTTP status codes:
//   - 429 Too Many Requests: When concurrent request limit is reached
//   - 504 Gateway Timeout: When retrieval timeout is exceeded
//
// # Metrics
//
// The gateway exposes Prometheus metrics for monitoring:
//
//   - ipfs_http_gw_concurrent_requests: Current number of concurrent requests
//   - ipfs_http_gw_responses_total: Total responses by status code
//   - ipfs_http_gw_retrieval_timeouts_total: Retrieval timeout events
//
// [IPFS HTTP Gateway]: https://specs.ipfs.tech/http-gateways/
// [Path Gateway]: https://specs.ipfs.tech/http-gateways/path-gateway/
// [Subdomain Gateway]: https://specs.ipfs.tech/http-gateways/subdomain-gateway/
// [Trustless Gateway]: https://specs.ipfs.tech/http-gateways/trustless-gateway/
// [DNSLink Gateway]: https://specs.ipfs.tech/http-gateways/dnslink-gateway/
// [Web Redirects File]: https://specs.ipfs.tech/http-gateways/web-redirects-file/
// [application/vnd.ipld.raw]: https://www.iana.org/assignments/media-types/application/vnd.ipld.raw
// [application/vnd.ipld.car]: https://www.iana.org/assignments/media-types/application/vnd.ipld.car
// [application/vnd.ipfs.ipns-record]: https://www.iana.org/assignments/media-types/application/vnd.ipfs.ipns-record
package gateway
