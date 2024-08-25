# IPFS Gateway

> A reference implementation of HTTP Gateway Specifications.

## Documentation

- Go Documentation: https://pkg.go.dev/github.com/ipfs/boxo/gateway
- Gateway Specification: https://specs.ipfs.tech/
- Types of HTTP Gateways: https://docs.ipfs.tech/how-to/address-ipfs-on-web/#http-gateways

## Example

This example shows how you can start your own gateway, assuming you have an `IPFSBackend`
implementation.

```go
conf := gateway.Config{}

// Initialize an IPFSBackend interface for both an online and offline versions.
// The offline version should not make any network request for missing content.
ipfsBackend := ...

// Create http mux and setup path gateway handler.
mux := http.NewServeMux()
handler := gateway.NewHandler(conf, ipfsBackend)
handler = gateway.NewHeaders(nil).ApplyCors().Wrap(handler)
mux.Handle("/ipfs/", handler)
mux.Handle("/ipns/", handler)


// Start the server on :8080 and voilá! You have a basic IPFS gateway running
// in http://localhost:8080.
_ = http.ListenAndServe(":8080", mux)
```
