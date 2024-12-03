package client

import (
	"reflect"
	"runtime/debug"

	"github.com/caddyserver/certmagic"
)

const (
	DefaultForgeDomain    = "libp2p.direct"
	DefaultForgeEndpoint  = "https://registration.libp2p.direct"
	DefaultCAEndpoint     = certmagic.LetsEncryptProductionCA
	DefaultCATestEndpoint = certmagic.LetsEncryptStagingCA

	// ForgeAuthEnv is optional environment variable that defines optional
	// secret that limits access to registration endpoint
	ForgeAuthEnv = "FORGE_ACCESS_TOKEN"

	// ForgeAuthHeader optional HTTP header that client should include when
	// talking to a limited access registration endpoint
	ForgeAuthHeader = "Forge-Authorization"
)

// defaultUserAgent is used as a fallback to inform HTTP server which library
// version sent a request
var defaultUserAgent = moduleVersion()

// importPath is the canonical import path that allows us to identify
// official client builds vs modified forks, and use that info in User-Agent header.
var importPath = getImportPath()

// getImportPath returns the path that library consumers would have in go.mod
func getImportPath() string {
	return reflect.ValueOf(P2PForgeCertMgr{}).Type().PkgPath()
}

// moduleVersion returns a useful user agent version string allowing us to
// identify requests coming from official releases of this module vs forks.
func moduleVersion() (ua string) {
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
