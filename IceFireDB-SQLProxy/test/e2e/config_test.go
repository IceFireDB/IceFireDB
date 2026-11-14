//go:build sqlproxe2e

package e2e

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/config"
)

// TestNodeConfigRendersAndParses keeps the suite's configuration template honest without
// needing Docker: a node that fails to parse its config never starts, and the failure then
// looks like a replication bug.
func TestNodeConfigRendersAndParses(t *testing.T) {
	const peerAddr = "/ip4/10.0.0.5/tcp/4001/p2p/12D3KooWExamplePeerID"

	rendered := renderConfig(nodeSpec{
		name:   "a",
		schema: schemaA,
		peers:  []string{peerAddr},
	}, 33306, 17878, "127.0.0.1:3306")

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(rendered), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := config.InitConfig(path); err != nil {
		t.Fatalf("config does not parse: %v\n%s", err, rendered)
	}

	got := config.Get()
	if got.Server.Addr != "127.0.0.1:33306" {
		t.Errorf("server.addr = %q", got.Server.Addr)
	}
	if got.Server.IdleTimeout != 60 || got.Server.WriteTimeout != 60 {
		t.Errorf("timeouts = %d/%d, want 60/60", got.Server.IdleTimeout, got.Server.WriteTimeout)
	}
	if !got.Debug.Enable || got.Debug.Port != 17878 {
		t.Errorf("debug = %v/%d", got.Debug.Enable, got.Debug.Port)
	}
	if got.Mysql.Admin.Addr != "127.0.0.1:3306" || got.Mysql.Admin.DBName != schemaA {
		t.Errorf("admin mysql = %q/%q", got.Mysql.Admin.Addr, got.Mysql.Admin.DBName)
	}
	if got.Mysql.Readonly.User != readerUser || got.Mysql.Readonly.DBName != schemaA {
		t.Errorf("readonly mysql = %q/%q", got.Mysql.Readonly.User, got.Mysql.Readonly.DBName)
	}
	if len(got.UserList) != 2 {
		t.Fatalf("userlist has %d entries, want 2", len(got.UserList))
	}
	if got.UserList[1].User != readonlyTenant || got.UserList[1].Role != "readonly" {
		t.Errorf("readonly tenant = %+v", got.UserList[1])
	}
	if got.P2P.AuthToken != p2pToken {
		t.Errorf("p2p.authToken = %q", got.P2P.AuthToken)
	}
	if len(got.P2P.BootstrapPeers) != 1 || got.P2P.BootstrapPeers[0] != peerAddr {
		t.Errorf("p2p.bootstrapPeers = %v", got.P2P.BootstrapPeers)
	}
}
