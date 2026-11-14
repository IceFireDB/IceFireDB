package mysql

import (
	"net"
	"testing"
	"time"
)

func TestNormalizeRole(t *testing.T) {
	cases := map[string]string{
		"":           roleAdmin,
		"admin":      roleAdmin,
		"ADMIN":      roleAdmin,
		"readonly":   roleReadonly,
		"ReadOnly":   roleReadonly,
		" readonly ": roleReadonly,
		"superuser":  roleAdmin,
	}
	for in, want := range cases {
		if got := normalizeRole(in); got != want {
			t.Errorf("normalizeRole(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRoleOfDefaultsToAdmin(t *testing.T) {
	m := &mysqlProxy{tenants: map[string]string{
		"owner":  roleAdmin,
		"reader": roleReadonly,
	}}
	if got := m.roleOf("owner"); got != roleAdmin {
		t.Errorf("roleOf(owner) = %q, want %q", got, roleAdmin)
	}
	if got := m.roleOf("reader"); got != roleReadonly {
		t.Errorf("roleOf(reader) = %q, want %q", got, roleReadonly)
	}
	// A tenant cannot exist without an entry, but the fallback must never be readonly
	// by accident: unknown users are anonymous and were rejected at the handshake.
	if got := m.roleOf("stranger"); got != roleAdmin {
		t.Errorf("roleOf(stranger) = %q, want %q", got, roleAdmin)
	}
}

func TestHasReadonlyTenant(t *testing.T) {
	onlyAdmin := &mysqlProxy{tenants: map[string]string{"a": roleAdmin}}
	if onlyAdmin.hasReadonlyTenant() {
		t.Error("hasReadonlyTenant() = true for an admin-only userlist")
	}
	withReader := &mysqlProxy{tenants: map[string]string{"a": roleAdmin, "b": roleReadonly}}
	if !withReader.hasReadonlyTenant() {
		t.Error("hasReadonlyTenant() = false with a read-only tenant")
	}
}

// TestDeadlineConnAppliesIdleTimeout is the slow-loris guard: a client that connects and
// then goes quiet must be dropped instead of holding a session forever.
func TestDeadlineConnAppliesIdleTimeout(t *testing.T) {
	c, peer := net.Pipe()
	defer peer.Close()

	dc := &deadlineConn{Conn: c, readTimeout: 20 * time.Millisecond}

	start := time.Now()
	if _, err := dc.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read without a peer write should fail on the deadline")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("deadline was not applied: Read blocked for %s", elapsed)
	}
}

func TestSeconds(t *testing.T) {
	if got := seconds(0); got != 0 {
		t.Errorf("seconds(0) = %s, want 0", got)
	}
	if got := seconds(-1); got != 0 {
		t.Errorf("seconds(-1) = %s, want 0", got)
	}
	if got := seconds(2); got != 2*time.Second {
		t.Errorf("seconds(2) = %s, want 2s", got)
	}
}
