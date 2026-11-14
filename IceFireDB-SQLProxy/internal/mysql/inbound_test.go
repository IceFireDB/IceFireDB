package mysql

import (
	"strings"
	"testing"
)

const testToken = "s3cr3t"

func signedPayload(t *testing.T, token, role string, statements ...string) []byte {
	t.Helper()
	msg := newReplicationMessage(messageKindSQL, role, statements)
	if token != "" {
		msg.sign(token)
	}
	payload, err := msg.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return payload
}

func newTestPolicy(token string, peers []string) *inboundPolicy {
	return newInboundPolicy(roleAdmin, token, peers, newDedupCache(16))
}

func TestInboundAcceptsSignedWrite(t *testing.T) {
	p := newTestPolicy(testToken, nil)
	payload := signedPayload(t, testToken, roleAdmin, "INSERT INTO t VALUES (1)")

	msg, reason, err := p.accept("peer-a", payload)
	if err != nil {
		t.Fatalf("accept: %v (reason %s)", err, reason)
	}
	if len(msg.Statements) != 1 {
		t.Fatalf("statements = %v", msg.Statements)
	}
}

func TestInboundRejectsUnsignedOrForeignSignature(t *testing.T) {
	unsigned := signedPayload(t, "", roleAdmin, "INSERT INTO t VALUES (1)")
	foreign := signedPayload(t, "other-token", roleAdmin, "INSERT INTO t VALUES (1)")
	tampered := []byte(strings.Replace(string(signedPayload(t, testToken, roleAdmin, "INSERT INTO t VALUES (1)")),
		"VALUES (1)", "VALUES (2)", 1))

	for name, payload := range map[string][]byte{
		"unsigned": unsigned,
		"foreign":  foreign,
		"tampered": tampered,
	} {
		p := newTestPolicy(testToken, nil)
		if _, reason, err := p.accept("peer-a", payload); err == nil || reason != rejectSignature {
			t.Errorf("%s: reason=%q err=%v, want %q", name, reason, err, rejectSignature)
		}
	}
}

func TestInboundRejectsUnsignedLegacyWhenTokenConfigured(t *testing.T) {
	p := newTestPolicy(testToken, nil)
	if _, reason, err := p.accept("peer-a", []byte("INSERT INTO t VALUES (1)")); err == nil || reason != rejectSignature {
		t.Fatalf("reason=%q err=%v, want %q", reason, err, rejectSignature)
	}

	open := newTestPolicy("", nil)
	if _, _, err := open.accept("peer-a", []byte("INSERT INTO t VALUES (1)")); err != nil {
		t.Fatalf("legacy payload refused without a token: %v", err)
	}
}

func TestInboundRejectsUnlistedPeer(t *testing.T) {
	p := newTestPolicy("", []string{"peer-a"})
	payload := signedPayload(t, "", roleAdmin, "INSERT INTO t VALUES (1)")

	if _, _, err := p.accept("peer-a", payload); err != nil {
		t.Fatalf("listed peer refused: %v", err)
	}
	if _, reason, err := p.accept("peer-b", payload); err == nil || reason != rejectPeer {
		t.Fatalf("reason=%q err=%v, want %q", reason, err, rejectPeer)
	}
}

func TestInboundRejectsOversizedPayload(t *testing.T) {
	p := newTestPolicy("", nil)
	huge := make([]byte, maxReplicationMessageBytes+1)
	for i := range huge {
		huge[i] = 'a'
	}
	if _, reason, err := p.accept("peer-a", huge); err == nil || reason != rejectOversized {
		t.Fatalf("reason=%q err=%v, want %q", reason, err, rejectOversized)
	}

	statements := make([]string, maxReplicationStatements+1)
	for i := range statements {
		statements[i] = "INSERT INTO t VALUES (1)"
	}
	if _, reason, err := p.accept("peer-a", signedPayload(t, "", roleAdmin, statements...)); err == nil || reason != rejectOversized {
		t.Fatalf("reason=%q err=%v, want %q", reason, err, rejectOversized)
	}
}

func TestInboundRejectsNonWriteStatements(t *testing.T) {
	p := newTestPolicy("", nil)
	for _, statement := range []string{
		"SELECT 1",
		"SET NAMES utf8mb4",
		"KILL 1",
		"/* c */ SHOW TABLES",
		"not sql at all",
	} {
		payload := signedPayload(t, "", roleAdmin, statement)
		if _, reason, err := p.accept("peer-a", payload); err == nil || reason != rejectIllegal {
			t.Errorf("%q: reason=%q err=%v, want %q", statement, reason, err, rejectIllegal)
		}
	}
}

func TestInboundRejectsTransactionWithNonWriteStatement(t *testing.T) {
	p := newTestPolicy("", nil)
	msg := newReplicationMessage(messageKindTxn, roleAdmin, []string{"INSERT INTO t VALUES (1)", "SELECT 1"})
	payload, err := msg.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, reason, err := p.accept("peer-a", payload); err == nil || reason != rejectIllegal {
		t.Fatalf("reason=%q err=%v, want %q", reason, err, rejectIllegal)
	}
}

func TestInboundRejectsRoleMismatch(t *testing.T) {
	p := newTestPolicy("", nil)
	payload := signedPayload(t, "", roleReadonly, "INSERT INTO t VALUES (1)")
	if _, reason, err := p.accept("peer-a", payload); err == nil || reason != rejectReplicationRole {
		t.Fatalf("reason=%q err=%v, want %q", reason, err, rejectReplicationRole)
	}
}

func TestInboundDropsDuplicateMessage(t *testing.T) {
	p := newTestPolicy("", nil)
	payload := signedPayload(t, "", roleAdmin, "INSERT INTO t VALUES (1)")

	if _, _, err := p.accept("peer-a", payload); err != nil {
		t.Fatalf("first delivery refused: %v", err)
	}
	if _, reason, err := p.accept("peer-a", payload); err == nil || reason != rejectDuplicate {
		t.Fatalf("reason=%q err=%v, want %q", reason, err, rejectDuplicate)
	}
	// A rejected payload must not consume its future ID.
	illegal := signedPayload(t, "", roleAdmin, "SELECT 1")
	if _, _, err := p.accept("peer-a", illegal); err == nil {
		t.Fatal("illegal statement accepted")
	}
	if _, _, err := p.accept("peer-a", signedPayload(t, "", roleAdmin, "INSERT INTO t VALUES (3)")); err != nil {
		t.Fatalf("fresh message refused: %v", err)
	}
}

func TestInboundRejectsMalformedEnvelope(t *testing.T) {
	p := newTestPolicy("", nil)
	for _, payload := range [][]byte{[]byte("{"), []byte(`{"v":99,"stmts":["INSERT INTO t VALUES (1)"]}`), []byte(`{"v":1,"stmts":[]}`)} {
		if _, reason, err := p.accept("peer-a", payload); err == nil || reason != rejectMalformed {
			t.Errorf("%q: reason=%q err=%v, want %q", payload, reason, err, rejectMalformed)
		}
	}
}

func TestDedupCacheEvictsOldest(t *testing.T) {
	c := newDedupCache(2)
	if !c.add("a") || !c.add("b") {
		t.Fatal("first insert should be new")
	}
	if c.add("a") {
		t.Fatal("duplicate reported as new")
	}
	if !c.add("c") { // evicts "a"
		t.Fatal("insert after eviction should be new")
	}
	if !c.add("a") {
		t.Fatal("evicted id should be accepted again")
	}
	if !c.add("") {
		// A message without an ID cannot be tracked, so it must never be dropped
		// as a duplicate.
		t.Fatal("empty id reported as duplicate")
	}
}
