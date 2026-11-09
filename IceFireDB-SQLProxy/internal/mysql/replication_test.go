package mysql

import "testing"

// captureEmit records every message a Handle publishes.
func captureEmit() (*[]*replicationMessage, func(*replicationMessage)) {
	var got []*replicationMessage
	return &got, func(msg *replicationMessage) { got = append(got, msg) }
}

// run feeds statements through afterExecute the way the query path would.
func run(t *testing.T, h *Handle, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		cls := classifySQL(statement)
		if err := h.checkReadOnly(cls); err != nil {
			t.Fatalf("checkReadOnly(%q): %v", statement, err)
		}
		h.afterExecute(nil, cls, statement)
	}
}

func newTestHandle(role string) (*Handle, *[]*replicationMessage) {
	got, emit := captureEmit()
	return newHandle(nil, newSession(role), emit), got
}

func TestAutocommitWriteIsPublishedImmediately(t *testing.T) {
	h, got := newTestHandle(roleAdmin)
	run(t, h, "INSERT INTO t VALUES (1)")

	if len(*got) != 1 {
		t.Fatalf("published %d messages, want 1", len(*got))
	}
	msg := (*got)[0]
	if msg.Kind != messageKindSQL || len(msg.Statements) != 1 || msg.Statements[0] != "INSERT INTO t VALUES (1)" {
		t.Fatalf("unexpected message: %+v", msg)
	}
	if msg.Version != replicationProtocolVersion || msg.ID == "" {
		t.Fatalf("message envelope incomplete: %+v", msg)
	}
}

func TestTransactionIsPublishedAsOneBatch(t *testing.T) {
	h, got := newTestHandle(roleAdmin)
	run(t, h,
		"BEGIN",
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"SELECT 1",
		"COMMIT",
	)

	if len(*got) != 1 {
		t.Fatalf("published %d messages, want 1 batch", len(*got))
	}
	msg := (*got)[0]
	if msg.Kind != messageKindTxn {
		t.Fatalf("kind = %q, want %q", msg.Kind, messageKindTxn)
	}
	want := []string{"INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"}
	if len(msg.Statements) != len(want) {
		t.Fatalf("statements = %v, want %v", msg.Statements, want)
	}
	for i := range want {
		if msg.Statements[i] != want[i] {
			t.Fatalf("statements = %v, want %v", msg.Statements, want)
		}
	}
	if h.session.inTx {
		t.Fatal("session still in transaction after COMMIT")
	}
}

func TestRollbackDiscardsBatch(t *testing.T) {
	h, got := newTestHandle(roleAdmin)
	run(t, h, "BEGIN", "INSERT INTO t VALUES (1)", "ROLLBACK")

	if len(*got) != 0 {
		t.Fatalf("published %d messages after ROLLBACK, want 0", len(*got))
	}
	if h.session.inTx || h.session.buffer != nil {
		t.Fatal("session state not reset after ROLLBACK")
	}
}

func TestCommitWithoutWritesPublishesNothing(t *testing.T) {
	h, got := newTestHandle(roleAdmin)
	run(t, h, "BEGIN", "SELECT 1", "COMMIT")

	if len(*got) != 0 {
		t.Fatalf("published %d messages, want 0", len(*got))
	}
}

func TestCommitWithoutBeginIsNotAReplicationBatch(t *testing.T) {
	h, got := newTestHandle(roleAdmin)
	run(t, h, "INSERT INTO t VALUES (1)", "COMMIT")

	if len(*got) != 1 || (*got)[0].Kind != messageKindSQL {
		t.Fatalf("unexpected messages: %+v", *got)
	}
}

func TestBeginDiscardsUncommittedBuffer(t *testing.T) {
	h, got := newTestHandle(roleAdmin)
	run(t, h, "BEGIN", "INSERT INTO t VALUES (1)", "BEGIN", "INSERT INTO t VALUES (2)", "COMMIT")

	if len(*got) != 1 {
		t.Fatalf("published %d messages, want 1", len(*got))
	}
	if got := (*got)[0].Statements; len(got) != 1 || got[0] != "INSERT INTO t VALUES (2)" {
		t.Fatalf("statements = %v, want only the second transaction's write", got)
	}
}

func TestReadOnlySessionPublishesNothing(t *testing.T) {
	h, got := newTestHandle(roleReadonly)
	run(t, h, "SELECT 1", "SET NAMES utf8mb4", "BEGIN", "COMMIT")

	if len(*got) != 0 {
		t.Fatalf("published %d messages from a read-only session, want 0", len(*got))
	}
}

func TestReadOnlySessionRejectsWrites(t *testing.T) {
	h, _ := newTestHandle(roleReadonly)
	for _, statement := range []string{
		"INSERT INTO t VALUES (1)",
		"/* c */ DELETE FROM t",
		"ALTER TABLE t ADD COLUMN b int",
		"KILL 1",
		"UNKNOWN STATEMENT",
	} {
		if err := h.checkReadOnly(classifySQL(statement)); err == nil {
			t.Errorf("read-only session accepted %q", statement)
		}
	}
	for _, statement := range []string{"SELECT 1", "SHOW TABLES", "SET NAMES utf8mb4", "BEGIN", "COMMIT"} {
		if err := h.checkReadOnly(classifySQL(statement)); err != nil {
			t.Errorf("read-only session refused %q: %v", statement, err)
		}
	}
}

func TestCloseConnResetsSessionState(t *testing.T) {
	h, got := newTestHandle(roleAdmin)
	run(t, h, "BEGIN", "INSERT INTO t VALUES (1)")
	if !h.session.inTx {
		t.Fatal("expected an open transaction")
	}

	// conn is nil in this test; CloseConn would roll back on a real connection, so
	// only the session bookkeeping is exercised here.
	h.session.reset()
	if h.session.inTx || h.session.buffer != nil {
		t.Fatal("session not reset")
	}
	if len(*got) != 0 {
		t.Fatalf("published %d messages for an aborted transaction, want 0", len(*got))
	}
}

func TestDecodeReplicationMessage(t *testing.T) {
	msg := newReplicationMessage(messageKindTxn, roleAdmin, []string{"INSERT INTO t VALUES (1)"})
	payload, err := msg.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, legacy, err := decodeReplicationMessage(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if legacy {
		t.Fatal("envelope decoded as legacy payload")
	}
	if decoded.ID != msg.ID || decoded.Kind != msg.Kind || len(decoded.Statements) != 1 {
		t.Fatalf("decoded = %+v, want %+v", decoded, msg)
	}

	if _, legacy, err := decodeReplicationMessage([]byte("INSERT INTO t VALUES (1)")); err != nil || !legacy {
		t.Fatalf("bare SQL should decode as legacy: legacy=%v err=%v", legacy, err)
	}
	if _, _, err := decodeReplicationMessage([]byte("{")); err == nil {
		t.Fatal("malformed JSON accepted")
	}
	if _, _, err := decodeReplicationMessage(nil); err == nil {
		t.Fatal("empty payload accepted")
	}
	if _, _, err := decodeReplicationMessage([]byte(`{"v":99,"stmts":["INSERT INTO t VALUES (1)"]}`)); err == nil {
		t.Fatal("unknown protocol version accepted")
	}
	if _, _, err := decodeReplicationMessage([]byte(`{"v":1,"stmts":[]}`)); err == nil {
		t.Fatal("envelope without statements accepted")
	}
}
