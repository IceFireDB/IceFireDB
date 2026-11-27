package mysql

import (
	"context"
	"strings"
	"testing"
)

// TestPopConnWithoutPoolFails is the regression guard for the old behaviour where a
// failed popConn returned (nil, nil) and the caller dereferenced the nil connection.
func TestPopConnWithoutPoolFails(t *testing.T) {
	m := &mysqlProxy{ctx: context.Background()}

	conn, err := m.popConn(nil)
	if err == nil {
		t.Fatal("popConn(nil pool) returned no error")
	}
	if conn != nil {
		t.Fatal("popConn(nil pool) returned a connection")
	}
	if !strings.Contains(err.Error(), "6") && !strings.Contains(err.Error(), "pool") {
		t.Fatalf("error should explain the cause: %v", err)
	}
}

// TestPushConnToleratesNil guards the deferred release path: a connection that was
// never acquired must not panic on the way out.
func TestPushConnToleratesNil(t *testing.T) {
	m := &mysqlProxy{ctx: context.Background()}
	m.pushConn(nil, nil, nil)
	m.pushConn(nil, errReadOnlyStatement, nil)
}
