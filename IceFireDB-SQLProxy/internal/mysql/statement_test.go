package mysql

import "testing"

func TestClassifyStatement(t *testing.T) {
	cases := []struct {
		sql  string
		want sqlKind
	}{
		{"SELECT 1", sqlKindRead},
		{"  \n\t select * from t", sqlKindRead},
		{"/* hint */ SELECT 1", sqlKindRead},
		{"-- comment\nSELECT 1", sqlKindRead},
		{"# comment\nSELECT 1", sqlKindRead},
		{"WITH x AS (SELECT 1) SELECT * FROM x", sqlKindRead},
		{"WITH x AS (SELECT 1), y AS (SELECT 2) SELECT * FROM x, y", sqlKindRead},
		{"WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT n FROM t", sqlKindRead},
		{"WITH x AS (SELECT 1) DELETE FROM t WHERE a IN (SELECT * FROM x)", sqlKindWrite},
		{"SHOW TABLES", sqlKindRead},
		{"DESC t", sqlKindRead},
		{"SET NAMES utf8mb4", sqlKindSession},
		{"SET autocommit = 0", sqlKindSession},
		{"USE db1", sqlKindSession},
		{"INSERT INTO t VALUES (1)", sqlKindWrite},
		{"update t set a = 1", sqlKindWrite},
		{"DELETE FROM t", sqlKindWrite},
		{"REPLACE INTO t VALUES (1)", sqlKindWrite},
		{"CREATE TABLE t (a int)", sqlKindWrite},
		{"/* ALERT was never a keyword */ ALTER TABLE t ADD COLUMN b int", sqlKindWrite},
		{"DROP TABLE t", sqlKindWrite},
		{"TRUNCATE t", sqlKindWrite},
		{"RENAME TABLE a TO b", sqlKindWrite},
		{"LOAD DATA INFILE 'x' INTO TABLE t", sqlKindWrite},
		{"BEGIN", sqlKindBegin},
		{"begin work", sqlKindBegin},
		{"BEGIN TRANSACTION", sqlKindBegin},
		{"START TRANSACTION", sqlKindBegin},
		{"START TRANSACTION READ WRITE", sqlKindBegin},
		{"START REPLICA", sqlKindAdmin},
		{"COMMIT", sqlKindCommit},
		{"commit work", sqlKindCommit},
		{"ROLLBACK", sqlKindRollback},
		{"ROLLBACK WORK", sqlKindRollback},
		{"ROLLBACK TO SAVEPOINT sp1", sqlKindAdmin},
		{"CALL p()", sqlKindAdmin},
		{"KILL 12", sqlKindAdmin},
		{"FLUSH PRIVILEGES", sqlKindAdmin},
		{"PRAGMA something", sqlKindUnknown},
		{"", sqlKindUnknown},
		{";;", sqlKindUnknown},
	}
	for _, c := range cases {
		if got := classifyStatement(c.sql); got != c.want {
			t.Errorf("classifyStatement(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestClassifySQLBatch(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		wantKind  sqlKind
		wantCount int
		wantWrite bool
	}{
		{"single read", "SELECT 1", sqlKindRead, 1, false},
		{"trailing semicolon", "SELECT 1;   ", sqlKindRead, 1, false},
		{"empty at start", "  ; SELECT 1", sqlKindRead, 1, false},
		{"read then write", "SELECT 1; INSERT INTO t VALUES (1)", sqlKindWrite, 2, true},
		{"write then read", "INSERT INTO t VALUES (1); SELECT 1", sqlKindWrite, 2, true},
		{"injected comment write", "/* x */ ; INSERT INTO t VALUES (1)", sqlKindWrite, 2, true},
		{"semicolon inside literal", "INSERT INTO t VALUES ('a;b')", sqlKindWrite, 1, true},
		{"semicolon inside comment", "SELECT 1 /* ; DROP TABLE t */", sqlKindRead, 1, false},
		{"semicolon inside backtick", "SELECT `a;b` FROM t", sqlKindRead, 1, false},
		{"escaped quote", "SELECT 'it\\'s; fine'", sqlKindRead, 1, false},
		{"doubled quote", "SELECT 'it''s; fine'", sqlKindRead, 1, false},
		{"begin only", "BEGIN", sqlKindBegin, 1, false},
		{"begin then insert", "BEGIN; INSERT INTO t VALUES (1)", sqlKindWrite, 2, true},
		{"empty", "", sqlKindUnknown, 0, false},
	}
	for _, c := range cases {
		got := classifySQL(c.sql)
		if got.Kind != c.wantKind || got.Count != c.wantCount || got.Write != c.wantWrite {
			t.Errorf("%s: classifySQL(%q) = {%v %d %v}, want {%v %d %v}",
				c.name, c.sql, got.Kind, got.Count, got.Write, c.wantKind, c.wantCount, c.wantWrite)
		}
	}
}

func TestReadOnlyAllowed(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"SELECT 1", true},
		{"SHOW TABLES", true},
		{"SET NAMES utf8mb4", true},
		{"BEGIN", true},
		{"COMMIT", true},
		{"INSERT INTO t VALUES (1)", false},
		{"/* c */ DELETE FROM t", false},
		{"  update t set a = 1", false},
		{"SELECT 1; DELETE FROM t", false},
		{"KILL 1", false},
		{"WEIRD COMMAND", false},
	}
	for _, c := range cases {
		if got := classifySQL(c.sql).readOnlyAllowed(); got != c.want {
			t.Errorf("readOnlyAllowed(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestReplicateSafe(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"INSERT INTO t VALUES (1)", true},
		{"ALTER TABLE t ADD COLUMN b int", true},
		{"SELECT 1", false},
		{"SET NAMES utf8mb4", false},
		{"KILL 1", false},
		{"INSERT INTO t VALUES (1); SELECT 1", true},
	}
	for _, c := range cases {
		if got := classifySQL(c.sql).replicateSafe(); got != c.want {
			t.Errorf("replicateSafe(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestUnterminatedBlockCommentIsNotRead(t *testing.T) {
	// An unterminated comment must never be mistaken for a safe read.
	if classifySQL("/* SELECT 1").Kind == sqlKindRead {
		t.Fatal("unterminated block comment classified as read")
	}
	if classifySQL("INSERT /* x").Kind != sqlKindWrite {
		t.Fatal("write followed by unterminated comment should stay a write")
	}
}
