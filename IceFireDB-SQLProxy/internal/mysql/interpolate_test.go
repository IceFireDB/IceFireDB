package mysql

import (
	"errors"
	"testing"
	"time"
)

func TestMaterialize(t *testing.T) {
	cases := []struct {
		name    string
		query   string
		args    []any
		want    string
		wantErr bool
	}{
		{
			name:  "string",
			query: "INSERT INTO t VALUES (?, ?)",
			args:  []any{"it's", 42},
			want:  `INSERT INTO t VALUES ('it\'s', 42)`,
		},
		{
			name:  "int types",
			query: "INSERT INTO t VALUES (?, ?, ?, ?, ?)",
			args:  []any{int8(-1), uint16(2), int32(-3), uint64(4), int64(5)},
			want:  "INSERT INTO t VALUES (-1, 2, -3, 4, 5)",
		},
		{
			name:  "floats and bool",
			query: "INSERT INTO t VALUES (?, ?, ?)",
			args:  []any{1.5, float32(0.25), true},
			want:  "INSERT INTO t VALUES (1.5, 0.25, 1)",
		},
		{
			name:  "nil",
			query: "INSERT INTO t VALUES (?)",
			args:  []any{nil},
			want:  "INSERT INTO t VALUES (NULL)",
		},
		{
			name:  "text bytes stay readable",
			query: "INSERT INTO t VALUES (?)",
			args:  []any{[]byte("2024-01-02 03:04:05")},
			want:  "INSERT INTO t VALUES ('2024-01-02 03:04:05')",
		},
		{
			name:  "binary bytes become hex",
			query: "INSERT INTO t VALUES (?)",
			args:  []any{[]byte{0x00, 0xff}},
			want:  "INSERT INTO t VALUES (X'00ff')",
		},
		{
			name:  "time",
			query: "INSERT INTO t VALUES (?)",
			args:  []any{time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
			want:  "INSERT INTO t VALUES ('2024-01-02 03:04:05')",
		},
		{
			name:  "escaped backslash and newline",
			query: "UPDATE t SET a = ?",
			args:  []any{"a\\b\nc"},
			want:  `UPDATE t SET a = 'a\\b\nc'`,
		},
		{
			name:  "placeholder inside literal is untouched",
			query: "INSERT INTO t VALUES ('?', ?)",
			args:  []any{7},
			want:  "INSERT INTO t VALUES ('?', 7)",
		},
		{
			name:  "placeholder inside comment is untouched",
			query: "INSERT /* ? */ INTO t VALUES (?)",
			args:  []any{7},
			want:  "INSERT /* ? */ INTO t VALUES (7)",
		},
		{
			name:  "placeholder inside backtick identifier untouched",
			query: "INSERT INTO `a?b` VALUES (?)",
			args:  []any{7},
			want:  "INSERT INTO `a?b` VALUES (7)",
		},
		{
			name:    "too few args",
			query:   "INSERT INTO t VALUES (?, ?)",
			args:    []any{1},
			wantErr: true,
		},
		{
			name:    "too many args",
			query:   "INSERT INTO t VALUES (?)",
			args:    []any{1, 2},
			wantErr: true,
		},
		{
			name:    "unsupported type",
			query:   "INSERT INTO t VALUES (?)",
			args:    []any{struct{}{}},
			wantErr: true,
		},
		{
			name:    "nan",
			query:   "INSERT INTO t VALUES (?)",
			args:    []any{mustNaN()},
			wantErr: true,
		},
	}

	for _, c := range cases {
		got, err := materialize(c.query, c.args)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: materialize(%q) = %q, want error", c.name, c.query, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: materialize(%q) = %q, want %q", c.name, c.query, got, c.want)
		}
	}
}

func TestMaterializeCountMismatchIsIdentifiable(t *testing.T) {
	_, err := materialize("INSERT INTO t VALUES (?)", nil)
	if !errors.Is(err, errArgCountMismatch) {
		t.Fatalf("got %v, want errArgCountMismatch", err)
	}
}

func mustNaN() float64 {
	var zero float64
	return zero / zero
}
