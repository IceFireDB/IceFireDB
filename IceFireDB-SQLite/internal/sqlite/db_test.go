package sqlite

import (
	"testing"
)

func TestGetTable(t *testing.T) {
	sqlList := [][]string{
		{"select * from a", "a"},
		{"select * FROM a", "a"},
		{"select * FROM a where 1=1", "a"},
		{"select version()", ""},
	}
	for _, sql := range sqlList {
		table := getTableName(sql[0])
		if table != sql[1] {
			t.Errorf("table name error, sql: %s, getTableName: %s, name: %s", sql[0], table, sql[1])
		}
	}
}

func TestGetColumnTypeAndLen(t *testing.T) {
	list := [][]string{
		{"VARCHAR(10)"},
		{"CHAR(10)"},
		{"INT(10)"},
		{"REAL"},
	}
	for _, ct := range list {
		t.Log(getColumnTypeAndLen(ct[0]))
	}
}
