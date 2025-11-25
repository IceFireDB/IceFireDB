package sqlite

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLite/pkg/mysql/mysql"
)

func TestGetTable(t *testing.T) {
	sqlList := [][]string{
		{"select * from a", "a"},
		{"select * FROM a", "a"},
		{"select * FROM a where 1=1", "a"},
		{"select version()", ""},
		{"SELECT * FROM users WHERE id = 1", "users"},
		{"select count(*) from orders", "orders"},
		{"FROM products select *", "products"}, // This actually extracts "products"
		{"", ""},                               // Empty SQL
	}
	for _, sql := range sqlList {
		table := getTableName(sql[0])
		if table != sql[1] {
			t.Errorf("table name error, sql: %s, getTableName: %s, expected: %s", sql[0], table, sql[1])
		}
	}
}

func TestGetColumnTypeAndLen(t *testing.T) {
	testCases := []struct {
		columnType   string
		expectedType byte
		expectedLen  uint32
	}{
		{"VARCHAR(10)", mysql.MYSQL_TYPE_VARCHAR, 10},
		{"CHAR(10)", mysql.MYSQL_TYPE_VARCHAR, 10},
		{"INT(10)", mysql.MYSQL_TYPE_INT24, 10},
		{"REAL", mysql.MYSQL_TYPE_DOUBLE, 0},
		{"FLOAT", mysql.MYSQL_TYPE_FLOAT, 0},
		{"DATE", mysql.MYSQL_TYPE_DATE, 0},
		{"DATETIME", mysql.MYSQL_TYPE_DATETIME, 0},
		{"TIMESTAMP", mysql.MYSQL_TYPE_TIMESTAMP, 0},
		{"DECIMAL", mysql.MYSQL_TYPE_DECIMAL, 0},
		{"BOOLEAN", mysql.MYSQL_TYPE_BIT, 0},
		{"NULL", mysql.MYSQL_TYPE_NULL, 0},
		{"TEXT", mysql.MYSQL_TYPE_BLOB, 0},
		{"UNKNOWN_TYPE", mysql.MYSQL_TYPE_VAR_STRING, 0},
	}

	for _, tc := range testCases {
		actualType, actualLen := getColumnTypeAndLen(tc.columnType)
		if actualType != tc.expectedType || actualLen != tc.expectedLen {
			t.Errorf("getColumnTypeAndLen(%s) = (%d, %d), expected (%d, %d)",
				tc.columnType, actualType, actualLen, tc.expectedType, tc.expectedLen)
		}
	}
}

func TestIsDML(t *testing.T) {
	testCases := []struct {
		sql      string
		expected bool
	}{
		{"BEGIN", true},
		{"BEGIN TRANSACTION", true},
		{"COMMIT", true},
		{"END TRANSACTION", true},
		{"ROLLBACK", true},
		{"INSERT INTO users VALUES (1)", true},
		{"UPDATE users SET name='test'", true},
		{"DELETE FROM users", true},
		{"CREATE TABLE test (id INT)", true},
		{"ALERT TABLE users ADD COLUMN email VARCHAR(100)", true},
		{"DROP TABLE users", true},
		{"SELECT * FROM users", false},
		{"select version()", false},
		{"", false},
	}

	for _, tc := range testCases {
		result := isDML(tc.sql)
		if result != tc.expected {
			t.Errorf("isDML(%s) = %v, expected %v", tc.sql, result, tc.expected)
		}
	}
}

func TestSQLiteIntegration(t *testing.T) {
	// Create a temporary database file
	tmpFile := "/tmp/test_sqlite_" + time.Now().Format("20060102150405") + ".db"
	defer os.Remove(tmpFile)

	// Initialize SQLite database
	ctx := context.Background()
	db := InitSQLite(ctx, tmpFile)
	defer db.Close()

	// Test table creation
	createSQL := "CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))"
	result, err := Exec(createSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if result.Status != 2 {
		t.Errorf("Expected status 2 for DML, got %d", result.Status)
	}

	// Test INSERT
	insertSQL := "INSERT INTO test_users (name, email) VALUES ('test_user', 'test@example.com')"
	result, err = Exec(insertSQL)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	if result.InsertId != 1 {
		t.Errorf("Expected InsertId 1, got %d", result.InsertId)
	}

	if result.AffectedRows != 1 {
		t.Errorf("Expected AffectedRows 1, got %d", result.AffectedRows)
	}

	// Test SELECT
	selectSQL := "SELECT * FROM test_users"
	result, err = Exec(selectSQL)
	if err != nil {
		t.Fatalf("Failed to select data: %v", err)
	}

	if result.Status != 31 {
		t.Errorf("Expected status 31 for SELECT, got %d", result.Status)
	}

	if len(result.Fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(result.Fields))
	}

	if result.AffectedRows != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.AffectedRows)
	}

	// Verify field information
	expectedFields := []string{"id", "name", "email"}
	for i, field := range result.Fields {
		if string(field.Name) != expectedFields[i] {
			t.Errorf("Field %d: expected %s, got %s", i, expectedFields[i], string(field.Name))
		}
	}

	// Test UPDATE
	updateSQL := "UPDATE test_users SET name = 'updated_user' WHERE id = 1"
	result, err = Exec(updateSQL)
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	if result.AffectedRows != 1 {
		t.Errorf("Expected 1 row affected for UPDATE, got %d", result.AffectedRows)
	}

	// Test DELETE
	deleteSQL := "DELETE FROM test_users WHERE id = 1"
	result, err = Exec(deleteSQL)
	if err != nil {
		t.Fatalf("Failed to delete data: %v", err)
	}

	if result.AffectedRows != 1 {
		t.Errorf("Expected 1 row affected for DELETE, got %d", result.AffectedRows)
	}
}

func TestTransactionOperations(t *testing.T) {
	tmpFile := "/tmp/test_transactions_" + time.Now().Format("20060102150405") + ".db"
	defer os.Remove(tmpFile)

	ctx := context.Background()
	db := InitSQLite(ctx, tmpFile)
	defer db.Close()

	// Create test table
	_, err := Exec("CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, amount INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test BEGIN TRANSACTION
	result, err := Exec("BEGIN TRANSACTION")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if result.Status != 2 {
		t.Errorf("Expected status 2 for BEGIN TRANSACTION, got %d", result.Status)
	}

	// Test INSERT within transaction
	_, err = Exec("INSERT INTO transactions (amount) VALUES (100)")
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Test COMMIT
	result, err = Exec("COMMIT")
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if result.Status != 2 {
		t.Errorf("Expected status 2 for COMMIT, got %d", result.Status)
	}

	// Verify data was committed
	result, err = Exec("SELECT * FROM transactions")
	if err != nil {
		t.Fatalf("Failed to select committed data: %v", err)
	}

	if result.AffectedRows != 1 {
		t.Errorf("Expected 1 row after commit, got %d", result.AffectedRows)
	}
}

func TestErrorHandling(t *testing.T) {
	tmpFile := "/tmp/test_errors_" + time.Now().Format("20060102150405") + ".db"
	defer os.Remove(tmpFile)

	ctx := context.Background()
	db := InitSQLite(ctx, tmpFile)
	defer db.Close()

	// Test invalid SQL
	_, err := Exec("INVALID SQL STATEMENT")
	if err == nil {
		t.Error("Expected error for invalid SQL, but got none")
	}

	// Test SELECT from non-existent table
	_, err = Exec("SELECT * FROM non_existent_table")
	if err == nil {
		t.Error("Expected error for non-existent table, but got none")
	}

	// Test malformed SQL
	_, err = Exec("SELECT FROM")
	if err == nil {
		t.Error("Expected error for malformed SQL, but got none")
	}
}
