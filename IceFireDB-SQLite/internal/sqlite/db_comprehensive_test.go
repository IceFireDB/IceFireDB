package sqlite

import (
	"context"
	"os"
	"testing"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLite/pkg/mysql/mysql"
	_ "github.com/mattn/go-sqlite3"
)

func TestMain(m *testing.M) {
	// Setup test database
	testDBPath := "/tmp/test_sqlite.db"
	os.Remove(testDBPath)

	// Initialize SQLite database
	db = InitSQLite(context.Background(), testDBPath)

	// Run tests
	code := m.Run()

	// Cleanup
	os.Remove(testDBPath)
	os.Exit(code)
}

func TestSQLiteInit(t *testing.T) {
	if db == nil {
		t.Fatal("Database not initialized")
	}

	// Test basic connection
	err := db.Ping()
	if err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}
}

func TestExecDDLCommands(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "Create table",
			sql:     "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))",
			wantErr: false,
		},
		{
			name:    "Create table with constraints",
			sql:     "CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, price REAL)",
			wantErr: false,
		},
		{
			name:    "Create index",
			sql:     "CREATE INDEX idx_users_name ON users(name)",
			wantErr: false,
		},
		{
			name:    "Drop table",
			sql:     "DROP TABLE IF EXISTS temp_table",
			wantErr: false,
		},
		{
			name:    "Invalid SQL",
			sql:     "CREATE TABLE",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Exec(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == nil {
				t.Error("Exec() returned nil result for valid query")
			}
		})
	}
}

func TestExecDMLCommands(t *testing.T) {
	// Setup: Create test table
	_, err := Exec("CREATE TABLE IF NOT EXISTS test_dml (id INTEGER PRIMARY KEY, name VARCHAR(50), value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	tests := []struct {
		name           string
		sql            string
		wantErr        bool
		expectedRows   int64
		expectedInsert int64
	}{
		{
			name:           "Insert single row",
			sql:            "INSERT INTO test_dml (name, value) VALUES ('test1', 100)",
			wantErr:        false,
			expectedRows:   1,
			expectedInsert: 1,
		},
		{
			name:           "Insert multiple rows",
			sql:            "INSERT INTO test_dml (name, value) VALUES ('test2', 200), ('test3', 300)",
			wantErr:        false,
			expectedRows:   2,
			expectedInsert: 3, // Last insert ID should be 3
		},
		{
			name:           "Update rows",
			sql:            "UPDATE test_dml SET value = 999 WHERE name = 'test1'",
			wantErr:        false,
			expectedRows:   1,
			expectedInsert: 3, // SQLite returns last insert ID even for UPDATE
		},
		{
			name:           "Delete rows",
			sql:            "DELETE FROM test_dml WHERE name = 'test2'",
			wantErr:        false,
			expectedRows:   1,
			expectedInsert: 3, // SQLite returns last insert ID even for DELETE
		},
		{
			name:    "Invalid insert",
			sql:     "INSERT INTO test_dml (invalid_column) VALUES (1)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Exec(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != nil {
				if result.AffectedRows != uint64(tt.expectedRows) {
					t.Errorf("Exec() AffectedRows = %v, want %v", result.AffectedRows, tt.expectedRows)
				}
				if result.InsertId != uint64(tt.expectedInsert) {
					t.Errorf("Exec() InsertId = %v, want %v", result.InsertId, tt.expectedInsert)
				}
			}
		})
	}

	// Cleanup
	Exec("DROP TABLE test_dml")
}

func TestQueryCommands(t *testing.T) {
	// Setup: Create and populate test table
	_, err := Exec("CREATE TABLE IF NOT EXISTS test_query (id INTEGER PRIMARY KEY, name VARCHAR(50), age INTEGER, salary REAL)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO test_query (name, age, salary) VALUES ('Alice', 30, 50000.0)",
		"INSERT INTO test_query (name, age, salary) VALUES ('Bob', 25, 45000.5)",
		"INSERT INTO test_query (name, age, salary) VALUES ('Charlie', 35, 60000.75)",
	}

	for _, sql := range testData {
		_, err := Exec(sql)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	tests := []struct {
		name         string
		sql          string
		wantErr      bool
		expectedRows int
		expectedCols int
	}{
		{
			name:         "Select all columns",
			sql:          "SELECT * FROM test_query",
			wantErr:      false,
			expectedRows: 3,
			expectedCols: 4,
		},
		{
			name:         "Select specific columns",
			sql:          "SELECT name, age FROM test_query",
			wantErr:      false,
			expectedRows: 3,
			expectedCols: 2,
		},
		{
			name:         "Select with WHERE clause",
			sql:          "SELECT * FROM test_query WHERE age > 30",
			wantErr:      false,
			expectedRows: 1,
			expectedCols: 4,
		},
		{
			name:         "Select with ORDER BY",
			sql:          "SELECT * FROM test_query ORDER BY age DESC",
			wantErr:      false,
			expectedRows: 3,
			expectedCols: 4,
		},
		{
			name:         "Select with aggregate function",
			sql:          "SELECT COUNT(*) FROM test_query",
			wantErr:      false,
			expectedRows: 1,
			expectedCols: 1,
		},
		{
			name:    "Invalid query",
			sql:     "SELECT * FROM non_existent_table",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Exec(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != nil {
				if len(result.Fields) != tt.expectedCols {
					t.Errorf("Exec() Fields count = %v, want %v", len(result.Fields), tt.expectedCols)
				}
				if int(result.AffectedRows) != tt.expectedRows {
					t.Errorf("Exec() AffectedRows = %v, want %v", result.AffectedRows, tt.expectedRows)
				}

				// Verify field metadata
				for i, field := range result.Fields {
					if field.Name == nil {
						t.Errorf("Field %d has nil name", i)
					}
					if field.Type == 0 {
						t.Errorf("Field %d has zero type", i)
					}
				}
			}
		})
	}

	// Cleanup
	Exec("DROP TABLE test_query")
}

func TestTransactionCommands(t *testing.T) {
	// Setup: Create test table
	_, err := Exec("CREATE TABLE IF NOT EXISTS test_tx (id INTEGER PRIMARY KEY, balance INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert initial data
	_, err = Exec("INSERT INTO test_tx (balance) VALUES (1000)")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "Begin transaction",
			sql:     "BEGIN",
			wantErr: false,
		},
		{
			name:    "Commit transaction",
			sql:     "COMMIT",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Exec(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == nil {
				t.Error("Exec() returned nil result for valid transaction command")
			}
		})
	}

	// Test complete transaction sequence
	_, err = Exec("BEGIN")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = Exec("UPDATE test_tx SET balance = balance - 100 WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update in transaction: %v", err)
	}

	_, err = Exec("COMMIT")
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify the update was committed
	result, err := Exec("SELECT balance FROM test_tx WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to verify transaction: %v", err)
	}

	if result.AffectedRows != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.AffectedRows)
	}

	// Cleanup
	Exec("DROP TABLE test_tx")
}

func TestGetTableNameComprehensive(t *testing.T) {
	tests := []struct {
		sql      string
		expected string
	}{
		{"select * from users", "users"},
		{"select * FROM users", "users"},
		{"select * FROM users where id = 1", "users"},
		{"select name, email from users", "users"},
		{"SELECT * FROM products", "products"},
		{"select version()", ""},
		{"select * from", ""},
		{"", ""},
		{"from users", "users"},
		{"select * from schema.table", "schema.table"},
		{"select * from users u", "users"},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			result := getTableName(tt.sql)
			if result != tt.expected {
				t.Errorf("getTableName(%q) = %q, want %q", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestGetColumnTypeAndLenComprehensive(t *testing.T) {
	tests := []struct {
		columnType   string
		expectedType byte
		expectedLen  uint32
	}{
		{"INT", mysql.MYSQL_TYPE_INT24, 0},
		{"INT(10)", mysql.MYSQL_TYPE_INT24, 10},
		{"VARCHAR(255)", mysql.MYSQL_TYPE_VARCHAR, 255},
		{"CHAR(10)", mysql.MYSQL_TYPE_VARCHAR, 10},
		{"TEXT", mysql.MYSQL_TYPE_BLOB, 0},
		{"REAL", mysql.MYSQL_TYPE_DOUBLE, 0},
		{"DOUBLE", mysql.MYSQL_TYPE_DOUBLE, 0},
		{"FLOAT", mysql.MYSQL_TYPE_FLOAT, 0},
		{"DATE", mysql.MYSQL_TYPE_DATE, 0},
		{"DATETIME", mysql.MYSQL_TYPE_DATETIME, 0},
		{"TIMESTAMP", mysql.MYSQL_TYPE_TIMESTAMP, 0},
		{"DECIMAL(10,2)", mysql.MYSQL_TYPE_DECIMAL, 0},
		{"NUMERIC(5,1)", mysql.MYSQL_TYPE_DECIMAL, 0},
		{"BOOLEAN", mysql.MYSQL_TYPE_BIT, 0},
		{"NULL", mysql.MYSQL_TYPE_NULL, 0},
		{"UNKNOWN_TYPE", mysql.MYSQL_TYPE_VAR_STRING, 0},
	}

	for _, tt := range tests {
		t.Run(tt.columnType, func(t *testing.T) {
			resultType, resultLen := getColumnTypeAndLen(tt.columnType)
			if resultType != tt.expectedType {
				t.Errorf("getColumnTypeAndLen(%q) type = %v, want %v", tt.columnType, resultType, tt.expectedType)
			}
			if resultLen != tt.expectedLen {
				t.Errorf("getColumnTypeAndLen(%q) length = %v, want %v", tt.columnType, resultLen, tt.expectedLen)
			}
		})
	}
}

func TestIsDML(t *testing.T) {
	tests := []struct {
		sql      string
		expected bool
	}{
		{"INSERT INTO users VALUES (1)", true},
		{"UPDATE users SET name = 'test'", true},
		{"DELETE FROM users", true},
		{"CREATE TABLE users (id INT)", true},
		{"DROP TABLE users", true},
		{"BEGIN", true},
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"SELECT * FROM users", false},
		{"select name from users", false},
		{"SHOW TABLES", false},
		{"DESCRIBE users", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			result := isDML(tt.sql)
			if result != tt.expected {
				t.Errorf("isDML(%q) = %v, want %v", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestConcurrentAccess(t *testing.T) {
	// Setup: Create test table
	_, err := Exec("CREATE TABLE IF NOT EXISTS test_concurrent (id INTEGER PRIMARY KEY, counter INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert initial data
	_, err = Exec("INSERT INTO test_concurrent (counter) VALUES (0)")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Test concurrent reads
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			result, err := Exec("SELECT counter FROM test_concurrent WHERE id = 1")
			if err != nil {
				t.Errorf("Concurrent read %d failed: %v", id, err)
			}
			if result == nil || result.AffectedRows != 1 {
				t.Errorf("Concurrent read %d returned unexpected result", id)
			}
			done <- true
		}(i)
	}

	// Wait for all reads to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Cleanup
	Exec("DROP TABLE test_concurrent")
}

func TestDataTypes(t *testing.T) {
	// Test various SQLite data types
	_, err := Exec(`CREATE TABLE IF NOT EXISTS test_types (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100),
		age INTEGER,
		salary REAL,
		active INTEGER,  -- Use INTEGER instead of BOOLEAN for SQLite compatibility
		created_at DATETIME
	)`)
	if err != nil {
		t.Fatalf("Failed to create types test table: %v", err)
	}

	// Insert data with different types
	_, err = Exec(`INSERT INTO test_types (name, age, salary, active, created_at) 
		VALUES ('John Doe', 30, 50000.50, 1, '2023-01-01 10:00:00')`)
	if err != nil {
		t.Fatalf("Failed to insert type test data: %v", err)
	}

	// Query and verify data types
	result, err := Exec("SELECT * FROM test_types")
	if err != nil {
		t.Fatalf("Failed to query type test data: %v", err)
	}

	if result == nil || len(result.Fields) != 6 {
		t.Errorf("Expected 6 fields, got %d", len(result.Fields))
	}

	// Cleanup
	Exec("DROP TABLE test_types")
}
