package sqlite

import (
	"context"
	"strconv"
	"testing"
)

func TestMySQLProtocolCompatibility(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping MySQL protocol compatibility test in short mode")
	}
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 测试MySQL协议兼容性
	testCases := []struct {
		name     string
		sql      string
		expected bool
	}{
		{"Basic SELECT", "SELECT 1", true},
		{"SELECT with alias", "SELECT 1 AS one", true},
		{"SELECT with WHERE", "SELECT * FROM sqlite_master WHERE type='table'", true},
		{"INSERT statement", "INSERT INTO test (id, col) VALUES (1, 1)", true},
		{"UPDATE statement", "UPDATE test SET col=2 WHERE id=1", true},
		{"DELETE statement", "DELETE FROM test WHERE id=1", true},
		{"CREATE TABLE", "CREATE TABLE test2 (id INTEGER)", true},
		{"DROP TABLE", "DROP TABLE test2", true},
		{"BEGIN transaction", "BEGIN", true},
		{"COMMIT transaction", "COMMIT", true},
		{"ROLLBACK transaction", "ROLLBACK", false}, // 没有活动事务时会失败
	}

	// 创建测试表
	_, err := Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER, col INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Exec(tc.sql)

			if tc.expected && err != nil {
				t.Errorf("Expected success for %s, got error: %v", tc.sql, err)
			} else if !tc.expected && err == nil {
				t.Errorf("Expected error for %s, but got success", tc.sql)
			}

			// 验证结果结构
			if err == nil && result != nil {
				if result.Status != 2 && result.Status != 31 {
					t.Errorf("Unexpected result status: %d", result.Status)
				}
			}
		})
	}
}

func TestConnectionManagement(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping connection management test in short mode")
	}
	ctx := context.Background()

	// 测试多个连接
	const numConnections = 3
	databases := make([]interface{}, numConnections)

	for i := 0; i < numConnections; i++ {
		db := InitSQLite(ctx, ":memory:")
		databases[i] = db
		defer db.Close()

		// 验证连接可用
		err := db.Ping()
		if err != nil {
			t.Fatalf("Connection %d ping failed: %v", i, err)
		}

		// 每个连接执行独立操作
		tableName := "conn_test_" + strconv.Itoa(i)
		_, err = Exec("CREATE TABLE " + tableName + " (id INTEGER)")
		if err != nil {
			t.Fatalf("Failed to create table on connection %d: %v", i, err)
		}

		// 在同一个连接中验证表创建
		result, err := Exec("SELECT name FROM sqlite_master WHERE type='table' AND name='" + tableName + "'")
		if err != nil {
			t.Fatalf("Failed to query table on connection %d: %v", i, err)
		}

		// 应该只有对应的表存在
		if len(result.RowDatas) != 1 {
			t.Errorf("Connection %d: expected 1 table, got %d", i, len(result.RowDatas))
		}
	}
}

func TestPreparedStatements(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping prepared statements test in short mode")
	}
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 创建测试表
	_, err := Exec("CREATE TABLE prep_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create prepared statement test table: %v", err)
	}

	// 测试多次执行相同SQL（模拟预处理语句）
	const numExecutions = 5
	for i := 0; i < numExecutions; i++ {
		sql := "INSERT INTO prep_test (name, value) VALUES ('test' || '" + strconv.Itoa(i) + "', " + strconv.Itoa(i*10) + ")"
		result, err := Exec(sql)
		if err != nil {
			t.Fatalf("Failed to execute prepared statement %d: %v", i, err)
		}

		if result.InsertId == 0 {
			t.Errorf("Expected non-zero insert ID for execution %d", i)
		}
	}

	// 验证所有数据插入成功
	result, err := Exec("SELECT COUNT(*) FROM prep_test")
	if err != nil {
		t.Fatalf("Failed to count prepared statement results: %v", err)
	}

	if len(result.RowDatas) != 1 {
		t.Errorf("Expected 1 row count result, got %d", len(result.RowDatas))
	}

	// 验证插入的数据
	result, err = Exec("SELECT * FROM prep_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query prepared statement results: %v", err)
	}

	if len(result.RowDatas) != numExecutions {
		t.Errorf("Expected %d rows, got %d", numExecutions, len(result.RowDatas))
	}
}

func TestProtocolErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping protocol error handling test in short mode")
	}
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 测试各种协议错误场景
	errorCases := []struct {
		name string
		sql  string
	}{
		{"Syntax error", "SELECT FROM"},
		{"Invalid table", "SELECT * FROM non_existent_table"},
		{"Invalid column", "SELECT invalid_column FROM sqlite_master"},
		{"Malformed SQL", "THIS IS NOT SQL"},
		{"Unclosed string", "SELECT 'unclosed string"},
		{"Invalid function", "SELECT invalid_func()"},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Exec(tc.sql)
			if err == nil {
				t.Errorf("Expected error for %s, but got success", tc.sql)
			}

			// 验证错误信息包含有用的信息
			if err != nil {
				t.Logf("Error (expected): %v", err)
			}
		})
	}

	// 测试边界情况 - 跳过可能导致超时的测试
	boundaryCases := []struct {
		name string
		sql  string
	}{
		{"Very long query", "SELECT " + string(make([]byte, 1000))},
	}

	for _, tc := range boundaryCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Exec(tc.sql)
			// 这些情况可能成功或失败，但不应崩溃
			if err != nil {
				t.Logf("Boundary case error (may be expected): %v", err)
			}
		})
	}
}
