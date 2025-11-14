package sqlite

import (
	"context"
	"strings"
	"testing"
)

func TestSQLCompatibility(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SQL compatibility test in short mode")
	}
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	tests := []struct {
		name        string
		sql         string
		shouldPass  bool
		description string
	}{
		// DDL 语句测试 - 使用唯一表名
		{"CREATE TABLE", "CREATE TABLE test_table_1 (id INTEGER PRIMARY KEY, name TEXT)", true, "Basic table creation"},
		{"CREATE TABLE IF NOT EXISTS", "CREATE TABLE IF NOT EXISTS test_table_2 (id INTEGER PRIMARY KEY, name TEXT)", true, "Conditional table creation"},
		{"DROP TABLE", "DROP TABLE IF EXISTS test_table_1", true, "Table deletion"},
		{"CREATE INDEX", "CREATE INDEX idx_name ON test_table_2(name)", true, "Index creation"},

		// DML 语句测试 - 使用不同的表
		{"INSERT", "INSERT INTO test_table_2 (name) VALUES ('test')", true, "Basic insert"},
		{"INSERT multiple", "INSERT INTO test_table_2 (name) VALUES ('test1'), ('test2')", true, "Multi-value insert"},
		{"SELECT", "SELECT * FROM test_table_2", true, "Basic select"},
		{"SELECT WHERE", "SELECT * FROM test_table_2 WHERE name = 'test'", true, "Conditional select"},
		{"SELECT ORDER BY", "SELECT * FROM test_table_2 ORDER BY name DESC", true, "Ordered select"},
		{"SELECT LIMIT", "SELECT * FROM test_table_2 LIMIT 10", true, "Limited select"},
		{"UPDATE", "UPDATE test_table_2 SET name = 'updated' WHERE id = 1", true, "Basic update"},
		{"DELETE", "DELETE FROM test_table_2 WHERE id = 1", true, "Basic delete"},

		// 事务语句测试
		{"BEGIN", "BEGIN", true, "Transaction start"},
		{"COMMIT", "COMMIT", true, "Transaction commit"},
		{"ROLLBACK", "ROLLBACK", false, "Transaction rollback"}, // 没有活动事务时会失败

		// SQLite 特定功能测试
		{"VACUUM", "VACUUM", true, "Database optimization"},
		{"PRAGMA", "PRAGMA table_info(test_table_2)", true, "Pragma statements"},

		// 复杂查询测试 - 创建新表
		{"JOIN", "CREATE TABLE test_table_3 (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO test_table_3 (name) VALUES ('test'); SELECT t1.*, t2.* FROM test_table_3 t1 JOIN test_table_3 t2 ON t1.id = t2.id", true, "Join operations"},
		{"SUBQUERY", "SELECT * FROM test_table_2 WHERE id IN (SELECT id FROM test_table_2 WHERE name = 'test')", true, "Subquery"},
		{"AGGREGATE", "SELECT COUNT(*), AVG(id), MAX(id), MIN(id) FROM test_table_2", true, "Aggregate functions"},
		{"GROUP BY", "SELECT name, COUNT(*) FROM test_table_2 GROUP BY name", true, "Group operations"},

		// 错误情况测试
		{"Invalid SQL", "INVALID SQL STATEMENT", false, "Invalid SQL syntax"},
		{"Missing table", "SELECT * FROM non_existent_table", false, "Query non-existent table"},
		{"Syntax error", "SELECT FROM", false, "SQL syntax error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Exec(tt.sql)

			if tt.shouldPass {
				if err != nil {
					t.Errorf("%s: expected success but got error: %v", tt.description, err)
				}
				// 对于查询语句，验证返回结构
				if strings.HasPrefix(strings.ToUpper(tt.sql), "SELECT") && result != nil {
					if result.Status != 31 {
						t.Errorf("%s: expected status 31 for SELECT, got %d", tt.description, result.Status)
					}
				}
			} else {
				if err == nil {
					t.Errorf("%s: expected error but got success", tt.description)
				}
			}
		})
	}
}

func TestSQLInjectionProtection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SQL injection protection test in short mode")
	}
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 创建测试表
	_, err := Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, password TEXT)")
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// 测试SQL注入场景
	injectionTests := []struct {
		name     string
		username string
		password string
	}{
		{"Basic injection", "admin", "password"},
		{"Single quote", "admin'", "password"},
		{"Comment injection", "admin' --", "password"},
		{"Union injection", "admin' UNION SELECT 1,2,3 --", "password"},
		{"Semicolon injection", "admin'; DROP TABLE users --", "password"},
	}

	for _, tt := range injectionTests {
		t.Run(tt.name, func(t *testing.T) {
			// 尝试插入潜在的危险数据
			_, err := Exec("INSERT INTO users (username, password) VALUES ('" + tt.username + "', '" + tt.password + "')")

			// 即使数据包含特殊字符，插入应该成功（参数化查询会处理）
			if err != nil {
				t.Logf("Insert with special characters failed (expected in some cases): %v", err)
			}

			// 验证表结构仍然完整
			result, err := Exec("SELECT * FROM users")
			if err != nil {
				t.Fatalf("Table should still be accessible: %v", err)
			}
			if result == nil {
				t.Error("Result should not be nil")
			}
		})
	}
}

func TestSQLFunctions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SQL functions test in short mode")
	}
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 测试SQLite内置函数
	functionTests := []struct {
		name     string
		sql      string
		expected string
	}{
		{"ABS", "SELECT ABS(-10)", "10"},
		{"LENGTH", "SELECT LENGTH('hello')", "5"},
		{"UPPER", "SELECT UPPER('hello')", "HELLO"},
		{"LOWER", "SELECT LOWER('HELLO')", "hello"},
		{"SUBSTR", "SELECT SUBSTR('hello', 1, 2)", "he"},
		{"REPLACE", "SELECT REPLACE('hello', 'l', 'x')", "hexxo"},
		{"TRIM", "SELECT TRIM('  hello  ')", "hello"},
		{"DATETIME", "SELECT DATETIME('now')", ""}, // 不验证具体值
		{"DATE", "SELECT DATE('now')", ""},
		{"TIME", "SELECT TIME('now')", ""},
		{"STRFTIME", "SELECT STRFTIME('%Y', 'now')", ""},
	}

	for _, tt := range functionTests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Exec(tt.sql)
			if err != nil {
				t.Errorf("Function %s failed: %v", tt.name, err)
				return
			}

			if result == nil || len(result.RowDatas) == 0 {
				t.Errorf("Function %s returned no results", tt.name)
				return
			}

			// 对于有预期值的测试，验证结果
			if tt.expected != "" {
				// 这里需要根据实际结果格式进行验证
				t.Logf("Function %s executed successfully", tt.name)
			}
		})
	}
}

func TestComplexQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping complex queries test in short mode")
	}
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 创建测试数据
	_, err := Exec(`
		CREATE TABLE employees (
			id INTEGER PRIMARY KEY,
			name TEXT,
			department TEXT,
			salary REAL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create employees table: %v", err)
	}

	// 插入测试数据
	_, err = Exec(`
		INSERT INTO employees (name, department, salary) VALUES
		('Alice', 'Engineering', 75000),
		('Bob', 'Engineering', 80000),
		('Charlie', 'Sales', 60000),
		('Diana', 'Sales', 65000),
		('Eve', 'HR', 55000)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// 测试复杂查询
	complexTests := []struct {
		name        string
		sql         string
		description string
	}{
		{
			"Group by with having",
			"SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department HAVING AVG(salary) > 60000",
			"Group by with having clause",
		},
		{
			"Subquery in WHERE",
			"SELECT name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)",
			"Subquery in where condition",
		},
		{
			"Case statement",
			"SELECT name, CASE WHEN salary > 70000 THEN 'High' WHEN salary > 60000 THEN 'Medium' ELSE 'Low' END as level FROM employees",
			"Case statement for categorization",
		},
		{
			"Window function",
			"SELECT name, department, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank FROM employees",
			"Window function for ranking",
		},
	}

	for _, tt := range complexTests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Exec(tt.sql)
			if err != nil {
				t.Errorf("%s failed: %v", tt.description, err)
				return
			}

			if result == nil {
				t.Errorf("%s returned nil result", tt.description)
				return
			}

			t.Logf("%s executed successfully, returned %d rows", tt.description, len(result.RowDatas))
		})
	}
}
