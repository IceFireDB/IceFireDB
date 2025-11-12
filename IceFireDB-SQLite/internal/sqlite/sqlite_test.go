package sqlite

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// 运行测试
	code := m.Run()

	// 清理测试数据库
	os.Remove("test.db")
	os.Exit(code)
}

func TestInitSQLite(t *testing.T) {
	ctx := context.Background()

	// 测试正常初始化
	db := InitSQLite(ctx, "test.db")
	if db == nil {
		t.Fatal("InitSQLite returned nil database")
	}
	defer db.Close()

	// 测试数据库连接是否可用
	err := db.Ping()
	if err != nil {
		t.Fatalf("Database ping failed: %v", err)
	}
}

func TestSQLiteIntegration(t *testing.T) {
	ctx := context.Background()
	db := InitSQLite(ctx, "test.db")
	defer db.Close()

	// 清理并重新创建表
	_, _ = Exec("DROP TABLE IF EXISTS test_table")

	// 测试表创建
	result, err := Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if result.Status != 2 {
		t.Errorf("Expected status 2, got %d", result.Status)
	}

	// 测试数据插入
	result, err = Exec("INSERT INTO test_table (name, age) VALUES ('test_user', 25)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	if result.InsertId < 1 {
		t.Errorf("Expected insert ID >= 1, got %d", result.InsertId)
	}
	if result.AffectedRows != 1 {
		t.Errorf("Expected affected rows 1, got %d", result.AffectedRows)
	}

	// 测试数据查询
	result, err = Exec("SELECT * FROM test_table")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	if result.Status != 31 {
		t.Errorf("Expected status 31 for query, got %d", result.Status)
	}
	if len(result.Fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(result.Fields))
	}
	if len(result.RowDatas) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.RowDatas))
	}

	// 测试数据更新
	result, err = Exec("UPDATE test_table SET age = 30 WHERE name = 'test_user'")
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}
	if result.AffectedRows != 1 {
		t.Errorf("Expected affected rows 1, got %d", result.AffectedRows)
	}

	// 测试数据删除
	result, err = Exec("DELETE FROM test_table WHERE name = 'test_user'")
	if err != nil {
		t.Fatalf("Failed to delete data: %v", err)
	}
	if result.AffectedRows != 1 {
		t.Errorf("Expected affected rows 1, got %d", result.AffectedRows)
	}
}

func TestTransactionOperations(t *testing.T) {
	ctx := context.Background()
	db := InitSQLite(ctx, "test.db")
	defer db.Close()

	// 清理并重新创建表
	_, _ = Exec("DROP TABLE IF EXISTS test_table")
	_, _ = Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

	// 测试事务开始
	result, err := Exec("BEGIN")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if result.Status != 2 {
		t.Errorf("Expected status 2, got %d", result.Status)
	}

	// 在事务中插入数据
	result, err = Exec("INSERT INTO test_table (name, age) VALUES ('transaction_user', 30)")
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// 测试事务提交
	result, err = Exec("COMMIT")
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	if result.Status != 2 {
		t.Errorf("Expected status 2, got %d", result.Status)
	}

	// 验证数据已提交
	result, err = Exec("SELECT * FROM test_table WHERE name = 'transaction_user'")
	if err != nil {
		t.Fatalf("Failed to query committed data: %v", err)
	}
	if len(result.RowDatas) != 1 {
		t.Errorf("Expected 1 row after commit, got %d", len(result.RowDatas))
	}
}

func TestDDLOperations(t *testing.T) {
	ctx := context.Background()
	db := InitSQLite(ctx, "test.db")
	defer db.Close()

	// 测试表创建
	_, err := Exec("CREATE TABLE ddl_test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 测试表修改（SQLite不支持ALTER TABLE ADD COLUMN，这里测试其他DDL）
	_, err = Exec("CREATE INDEX idx_ddl_test ON ddl_test(data)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 测试表删除
	_, err = Exec("DROP TABLE ddl_test")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

func TestConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	db := InitSQLite(ctx, "test.db")
	defer db.Close()

	// 创建测试表
	_, err := Exec("CREATE TABLE IF NOT EXISTS concurrent_test (id INTEGER PRIMARY KEY, counter INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 并发插入测试
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for range numGoroutines {
		go func() {
			_, err := Exec("INSERT INTO concurrent_test (counter) VALUES (1)")
			if err != nil {
				t.Errorf("Concurrent insert failed: %v", err)
			}
			done <- true
		}()
	}

	// 等待所有goroutine完成
	for range numGoroutines {
		<-done
	}

	// 验证插入的数据
	result, err := Exec("SELECT COUNT(*) FROM concurrent_test")
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if len(result.RowDatas) != 1 {
		t.Errorf("Expected 1 row count result, got %d", len(result.RowDatas))
	}
}

func TestErrorHandling(t *testing.T) {
	ctx := context.Background()
	db := InitSQLite(ctx, "test.db")
	defer db.Close()

	// 测试无效SQL
	_, err := Exec("INVALID SQL STATEMENT")
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}

	// 测试查询不存在的表
	_, err = Exec("SELECT * FROM non_existent_table")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	// 测试语法错误
	_, err = Exec("SELECT FROM")
	if err == nil {
		t.Error("Expected error for syntax error")
	}
}

func TestPerformanceBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	db := InitSQLite(ctx, "test.db")
	defer db.Close()

	// 创建性能测试表
	_, err := Exec("CREATE TABLE IF NOT EXISTS perf_test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create performance test table: %v", err)
	}

	// 批量插入性能测试
	start := time.Now()
	const batchSize = 100

	for range batchSize {
		_, err := Exec("INSERT INTO perf_test (data) VALUES ('test data')")
		if err != nil {
			t.Fatalf("Batch insert failed: %v", err)
		}
	}

	duration := time.Since(start)
	t.Logf("Inserted %d records in %v", batchSize, duration)

	// 批量查询性能测试
	start = time.Now()
	result, err := Exec("SELECT * FROM perf_test")
	if err != nil {
		t.Fatalf("Batch query failed: %v", err)
	}

	queryDuration := time.Since(start)
	t.Logf("Queried %d records in %v", len(result.RowDatas), queryDuration)

	if len(result.RowDatas) != batchSize {
		t.Errorf("Expected %d records, got %d", batchSize, len(result.RowDatas))
	}
}
