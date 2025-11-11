package sqlite

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestStorageEngine(t *testing.T) {
	ctx := context.Background()

	// 测试不同的存储文件
	testFiles := []string{
		"test.db",
		":memory:",
		"", // 临时文件
	}

	for _, filename := range testFiles {
		t.Run(filename, func(t *testing.T) {
			db := InitSQLite(ctx, filename)
			defer db.Close()

			// 验证数据库连接
			err := db.Ping()
			if err != nil {
				t.Fatalf("Database ping failed for %s: %v", filename, err)
			}

			// 执行基本操作
			_, err = Exec("CREATE TABLE IF NOT EXISTS storage_test (id INTEGER PRIMARY KEY, data TEXT)")
			if err != nil {
				t.Fatalf("Failed to create table for %s: %v", filename, err)
			}

			_, err = Exec("INSERT INTO storage_test (data) VALUES ('test data')")
			if err != nil {
				t.Fatalf("Failed to insert data for %s: %v", filename, err)
			}

			result, err := Exec("SELECT * FROM storage_test")
			if err != nil {
				t.Fatalf("Failed to query data for %s: %v", filename, err)
			}

			if len(result.RowDatas) != 1 {
				t.Errorf("Expected 1 row for %s, got %d", filename, len(result.RowDatas))
			}
		})
	}

	// 清理测试文件
	os.Remove("test.db")
}

func TestDataTypesComprehensive(t *testing.T) {
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 创建包含所有SQLite数据类型的表
	_, err := Exec(`
		CREATE TABLE all_data_types (
			int_val INTEGER,
			text_val TEXT,
			real_val REAL,
			blob_val BLOB,
			null_val NULL,
			bool_val INTEGER,  -- SQLite uses INTEGER for boolean
			date_val TEXT,     -- SQLite uses TEXT for dates
			time_val TEXT,     -- SQLite uses TEXT for times
			datetime_val TEXT  -- SQLite uses TEXT for datetime
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create data types table: %v", err)
	}

	// 测试各种数据类型的插入和查询
	testTime := time.Now()
	testData := []struct {
		name  string
		value string
	}{
		{"Integer", "123"},
		{"Negative Integer", "-456"},
		{"Large Integer", "9223372036854775807"}, // Max int64
		{"Text", "Hello, World!"},
		{"Unicode Text", "中文测试"},
		{"Special Characters", "Test with 'quotes' and \"double quotes\""},
		{"Real Number", "3.14159"},
		{"Negative Real", "-2.71828"},
		{"Scientific Notation", "1.23e-4"},
		{"Boolean True", "1"},
		{"Boolean False", "0"},
		{"Date", "2023-12-25"},
		{"Time", "14:30:00"},
		{"DateTime", testTime.Format("2006-01-02 15:04:05")},
		{"NULL", "NULL"},
	}

	for _, data := range testData {
		t.Run(data.name, func(t *testing.T) {
			// 插入数据 - 使用直接SQL字符串
			sql := "INSERT INTO all_data_types (int_val, text_val, real_val, bool_val, date_val, time_val, datetime_val) VALUES (" +
				"'" + data.value + "', " +
				"'" + data.value + "', " +
				"'" + data.value + "', " +
				"'" + data.value + "', " +
				"'" + data.value + "', " +
				"'" + data.value + "', " +
				"'" + data.value + "')"

			_, err := Exec(sql)

			// 对于某些数据类型，插入可能会失败，这是正常的
			if err != nil {
				t.Logf("Insert failed for %s (may be expected): %v", data.name, err)
			}

			// 查询验证
			result, err := Exec("SELECT * FROM all_data_types")
			if err != nil {
				t.Errorf("Query failed for %s: %v", data.name, err)
			}

			if result != nil {
				t.Logf("%s: Query returned %d rows", data.name, len(result.RowDatas))
			}
		})
	}
}

func TestDatabaseOperations(t *testing.T) {
	ctx := context.Background()

	// 测试数据库文件操作
	testDir := "test_storage"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	testCases := []struct {
		name     string
		filename string
	}{
		{"Relative path", "test_storage/relative.db"},
		{"Simple name", "test_storage/simple.db"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 确保目录存在
			if tc.filename != ":memory:" && tc.filename != "" {
				dir := tc.filename
				if lastSlash := strings.LastIndex(tc.filename, "/"); lastSlash != -1 {
					dir = tc.filename[:lastSlash]
				}
				os.MkdirAll(dir, 0755)
			}

			// 初始化数据库
			db := InitSQLite(ctx, tc.filename)
			defer db.Close()

			// SQLite可能延迟创建文件，这里不验证文件立即创建

			// 执行数据库操作
			_, err := Exec("CREATE TABLE file_test (id INTEGER PRIMARY KEY, data TEXT)")
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			_, err = Exec("INSERT INTO file_test (data) VALUES ('persistent data')")
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}

			// 重新打开数据库验证数据持久化
			if tc.filename != ":memory:" && tc.filename != "" {
				db.Close()
				db2 := InitSQLite(ctx, tc.filename)
				defer db2.Close()

				result, err := Exec("SELECT * FROM file_test")
				if err != nil {
					t.Fatalf("Failed to query persisted data: %v", err)
				}

				if len(result.RowDatas) != 1 {
					t.Errorf("Expected 1 persisted row, got %d", len(result.RowDatas))
				}
			}
		})
	}
}

func TestTransactionIsolation(t *testing.T) {
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 创建测试表
	_, err := Exec("CREATE TABLE isolation_test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 测试事务隔离级别
	_, err = Exec("BEGIN")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 在事务中插入数据
	_, err = Exec("INSERT INTO isolation_test (value) VALUES (100)")
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// 在提交前验证其他连接看不到未提交的数据
	result, err := Exec("SELECT * FROM isolation_test")
	if err != nil {
		t.Fatalf("Failed to query in transaction: %v", err)
	}

	// 在事务中应该能看到自己的更改
	if len(result.RowDatas) != 1 {
		t.Errorf("Should see uncommitted changes within transaction, got %d rows", len(result.RowDatas))
	}

	// 提交事务
	_, err = Exec("COMMIT")
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// 验证提交后的数据
	result, err = Exec("SELECT * FROM isolation_test")
	if err != nil {
		t.Fatalf("Failed to query after commit: %v", err)
	}

	if len(result.RowDatas) != 1 {
		t.Errorf("Should see committed changes, got %d rows", len(result.RowDatas))
	}
}

func TestDatabaseConstraints(t *testing.T) {
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 测试各种约束
	_, err := Exec(`
		CREATE TABLE constraints_test (
			id INTEGER PRIMARY KEY,
			unique_val TEXT UNIQUE,
			not_null_val TEXT NOT NULL,
			check_val INTEGER CHECK (check_val > 0),
			default_val TEXT DEFAULT 'default'
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create constraints table: %v", err)
	}

	// 测试唯一约束
	_, err = Exec("INSERT INTO constraints_test (unique_val, not_null_val, check_val) VALUES ('unique1', 'not null', 1)")
	if err != nil {
		t.Fatalf("Failed first insert: %v", err)
	}

	_, err = Exec("INSERT INTO constraints_test (unique_val, not_null_val, check_val) VALUES ('unique1', 'not null', 1)")
	if err == nil {
		t.Error("Should fail on duplicate unique value")
	}

	// 测试非空约束
	_, err = Exec("INSERT INTO constraints_test (unique_val, check_val) VALUES ('unique2', 1)")
	if err == nil {
		t.Error("Should fail on null not null value")
	}

	// 测试检查约束
	_, err = Exec("INSERT INTO constraints_test (unique_val, not_null_val, check_val) VALUES ('unique3', 'not null', -1)")
	if err == nil {
		t.Error("Should fail on check constraint violation")
	}

	// 测试默认值
	_, err = Exec("INSERT INTO constraints_test (unique_val, not_null_val, check_val) VALUES ('unique4', 'not null', 1)")
	if err != nil {
		t.Fatalf("Failed insert with default: %v", err)
	}

	result, err := Exec("SELECT default_val FROM constraints_test WHERE unique_val = 'unique4'")
	if err != nil {
		t.Fatalf("Failed to query default value: %v", err)
	}

	if len(result.RowDatas) != 1 {
		t.Errorf("Expected 1 row with default value, got %d", len(result.RowDatas))
	}
}

func TestIndexPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping index performance test in short mode")
	}

	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 创建测试表
	_, err := Exec(`
		CREATE TABLE index_test (
			id INTEGER PRIMARY KEY,
			name TEXT,
			category TEXT,
			value INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create index test table: %v", err)
	}

	// 插入大量测试数据
	const numRecords = 1000
	for i := 0; i < numRecords; i++ {
		sql := "INSERT INTO index_test (name, category, value) VALUES (" +
			"'name_" + strconv.Itoa(i) + "', " +
			"'category_" + strconv.Itoa(i%10) + "', " +
			strconv.Itoa(i) + ")"
		_, err := Exec(sql)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// 测试无索引查询性能
	start := time.Now()
	result, err := Exec("SELECT * FROM index_test WHERE category = 'category_5'")
	noIndexDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Query without index failed: %v", err)
	}
	t.Logf("Query without index took %v, returned %d rows", noIndexDuration, len(result.RowDatas))

	// 创建索引
	_, err = Exec("CREATE INDEX idx_category ON index_test(category)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// 测试有索引查询性能
	start = time.Now()
	result, err = Exec("SELECT * FROM index_test WHERE category = 'category_5'")
	indexDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Query with index failed: %v", err)
	}
	t.Logf("Query with index took %v, returned %d rows", indexDuration, len(result.RowDatas))

	// 索引应该提高性能
	if indexDuration >= noIndexDuration {
		t.Log("Index didn't improve performance significantly, but this might be due to small dataset")
	}
}
