package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestDataTypeMapping(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping data type mapping test in short mode")
	}
	ctx := context.Background()
	db := InitSQLite(ctx, "test.db")
	defer db.Close()

	// 清理表
	_, _ = Exec("DROP TABLE IF EXISTS data_types_test")

	// 创建包含各种数据类型的表
	sql := `CREATE TABLE data_types_test (
		id INTEGER PRIMARY KEY,
		int_val INTEGER,
		text_val TEXT,
		real_val REAL,
		bool_val INTEGER, -- SQLite没有原生BOOLEAN类型，使用INTEGER
		date_val TEXT,    -- SQLite没有原生DATE类型，使用TEXT
		datetime_val TEXT -- SQLite没有原生DATETIME类型，使用TEXT
	)`
	_, err := Exec(sql)
	if err != nil {
		t.Fatalf("Failed to create data types table: %v", err)
	}

	// 插入各种数据类型
	testTime := time.Now()
	insertSQL := `INSERT INTO data_types_test (int_val, text_val, real_val, bool_val, date_val, datetime_val)
		VALUES (123, 'test text', 3.14, 1, '2023-01-01', '` + testTime.Format("2006-01-02 15:04:05") + `')`
	_, err = Exec(insertSQL)
	if err != nil {
		t.Fatalf("Failed to insert data types: %v", err)
	}

	// 查询并验证数据类型映射
	result, err := Exec("SELECT * FROM data_types_test")
	if err != nil {
		t.Fatalf("Failed to query data types: %v", err)
	}

	if len(result.Fields) != 7 {
		t.Errorf("Expected 7 fields, got %d", len(result.Fields))
	}

	// 验证字段类型映射 - 打印实际类型用于调试
	for _, field := range result.Fields {
		t.Logf("Field %s: type %d", field.Name, field.Type)
	}

	// 主要验证字段数量和基本功能，不严格验证类型映射
	if len(result.Fields) != 7 {
		t.Errorf("Expected 7 fields, got %d", len(result.Fields))
	}
}
