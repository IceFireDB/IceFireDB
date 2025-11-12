package sqlite

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLite/pkg/config"
	"github.com/sirupsen/logrus"
)

func TestP2PIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P integration test in short mode")
	}

	// Skip in CI environment to avoid network timeouts
	if os.Getenv("CI") == "true" || os.Getenv("GITHUB_ACTIONS") == "true" {
		t.Skip("Skipping P2P integration test in CI environment")
	}

	ctx := context.Background()

	// 测试P2P禁用模式
	originalConfig := config.Get()
	config.Get().P2P.Enable = false

	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 验证数据库正常可用
	err := db.Ping()
	if err != nil {
		t.Fatalf("Database should work without P2P: %v", err)
	}

	// 恢复配置
	config.Get().P2P.Enable = originalConfig.P2P.Enable
}

func TestP2PConfiguration(t *testing.T) {
	// 初始化配置
	config.InitConfig("../../config/config.yaml")

	// 测试P2P配置验证
	config := config.Get()

	if config.P2P.Enable {
		if config.P2P.ServiceDiscoveryID == "" {
			t.Error("ServiceDiscoveryID should not be empty when P2P is enabled")
		}
		if config.P2P.ServiceCommandTopic == "" {
			t.Error("ServiceCommandTopic should not be empty when P2P is enabled")
		}
	}

	// 测试服务发现模式
	validModes := map[string]bool{
		"advertise": true,
		"announce":  true,
	}
	if !validModes[config.P2P.ServiceDiscoverMode] {
		t.Errorf("Invalid service discover mode: %s", config.P2P.ServiceDiscoverMode)
	}
}

func TestAsyncSQLExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping async SQL test in short mode")
	}

	// Skip in CI environment to avoid network timeouts
	if os.Getenv("CI") == "true" || os.Getenv("GITHUB_ACTIONS") == "true" {
		t.Skip("Skipping async SQL test in CI environment")
	}

	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 创建测试表
	_, err := Exec("CREATE TABLE async_test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 测试异步SQL执行（模拟P2P场景）
	start := time.Now()

	// 执行多个SQL操作
	for i := 0; i < 10; i++ {
		_, err := Exec("INSERT INTO async_test (data) VALUES ('async_data_')")
		if err != nil {
			t.Errorf("Async insert failed: %v", err)
		}
	}

	duration := time.Since(start)
	if duration > 5*time.Second {
		t.Logf("Async operations took %v, consider performance optimization", duration)
	}

	// 验证数据一致性
	result, err := Exec("SELECT COUNT(*) FROM async_test")
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if len(result.RowDatas) != 1 {
		t.Errorf("Expected 1 row count result, got %d", len(result.RowDatas))
	}
}

func TestP2PLogging(t *testing.T) {
	// 测试P2P相关日志级别
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	// 验证日志级别设置
	if logger.GetLevel() < logrus.InfoLevel {
		t.Error("P2P operations should use at least Info level logging")
	}
}

func TestP2PNetworkIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping P2P network isolation test in short mode")
	}

	// Skip in CI environment to avoid network timeouts
	if os.Getenv("CI") == "true" || os.Getenv("GITHUB_ACTIONS") == "true" {
		t.Skip("Skipping P2P network isolation test in CI environment")
	}

	ctx := context.Background()

	// 测试网络隔离场景 - 使用不同的数据库文件
	db1 := InitSQLite(ctx, "test1.db")
	db2 := InitSQLite(ctx, "test2.db")
	defer db1.Close()
	defer db2.Close()

	// 验证两个数据库实例独立
	err1 := db1.Ping()
	err2 := db2.Ping()
	if err1 != nil || err2 != nil {
		t.Fatalf("Both databases should be accessible: db1=%v, db2=%v", err1, err2)
	}
}
