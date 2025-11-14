package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLite/internal/sqlite"
)

func TestNewMysqlProxy(t *testing.T) {
	proxy := NewMysqlProxy()
	if proxy == nil {
		t.Fatal("NewMysqlProxy returned nil")
	}
}

func TestMysqlProxyHandleQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping MySQL proxy handle query test in short mode")
	}
	// 初始化SQLite数据库
	ctx := context.Background()
	sqlite.InitSQLite(ctx, ":memory:")

	proxy := NewMysqlProxy()

	// 测试SET命令
	result, err := proxy.HandleQuery(nil, "SET autocommit=1")
	if err != nil {
		t.Fatalf("HandleQuery failed for SET command: %v", err)
	}
	if result.Status != 2 {
		t.Errorf("Expected status 2 for SET command, got %d", result.Status)
	}

	// 测试VERSION()查询
	result, err = proxy.HandleQuery(nil, "SELECT VERSION()")
	if err != nil {
		t.Fatalf("HandleQuery failed for VERSION(): %v", err)
	}
	if result.Status != 31 {
		t.Errorf("Expected status 31 for SELECT, got %d", result.Status)
	}

	// 测试NOW()查询
	result, err = proxy.HandleQuery(nil, "SELECT NOW()")
	if err != nil {
		t.Fatalf("HandleQuery failed for NOW(): %v", err)
	}
	if result.Status != 31 {
		t.Errorf("Expected status 31 for SELECT, got %d", result.Status)
	}
}

func TestMysqlProxyUseDB(t *testing.T) {
	proxy := NewMysqlProxy()

	// 测试UseDB方法（应该总是返回nil）
	err := proxy.UseDB(nil, "test_db")
	if err != nil {
		t.Errorf("UseDB should return nil, got: %v", err)
	}
}

func TestMysqlProxyCloseConn(t *testing.T) {
	proxy := NewMysqlProxy()

	// 测试CloseConn方法 - 传递nil连接应该安全处理
	err := proxy.CloseConn(nil)
	if err != nil {
		t.Errorf("CloseConn should return nil for nil connection, got: %v", err)
	}
}

func TestMysqlProxyFieldList(t *testing.T) {
	proxy := NewMysqlProxy()

	// 测试FieldList方法（应该返回nil）
	fields, err := proxy.HandleFieldList(nil, "test_table", "*")
	if err != nil {
		t.Errorf("HandleFieldList should return nil error, got: %v", err)
	}
	if fields != nil {
		t.Errorf("HandleFieldList should return nil fields, got: %v", fields)
	}
}

func TestMysqlProxyStmtOperations(t *testing.T) {
	proxy := NewMysqlProxy()

	// 测试Prepare语句
	paramCount, columns, context, err := proxy.HandleStmtPrepare(nil, "SELECT 1")
	if err != nil {
		t.Errorf("HandleStmtPrepare should return nil error, got: %v", err)
	}
	if paramCount != 0 {
		t.Errorf("Expected paramCount 0, got %d", paramCount)
	}
	if columns != 0 {
		t.Errorf("Expected columns 0, got %d", columns)
	}
	if context != nil {
		t.Errorf("Expected context nil, got %v", context)
	}

	// 测试StmtClose
	err = proxy.HandleStmtClose(nil, nil)
	if err != nil {
		t.Errorf("HandleStmtClose should return nil error, got: %v", err)
	}
}

func TestMysqlProxyOtherCommand(t *testing.T) {
	proxy := NewMysqlProxy()

	// 测试其他命令（应该返回错误）
	err := proxy.HandleOtherCommand(nil, 0x00, []byte{})
	if err == nil {
		t.Error("HandleOtherCommand should return error for unsupported commands")
	}
}

func TestMysqlProxyIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := NewMysqlProxy()

	// 测试代理运行（应该正常启动和停止）
	go func() {
		proxy.Run(ctx)
	}()

	// 给一点时间让代理启动
	time.Sleep(100 * time.Millisecond)

	// 取消上下文来停止代理
	cancel()
}
