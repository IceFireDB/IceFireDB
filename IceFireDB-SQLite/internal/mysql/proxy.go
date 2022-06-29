package mysql

import (
	"context"
	"database/sql"
	"errors"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/mysql/server"
	"net"
	"runtime"
	"sync"

	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

type mysqlProxy struct {
	ctx        context.Context
	server     *server.Server
	credential server.CredentialProvider
	pLock      sync.RWMutex
	closed     atomic.Value
	db         *sql.DB
}

// 代理连接
func (m *mysqlProxy) onConn(c net.Conn) {
	// 接收客户端数据包，解析包内容获取账号密码数据库
	conn, err := server.NewClientConn(c, m.server, m.credential, m)
	if err != nil {
		return
	}
	defer func() {
		err := recover()
		if err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] // 获得当前goroutine的stacktrace
			logrus.Errorf("panic错误: %s", string(buf))
		}
		// 关闭客户端连接
		if !conn.Closed() {
			conn.Close()
		}
	}()

	for {
		err = conn.HandleCommand()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				err = nil
				return
			}
			logrus.Warningf("command error: %v", err)
			return
		}
	}
}
