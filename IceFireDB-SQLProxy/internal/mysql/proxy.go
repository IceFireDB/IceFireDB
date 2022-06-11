package mysql

import (
	"context"
	"errors"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/mysql"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/server"
	"net"
	"runtime"
	"sync"

	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

const (
	GetConnRetry = 3 // 从连接池获取连接最大重试次数
)

type mysqlProxy struct {
	ctx        context.Context
	server     *server.Server
	credential server.CredentialProvider
	pLock      sync.RWMutex
	closed     atomic.Value
	pool       *client.Pool
}

// 代理连接
func (m *mysqlProxy) onConn(c net.Conn) {
	// 接收客户端数据包，解析包内容获取账号密码数据库
	clientConn, err := m.popMysqlConn()
	if err != nil {
		logrus.Errorf("get remote conn err:", err)
		return
	}
	defer func() {
		m.pushMysqlConn(clientConn, err)
	}()
	h := &Handle{conn: clientConn}

	conn, err := server.NewClientConn(c, m.server, m.credential, h)
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

// 从连接池获取远程mysql连接
func (m *mysqlProxy) popMysqlConn() (*client.Conn, error) {
	var mysqlConn *client.Conn
	var err error
	for i := 0; i < GetConnRetry; i++ {
		mysqlConn, err = m.pool.GetConn(m.ctx)
		if err != nil {
			continue
		}
		err = mysqlConn.Ping()
		if err != nil {
			continue
		}
		// 如果是事务则回滚上次提交
		if mysqlConn.IsInTransaction() {
			if err := mysqlConn.Rollback(); err != nil {
				mysqlConn.Close()
				continue
			}
		}
		// 如果不是自动提交，则设置自动提交
		if !mysqlConn.IsAutoCommit() {
			if err := mysqlConn.SetAutoCommit(true); err != nil {
				mysqlConn.Close()
				continue
			}
		}
		// 如果编码不是默认编码则设置为默认
		if mysqlConn.GetCharset() != mysql.DEFAULT_CHARSET {
			if err := mysqlConn.SetCharset(mysql.DEFAULT_CHARSET); err != nil {
				mysqlConn.Close()
				continue
			}
		}
		break
	}
	return mysqlConn, nil
}

// 回收连接
func (m *mysqlProxy) pushMysqlConn(mysqlConn *client.Conn, err error) {
	if errors.Is(err, mysql.ErrBadConn) {
		mysqlConn.Close()
		return
	}
	if mysqlConn.IsInTransaction() {
		return
	}
	if err != nil {
		if err := mysqlConn.Rollback(); err != nil {
			mysqlConn.Close()
			return
		}
	}
	if mysqlConn.IOErr() != nil {
		mysqlConn.Close()
		return
	}
	if mysqlConn.Conn != nil {
		m.pool.PutConn(mysqlConn)
	}
}
