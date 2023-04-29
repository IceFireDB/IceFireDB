package mysql

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"

	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/mysql"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/server"

	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

const (
	GetConnRetry = 3
)

type mysqlProxy struct {
	ctx        context.Context
	server     *server.Server
	credential server.CredentialProvider
	pLock      sync.RWMutex
	closed     atomic.Value
	pool       *client.Pool
}

func (m *mysqlProxy) onConn(c net.Conn) {
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
			buf = buf[:runtime.Stack(buf, false)]
			logrus.Errorf("panic %s", string(buf))
		}

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

		if mysqlConn.IsInTransaction() {
			if err := mysqlConn.Rollback(); err != nil {
				mysqlConn.Close()
				continue
			}
		}
		if !mysqlConn.IsAutoCommit() {
			if err := mysqlConn.SetAutoCommit(true); err != nil {
				mysqlConn.Close()
				continue
			}
		}

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
