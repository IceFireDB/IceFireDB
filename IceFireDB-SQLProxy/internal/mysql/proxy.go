package mysql

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/mysql"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/server"

	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

const (
	GetConnRetry = 3
)

type mysqlProxy struct {
	ctx          context.Context
	server       *server.Server
	credential   server.CredentialProvider
	pLock        sync.RWMutex
	closed       atomic.Value
	adminPool    *client.Pool
	readonlyPool *client.Pool
}

func NewMySQLProxy(ctx context.Context, server *server.Server, credential server.CredentialProvider) *mysqlProxy {
	proxy := &mysqlProxy{
		ctx:        ctx,
		server:     server,
		credential: credential,
	}

	// Initialize admin connection pool
	adminPool, err := client.NewPool(
		logrus.Infof,
		config.Get().Mysql.Admin.MinAlive,
		config.Get().Mysql.Admin.MaxAlive,
		config.Get().Mysql.Admin.MaxIdle,
		config.Get().Mysql.Admin.Addr,
		config.Get().Mysql.Admin.User,
		config.Get().Mysql.Admin.Password,
		config.Get().Mysql.Admin.DBName,
	)
	if err != nil {
		logrus.Fatalf("Failed to create admin pool: %v", err)
	}
	proxy.adminPool = adminPool

	// Initialize readonly connection pool
	readonlyPool, err := client.NewPool(
		logrus.Infof,
		config.Get().Mysql.Readonly.MinAlive,
		config.Get().Mysql.Readonly.MaxAlive,
		config.Get().Mysql.Readonly.MaxIdle,
		config.Get().Mysql.Readonly.Addr,
		config.Get().Mysql.Readonly.User,
		config.Get().Mysql.Readonly.Password,
		config.Get().Mysql.Readonly.DBName,
	)
	if err != nil {
		logrus.Fatalf("Failed to create readonly pool: %v", err)
	}
	proxy.readonlyPool = readonlyPool

	return proxy
}

func (m *mysqlProxy) onConn(c net.Conn) {
	// Default to admin connection for direct connections
	clientConn, err := m.popAdminConn()
	if err != nil {
		logrus.Errorf("get remote conn err: %v", err)
		return
	}
	defer func() {
		m.pushAdminConn(clientConn, err)
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

func (m *mysqlProxy) popAdminConn() (*client.Conn, error) {
	return m.popConn(m.adminPool)
}

func (m *mysqlProxy) popReadonlyConn() (*client.Conn, error) {
	return m.popConn(m.readonlyPool)
}

func (m *mysqlProxy) popConn(pool *client.Pool) (*client.Conn, error) {
	var mysqlConn *client.Conn
	var err error
	for i := 0; i < GetConnRetry; i++ {
		mysqlConn, err = pool.GetConn(m.ctx)
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

func (m *mysqlProxy) pushAdminConn(mysqlConn *client.Conn, err error) {
	m.pushConn(mysqlConn, err, m.adminPool)
}

func (m *mysqlProxy) pushReadonlyConn(mysqlConn *client.Conn, err error) {
	m.pushConn(mysqlConn, err, m.readonlyPool)
}

func (m *mysqlProxy) pushConn(mysqlConn *client.Conn, err error, pool *client.Pool) {
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
		pool.PutConn(mysqlConn)
	}
}
