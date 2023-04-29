package mysql

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"runtime"
	"sync"

	"github.com/IceFireDB/IceFireDB-SQLite/pkg/mysql/server"

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

func (m *mysqlProxy) onConn(c net.Conn) {
	conn, err := server.NewClientConn(c, m.server, m.credential, m)
	if err != nil {
		return
	}
	defer func() {
		err := recover()
		if err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			logrus.Errorf("panic: %s", string(buf))
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
