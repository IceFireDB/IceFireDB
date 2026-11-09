package mysql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/server"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/utils"
	"github.com/sirupsen/logrus"
)

const (
	// acceptBackoff is how long a failed Accept waits before retrying. Without it a
	// transient error (EMFILE, for instance) turns into a busy loop or a crash.
	acceptBackoff = 100 * time.Millisecond
	// drainTimeout bounds how long shutdown waits for in-flight client connections.
	drainTimeout = 5 * time.Second
)

// Run serves the MySQL protocol until ctx is cancelled, then shuts down: the listener
// stops accepting, in-flight connections are given a grace period, and the upstream
// pools are closed.
func Run(ctx context.Context) error {
	ms := newMysqlProxy()
	ms.ctx = ctx
	ms.startedAt = time.Now()

	if err := ms.initClientPool(); err != nil {
		return fmt.Errorf("init client pool: %w", err)
	}

	cfg := config.Get()
	ln, err := net.Listen("tcp4", cfg.Server.Addr)
	if err != nil {
		ms.Close()
		return fmt.Errorf("listen on %s: %w", cfg.Server.Addr, err)
	}

	ms.closed.Store(false)
	logrus.Infof("IceFireDB-SQLProxy %s serving MySQL protocol on %s", Version, cfg.Server.Addr)

	if cfg.P2P.Enable {
		initP2P(ms)
	}

	stopObservability, err := startObservability(ms)
	if err != nil {
		// Observability is not worth refusing to serve for.
		logrus.Errorf("debug endpoints disabled: %v", err)
		stopObservability = func() {}
	}
	defer stopObservability()

	// Closing the listener is what unblocks Accept; it happens on cancellation and must
	// not be repeated by the deferred cleanup.
	var stopWatcher = make(chan struct{})
	utils.GoWithRecover(func() {
		select {
		case <-ctx.Done():
		case <-stopWatcher:
			return
		}
		ms.closed.Store(true)
		_ = ln.Close()
	}, nil)

	defer func() {
		close(stopWatcher)
		_ = ln.Close()
		ms.drain(drainTimeout)
		ms.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			logrus.Errorf("accept error: %v", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(acceptBackoff):
			}
			continue
		}
		go ms.serveConn(conn)
	}
}

// serveConn applies the configured deadlines and runs one client connection.
func (m *mysqlProxy) serveConn(c net.Conn) {
	m.trackConn(c)
	defer m.untrackConn(c)

	cfg := config.Get().Server
	if cfg.IdleTimeout > 0 || cfg.WriteTimeout > 0 {
		c = &deadlineConn{
			Conn:         c,
			readTimeout:  seconds(cfg.IdleTimeout),
			writeTimeout: seconds(cfg.WriteTimeout),
		}
	}
	m.onConn(c)
}

func seconds(v int) time.Duration {
	if v <= 0 {
		return 0
	}
	return time.Duration(v) * time.Second
}

// deadlineConn applies a deadline to every read and write, so a client that opens a
// connection and then goes quiet cannot hold a session forever.
type deadlineConn struct {
	net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (c *deadlineConn) Read(p []byte) (int, error) {
	if c.readTimeout > 0 {
		_ = c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	return c.Conn.Read(p)
}

func (c *deadlineConn) Write(p []byte) (int, error) {
	if c.writeTimeout > 0 {
		_ = c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	return c.Conn.Write(p)
}

func newMysqlProxy() *mysqlProxy {
	p := &mysqlProxy{tenants: make(map[string]string)}
	p.server = server.NewDefaultServer()
	p.credential = server.NewInMemoryProvider()
	for _, info := range config.Get().UserList {
		p.credential.AddUser(info.User, info.Password)
		if prev, ok := p.tenants[info.User]; ok && prev != normalizeRole(info.Role) {
			logrus.Warnf("user %q is declared more than once with different roles; using %q",
				info.User, normalizeRole(info.Role))
		}
		p.tenants[info.User] = normalizeRole(info.Role)
	}
	return p
}

// hasReadonlyTenant reports whether any configured tenant needs the readonly pool.
func (m *mysqlProxy) hasReadonlyTenant() bool {
	for _, role := range m.tenants {
		if role == roleReadonly {
			return true
		}
	}
	return false
}

func (m *mysqlProxy) initClientPool() error {
	mc := config.Get().Mysql

	// Initialize admin pool
	adminPool, err := client.NewPool(
		logrus.Infof,
		mc.Admin.MinAlive,
		mc.Admin.MaxAlive,
		mc.Admin.MaxIdle,
		mc.Admin.Addr,
		mc.Admin.User,
		mc.Admin.Password,
		mc.Admin.DBName,
	)
	if err != nil {
		return fmt.Errorf("failed to create admin pool: %w", err)
	}
	m.adminPool = adminPool

	// Initialize readonly pool if configured. A read-only tenant without a readonly
	// pool would be served from the admin pool, so that combination is a startup error.
	if mc.Readonly.Addr == "" {
		if m.hasReadonlyTenant() {
			adminPool.Close()
			return errors.New("userlist declares a read-only tenant but mysql.readonly.addr is empty")
		}
		logrus.Warn("mysql.readonly.addr is empty: no read-only pool is created")
		return nil
	}

	readonlyPool, err := client.NewPool(
		logrus.Infof,
		mc.Readonly.MinAlive,
		mc.Readonly.MaxAlive,
		mc.Readonly.MaxIdle,
		mc.Readonly.Addr,
		mc.Readonly.User,
		mc.Readonly.Password,
		mc.Readonly.DBName,
	)
	if err != nil {
		adminPool.Close()
		return fmt.Errorf("failed to create readonly pool: %w", err)
	}
	m.readonlyPool = readonlyPool

	return nil
}
