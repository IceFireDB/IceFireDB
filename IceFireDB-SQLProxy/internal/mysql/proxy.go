package mysql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/mysql"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/server"

	"github.com/sirupsen/logrus"
)

const (
	// GetConnRetry is how often a usable upstream connection is attempted before the
	// client connection is refused.
	GetConnRetry = 3
	// connRetryBackoff is the base delay between those attempts.
	connRetryBackoff = 50 * time.Millisecond
	// connAcquireTimeout bounds a single pool.GetConn attempt. It is what keeps a proxy
	// with a dead upstream from blocking a client forever: the pool waits for a
	// connection that is never created, so the wait has to be given a deadline.
	connAcquireTimeout = 1 * time.Second
)

type mysqlProxy struct {
	ctx          context.Context
	server       *server.Server
	credential   server.CredentialProvider
	pLock        sync.RWMutex
	adminPool    *client.Pool
	readonlyPool *client.Pool
	// tenants maps an authenticated proxy user to its role.
	tenants map[string]string
	// startedAt is used by /healthz to report uptime.
	startedAt time.Time

	// Active client connections, tracked so shutdown can drain them and force the
	// stragglers closed.
	connWG sync.WaitGroup
	connMu sync.Mutex
	active map[net.Conn]struct{}
}

// trackConn registers a client connection for draining.
func (m *mysqlProxy) trackConn(c net.Conn) {
	m.connMu.Lock()
	if m.active == nil {
		m.active = make(map[net.Conn]struct{})
	}
	m.active[c] = struct{}{}
	m.connMu.Unlock()
	m.connWG.Add(1)
}

func (m *mysqlProxy) untrackConn(c net.Conn) {
	m.connMu.Lock()
	delete(m.active, c)
	m.connMu.Unlock()
	m.connWG.Done()
}

// closeActiveConns closes whatever client connections are still open.
func (m *mysqlProxy) closeActiveConns() {
	m.connMu.Lock()
	conns := make([]net.Conn, 0, len(m.active))
	for c := range m.active {
		conns = append(conns, c)
	}
	m.connMu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
}

// drain waits for in-flight client connections, then forces the remainder closed once
// the grace period expires.
func (m *mysqlProxy) drain(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		m.connWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		logrus.Warnf("client connections still open after %s, closing them", timeout)
		m.closeActiveConns()
		<-done
	}
}

func (m *mysqlProxy) onConn(c net.Conn) {
	sess := newSession(roleAdmin)
	h := newHandle(nil, sess, publishMessage)

	conn, err := server.NewClientConn(c, m.server, m.credential, h)
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			logrus.Errorf("panic %s", string(buf))
		}
		if !conn.Closed() {
			conn.Close()
		}
	}()

	// The tenant's role is only known after the handshake authenticated the user, so the
	// upstream connection is chosen here rather than before it. A read-only tenant must
	// never be served from the admin pool.
	sess = newSession(m.roleOf(conn.GetUser()))
	_ = h.attach(nil, sess)

	upstream, err := m.popConnForRole(sess.role)
	if err != nil {
		// Accept the session and report the problem per statement: writing an error packet
		// out of band here would desync the protocol sequence.
		logrus.Errorf("no upstream connection for tenant %q (role %s): %v", conn.GetUser(), sess.role, err)
		h.attachErr = mysql.NewError(mysql.ER_CON_COUNT_ERROR, "upstream database is unavailable")
	} else {
		if attachErr := h.attach(upstream, sess); attachErr != nil {
			logrus.Errorf("select database for tenant %q: %v", conn.GetUser(), attachErr)
			h.attachErr = mysql.NewError(mysql.ER_BAD_DB_ERROR, "unknown database")
		}
		// Release the upstream connection exactly once, whatever path the client takes.
		defer func() {
			if resetErr := h.resetSession(); resetErr != nil {
				pushConnForRole(m, upstream, resetErr, sess.role)
				return
			}
			pushConnForRole(m, upstream, nil, sess.role)
		}()
	}

	for {
		if err := conn.HandleCommand(); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logrus.Warningf("command error: %v", err)
			return
		}
	}
}

// roleOf returns the role an authenticated user was configured with.
func (m *mysqlProxy) roleOf(user string) string {
	if role, ok := m.tenants[user]; ok {
		return role
	}
	return roleAdmin
}

// popConnForRole returns an upstream connection from the pool that serves role.
func (m *mysqlProxy) popConnForRole(role string) (*client.Conn, error) {
	if role == roleReadonly {
		return m.popReadonlyConn()
	}
	return m.popAdminConn()
}

func (m *mysqlProxy) popAdminConn() (*client.Conn, error) {
	return m.popConn(m.adminPool)
}

func (m *mysqlProxy) popReadonlyConn() (*client.Conn, error) {
	return m.popConn(m.readonlyPool)
}

// popConn returns a connection that has been pinged and reset to a known state, or an
// error when every attempt failed. Returning a nil connection with a nil error — which
// used to happen — made the caller dereference it.
func (m *mysqlProxy) popConn(pool *client.Pool) (*client.Conn, error) {
	if pool == nil {
		return nil, errors.New("upstream connection pool is not configured")
	}

	var lastErr error
	for attempt := 0; attempt < GetConnRetry; attempt++ {
		ctx, cancel := context.WithTimeout(m.ctx, connAcquireTimeout)
		mysqlConn, err := pool.GetConn(ctx)
		cancel()
		if err != nil {
			lastErr = err
		} else if err := prepareConn(mysqlConn); err != nil {
			lastErr = err
			_ = mysqlConn.Close()
		} else {
			return mysqlConn, nil
		}

		if attempt+1 == GetConnRetry {
			break
		}
		select {
		case <-m.ctx.Done():
			return nil, m.ctx.Err()
		case <-time.After(time.Duration(attempt+1) * connRetryBackoff):
		}
	}

	return nil, fmt.Errorf("no usable upstream connection after %d attempts: %w", GetConnRetry, lastErr)
}

// prepareConn brings a pooled connection back to the state a fresh client expects.
func prepareConn(conn *client.Conn) error {
	if err := conn.Ping(); err != nil {
		return err
	}
	if conn.IsInTransaction() {
		if err := conn.Rollback(); err != nil {
			return err
		}
	}
	if !conn.IsAutoCommit() {
		if err := conn.SetAutoCommit(true); err != nil {
			return err
		}
	}
	if conn.GetCharset() != mysql.DEFAULT_CHARSET {
		if err := conn.SetCharset(mysql.DEFAULT_CHARSET); err != nil {
			return err
		}
	}
	return nil
}

func (m *mysqlProxy) pushAdminConn(mysqlConn *client.Conn, err error) {
	m.pushConn(mysqlConn, err, m.adminPool)
}

func (m *mysqlProxy) pushReadonlyConn(mysqlConn *client.Conn, err error) {
	m.pushConn(mysqlConn, err, m.readonlyPool)
}

// pushConn returns a connection to the pool, or closes it when it cannot be trusted.
// A connection is never silently dropped: that leaks a file descriptor and a MySQL
// session for every occurrence.
func (m *mysqlProxy) pushConn(mysqlConn *client.Conn, err error, pool *client.Pool) {
	if mysqlConn == nil || mysqlConn.Conn == nil {
		return
	}
	if errors.Is(err, mysql.ErrBadConn) || mysqlConn.IOErr() != nil {
		_ = mysqlConn.Close()
		return
	}
	if mysqlConn.IsInTransaction() {
		// Handing an open transaction to the next client would leak it into their
		// session; closing rolls it back server-side.
		_ = mysqlConn.Close()
		return
	}
	if err != nil {
		if rollbackErr := mysqlConn.Rollback(); rollbackErr != nil {
			_ = mysqlConn.Close()
			return
		}
	}
	if pool == nil {
		_ = mysqlConn.Close()
		return
	}
	pool.PutConn(mysqlConn)
}

// Close stops the upstream pools.
func (m *mysqlProxy) Close() {
	m.pLock.Lock()
	defer m.pLock.Unlock()
	if m.adminPool != nil {
		m.adminPool.Close()
	}
	if m.readonlyPool != nil {
		m.readonlyPool.Close()
	}
}
