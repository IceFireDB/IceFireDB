package mysql

import (
	"errors"
	"fmt"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/mysql"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/server"
)

var (
	// errReadOnlyStatement refuses data and server-state changes from a read-only tenant.
	errReadOnlyStatement = errors.New("readonly user cannot execute write operations")
	// errStmtContext reports a prepared statement whose context is not ours.
	errStmtContext = errors.New("prepared statement context has unexpected type")
	// errUpstreamUnavailable is returned when a query arrives without an upstream
	// connection, which should not happen after a successful attach.
	errUpstreamUnavailable = errors.New("upstream database connection is not available")
)

// Handle serves one client connection against one upstream MySQL connection. It owns
// the session state needed to replicate writes correctly.
type Handle struct {
	conn    *client.Conn
	session *session
	// emit publishes a replication message. nil means replication is disabled.
	emit func(*replicationMessage)
	// pendingDB is the database the client asked for during the handshake, before the
	// tenant's role identified an upstream connection.
	pendingDB string
	// attachErr is why the session has no upstream connection. It is reported through the
	// normal command path so the client receives a proper MySQL error packet with the
	// right sequence number, instead of a connection that breaks mid-protocol.
	attachErr error
}

func newHandle(conn *client.Conn, sess *session, emit func(*replicationMessage)) *Handle {
	if sess == nil {
		sess = newSession(roleAdmin)
	}
	return &Handle{conn: conn, session: sess, emit: emit}
}

// attach binds the upstream connection and session once the tenant is authenticated. A
// database selected during the handshake is applied here, because the upstream
// connection does not exist yet at that point.
func (h *Handle) attach(conn *client.Conn, sess *session) error {
	if sess != nil {
		h.session = sess
	}
	h.conn = conn
	if conn == nil || h.pendingDB == "" {
		return nil
	}
	db := h.pendingDB
	h.pendingDB = ""
	return conn.UseDB(db)
}

// CloseConn is called when the client connection goes away. An open transaction is
// rolled back — closing a MySQL connection never commits it — and the connection is
// left clean for the pool.
func (h *Handle) CloseConn(c *server.Conn) error {
	return h.resetSession()
}

// resetSession discards any open transaction and its replication buffer. It is called
// both on an orderly disconnect (COM_QUIT) and when the client vanishes mid-session.
func (h *Handle) resetSession() error {
	inTx := h.session.inTx
	h.session.reset()
	if !inTx || h.conn == nil {
		return nil
	}
	return h.conn.Rollback()
}

// UseDB is called during the handshake and on COM_INIT_DB. During the handshake there is
// no upstream connection yet — the tenant's role is only known once the user is
// authenticated — so the requested database is remembered and applied on attach.
func (h *Handle) UseDB(c *server.Conn, dbName string) error {
	if h.conn == nil {
		if h.attachErr != nil {
			return h.attachErr
		}
		h.pendingDB = dbName
		return nil
	}
	return h.conn.UseDB(dbName)
}

func (h *Handle) HandleQuery(c *server.Conn, query string) (*mysql.Result, error) {
	if err := h.checkUpstream(); err != nil {
		return nil, err
	}
	cls := classifySQL(query)
	if err := h.checkReadOnly(cls); err != nil {
		return nil, err
	}

	res, err := h.conn.Execute(query)
	if err != nil {
		return nil, err
	}

	h.afterExecute(c, cls, query)
	return res, nil
}

func (h *Handle) HandleFieldList(c *server.Conn, table string, fieldWildcard string) ([]*mysql.Field, error) {
	if err := h.checkUpstream(); err != nil {
		return nil, err
	}
	return h.conn.FieldList(table, fieldWildcard)
}

func (h *Handle) HandleStmtPrepare(c *server.Conn, query string) (int, int, any, error) {
	if err := h.checkUpstream(); err != nil {
		return 0, 0, nil, err
	}
	stmt, err := h.conn.Prepare(query)
	if err != nil {
		return 0, 0, nil, err
	}
	return stmt.ParamNum(), stmt.ColumnNum(), stmt, nil
}

func (h *Handle) HandleStmtExecute(c *server.Conn, context any, query string, args []any) (*mysql.Result, error) {
	if err := h.checkUpstream(); err != nil {
		return nil, err
	}
	stmt, ok := context.(*client.Stmt)
	if !ok {
		return nil, errStmtContext
	}

	// Replication can only carry SQL text, so the parameters are inlined before the
	// statement is classified and published. A bare "INSERT ... VALUES (?)" would be
	// executed literally by a peer.
	statement, err := materialize(query, args)
	if err != nil {
		return nil, err
	}

	cls := classifySQL(statement)
	if err := h.checkReadOnly(cls); err != nil {
		return nil, err
	}

	res, err := stmt.Execute(args...)
	if err != nil {
		return nil, err
	}

	h.afterExecute(c, cls, statement)
	return res, nil
}

func (h *Handle) HandleStmtClose(c *server.Conn, context any) error {
	if h.conn == nil {
		return nil
	}
	stmt, ok := context.(*client.Stmt)
	if !ok {
		return errStmtContext
	}
	return stmt.Close()
}

func (h *Handle) HandleOtherCommand(c *server.Conn, cmd byte, data []byte) error {
	return fmt.Errorf("command %d is not supported", cmd)
}

// checkUpstream returns why this session cannot serve a statement, or nil.
func (h *Handle) checkUpstream() error {
	if h.conn != nil {
		return nil
	}
	if h.attachErr != nil {
		return h.attachErr
	}
	return errUpstreamUnavailable
}

// checkReadOnly refuses anything a read-only tenant must not run. The classifier
// answers "may this run at all"; unrecognised statements are refused too.
func (h *Handle) checkReadOnly(cls sqlClassification) error {
	if h.session.readOnly() && !cls.readOnlyAllowed() {
		recordRejected("readonly")
		return errReadOnlyStatement
	}
	return nil
}

// afterExecute replicates what just succeeded locally and advances the transaction
// state machine. It is only called after the local statement succeeded, so a failed
// write is never shared with peers.
func (h *Handle) afterExecute(c *server.Conn, cls sqlClassification, statement string) {
	if cls.replicated() {
		if h.session.readOnly() {
			// Unreachable: read-only sessions are refused before execution.
			return
		}
		if h.session.inTx {
			// Held until COMMIT so peers never see a partial transaction.
			h.session.buffer = append(h.session.buffer, statement)
			return
		}
		h.emitMessage(newReplicationMessage(messageKindSQL, h.session.role, []string{statement}))
		return
	}

	if cls.Count != 1 {
		// A batch that mixes transaction control with other statements keeps the
		// state machine untouched; it is replicated as text when it writes.
		return
	}

	switch cls.Kind {
	case sqlKindBegin:
		h.session.inTx = true
		h.session.buffer = nil
		setTransactionStatus(c, true)
	case sqlKindCommit:
		if h.session.inTx && len(h.session.buffer) > 0 {
			h.emitMessage(newReplicationMessage(messageKindTxn, h.session.role, h.session.buffer))
		}
		h.session.reset()
		setTransactionStatus(c, false)
	case sqlKindRollback:
		h.session.reset()
		setTransactionStatus(c, false)
	}
}

// setTransactionStatus keeps the status flags the client sees in line with the session,
// so drivers that track the explicit transaction state stay correct.
func setTransactionStatus(c *server.Conn, inTx bool) {
	if c == nil {
		return
	}
	if inTx {
		c.SetInTransaction()
		return
	}
	c.ClearInTransaction()
}

func (h *Handle) emitMessage(msg *replicationMessage) {
	if h.emit == nil {
		return
	}
	h.emit(msg)
}
