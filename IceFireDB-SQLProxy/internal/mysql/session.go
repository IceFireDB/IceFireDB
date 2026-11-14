package mysql

import "strings"

// Tenant roles a client connection can authenticate as. The role decides which
// upstream connection pool the session is served from and whether writes are
// accepted at all.
const (
	roleAdmin    = "admin"
	roleReadonly = "readonly"
)

// session is the per-client-connection state needed to replicate writes correctly:
// the role the connection authenticated as, whether a transaction is open, and the
// statements executed inside it.
//
// A transaction is replicated as one batch at COMMIT time. Publishing statements as
// they run would hand peers a partial transaction — a lone INSERT with no BEGIN and
// no COMMIT, which commits immediately there and cannot be rolled back with it.
type session struct {
	role   string
	inTx   bool
	buffer []string
}

func newSession(role string) *session {
	return &session{role: normalizeRole(role)}
}

// normalizeRole maps a configured role onto the roles the proxy understands. An
// unknown or missing role is admin, which is the behaviour every pre-1.0.0 tenant had.
func normalizeRole(role string) string {
	if strings.EqualFold(strings.TrimSpace(role), roleReadonly) {
		return roleReadonly
	}
	return roleAdmin
}

// readOnly reports whether the session must be refused data writes.
func (s *session) readOnly() bool { return s.role == roleReadonly }

// reset drops any open transaction and its buffered statements.
func (s *session) reset() {
	s.inTx = false
	s.buffer = nil
}
