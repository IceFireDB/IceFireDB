package mysql

import "strings"

// sqlKind classifies a SQL statement from the proxy's point of view: what may be
// executed locally, what must be replicated to peers, and what a read-only tenant
// is allowed to run.
type sqlKind int

const (
	sqlKindUnknown sqlKind = iota
	// sqlKindRead only observes data.
	sqlKindRead
	// sqlKindSession changes session state only (SET/USE/DO); allowed for
	// read-only tenants and never replicated.
	sqlKindSession
	// sqlKindWrite changes data and is replicated to peers.
	sqlKindWrite
	// sqlKindAdmin changes server state (KILL/FLUSH/CALL/...): it is neither
	// replicated nor allowed for a read-only tenant.
	sqlKindAdmin
	sqlKindBegin
	sqlKindCommit
	sqlKindRollback
)

func (k sqlKind) String() string {
	switch k {
	case sqlKindRead:
		return "read"
	case sqlKindSession:
		return "session"
	case sqlKindWrite:
		return "write"
	case sqlKindAdmin:
		return "admin"
	case sqlKindBegin:
		return "begin"
	case sqlKindCommit:
		return "commit"
	case sqlKindRollback:
		return "rollback"
	default:
		return "unknown"
	}
}

// sqlClassification is what the proxy needs to know about a query before running it.
type sqlClassification struct {
	// Kind is the class of the first statement, or sqlKindWrite when any
	// statement in the batch writes (see Write).
	Kind sqlKind
	// Count is the number of non-empty statements in the batch.
	Count int
	// Write reports that at least one statement changes data and therefore has
	// to be replicated.
	Write bool
}

// replicated reports whether the batch must be published to peers.
func (c sqlClassification) replicated() bool { return c.Write }

// readOnlyAllowed reports whether a read-only tenant may execute the batch.
// Anything the classifier does not positively recognise as a read, a session
// statement or a transaction control statement is refused.
func (c sqlClassification) readOnlyAllowed() bool {
	switch c.Kind {
	case sqlKindRead, sqlKindSession, sqlKindBegin, sqlKindCommit, sqlKindRollback:
		return !c.Write
	default:
		return false
	}
}

// replicateSafe reports whether the batch may be executed on a peer. Only data
// writes are replicated: session and admin statements are node-local by design.
func (c sqlClassification) replicateSafe() bool { return c.Write }

// Statements whose leading keywords are recognised. A statement that starts with
// anything else is classified sqlKindUnknown: read-only tenants are refused it and
// it is not replicated.
var (
	readKeywords = map[string]struct{}{
		"SELECT": {}, "SHOW": {}, "DESCRIBE": {}, "DESC": {}, "EXPLAIN": {},
		"HELP": {}, "TABLE": {}, "VALUES": {},
	}

	sessionKeywords = map[string]struct{}{
		"SET": {}, "USE": {}, "DO": {},
	}

	writeKeywords = map[string]struct{}{
		"INSERT": {}, "UPDATE": {}, "DELETE": {}, "REPLACE": {},
		"CREATE": {}, "ALTER": {}, "DROP": {}, "TRUNCATE": {}, "RENAME": {},
		"GRANT": {}, "REVOKE": {}, "LOAD": {},
	}

	adminKeywords = map[string]struct{}{
		"CALL": {}, "KILL": {}, "FLUSH": {}, "PURGE": {}, "INSTALL": {},
		"UNINSTALL": {}, "SHUTDOWN": {}, "OPTIMIZE": {}, "ANALYZE": {},
		"REPAIR": {}, "LOCK": {}, "UNLOCK": {}, "HANDLER": {}, "IMPORT": {},
		"RESET": {}, "CHANGE": {}, "CACHE": {}, "START": {}, "STOP": {},
		"SAVEPOINT": {}, "RELEASE": {}, "XA": {}, "PREPARE": {}, "EXECUTE": {},
		"DEALLOCATE": {}, "CHECKSUM": {}, "CHECK": {}, "RESTART": {},
	}
)

// classifySQL splits a query into statements, ignores comments, and classifies the
// result. It is deliberately conservative: every branch that cannot prove a
// statement safe returns something other than sqlKindRead.
func classifySQL(sql string) sqlClassification {
	stmts := splitStatements(sql)
	c := sqlClassification{Count: len(stmts)}
	if len(stmts) == 0 {
		return c
	}
	c.Kind = classifyStatement(stmts[0])
	for _, stmt := range stmts {
		if classifyStatement(stmt) == sqlKindWrite {
			c.Write = true
			c.Kind = sqlKindWrite
			break
		}
	}
	return c
}

// classifyStatement classifies a single statement (no top-level semicolons).
func classifyStatement(stmt string) sqlKind {
	first, second := leadingKeywords(stmt)
	switch first {
	case "":
		return sqlKindUnknown
	case "BEGIN":
		if second == "" || second == "WORK" || second == "TRANSACTION" {
			return sqlKindBegin
		}
	case "START":
		if second == "TRANSACTION" {
			return sqlKindBegin
		}
		return sqlKindAdmin
	case "COMMIT":
		return sqlKindCommit
	case "END":
		if second == "" || second == "TRANSACTION" {
			return sqlKindCommit
		}
	case "ROLLBACK":
		if second != "TO" {
			return sqlKindRollback
		}
		return sqlKindAdmin
	}
	if _, ok := readKeywords[first]; ok {
		return sqlKindRead
	}
	if _, ok := sessionKeywords[first]; ok {
		return sqlKindSession
	}
	if _, ok := writeKeywords[first]; ok {
		return sqlKindWrite
	}
	if _, ok := adminKeywords[first]; ok {
		return sqlKindAdmin
	}
	return sqlKindUnknown
}

// leadingKeywords returns the first two keywords of a statement, skipping leading
// comments and whitespace. A leading WITH is resolved through its CTE list so that
// "WITH x AS (...) SELECT ..." is recognised as the statement it really is.
func leadingKeywords(stmt string) (string, string) {
	i := skipIgnorable(stmt, 0)
	word, next := readWord(stmt, i)
	if word != "WITH" {
		if next < 0 {
			return word, ""
		}
		second, _ := readWord(stmt, skipIgnorable(stmt, next))
		return word, second
	}

	main := skipCTEList(stmt, skipIgnorable(stmt, next))
	if main < 0 {
		return "WITH", ""
	}
	first, afterFirst := readWord(stmt, skipIgnorable(stmt, main))
	if afterFirst < 0 {
		return first, ""
	}
	second, _ := readWord(stmt, skipIgnorable(stmt, afterFirst))
	return first, second
}

// splitStatements splits on top-level semicolons, honouring quotes and comments.
func splitStatements(sql string) []string {
	var stmts []string
	start := 0
	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '\'' || sql[i] == '"' || sql[i] == '`':
			i = skipQuoted(sql, i)
		case sql[i] == '-' && i+1 < len(sql) && sql[i+1] == '-' &&
			(i+2 == len(sql) || isSpace(sql[i+2])):
			i = skipLineComment(sql, i)
		case sql[i] == '#':
			i = skipLineComment(sql, i)
		case sql[i] == '/' && i+1 < len(sql) && sql[i+1] == '*':
			next := skipBlockComment(sql, i)
			if next < 0 {
				// An unterminated comment swallows the rest of the input, so the
				// statement ends where the comment began.
				if s := strings.TrimSpace(sql[start:i]); s != "" {
					stmts = append(stmts, s)
				}
				return stmts
			}
			i = next
		case sql[i] == ';':
			if s := strings.TrimSpace(sql[start:i]); s != "" {
				stmts = append(stmts, s)
			}
			i++
			start = i
		default:
			i++
		}
	}
	if s := strings.TrimSpace(sql[start:]); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// skipIgnorable skips whitespace and comments. It returns len(s) at the end of the
// input, and -1 for an unterminated block comment.
func skipIgnorable(s string, i int) int {
	for i < len(s) {
		switch {
		case isSpace(s[i]):
			i++
		case s[i] == '-' && i+1 < len(s) && s[i+1] == '-' &&
			(i+2 == len(s) || isSpace(s[i+2])):
			i = skipLineComment(s, i)
		case s[i] == '#':
			i = skipLineComment(s, i)
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '*':
			i = skipBlockComment(s, i)
			if i < 0 {
				return -1
			}
		default:
			return i
		}
	}
	return i
}

// readWord reads an identifier-like keyword starting at i (which must already be
// past ignorable characters) and returns it upper-cased plus the index after it.
// It returns ("", -1) when there is no word.
func readWord(s string, i int) (string, int) {
	if i < 0 || i >= len(s) {
		return "", -1
	}
	start := i
	for i < len(s) && isWordChar(s[i]) {
		i++
	}
	if i == start {
		return "", -1
	}
	return strings.ToUpper(s[start:i]), i
}

// skipCTEList walks the CTE definitions that follow WITH and returns the index of
// the main statement, or -1 when the list cannot be parsed.
func skipCTEList(s string, i int) int {
	if i < 0 {
		return -1
	}
	if word, next := readWord(s, i); word == "RECURSIVE" {
		i = skipIgnorable(s, next)
	}
	for {
		i = skipIgnorable(s, i)
		if i < 0 || i >= len(s) {
			return -1
		}
		if _, next := readWord(s, i); next < 0 {
			return -1
		} else {
			i = skipIgnorable(s, next)
		}
		if i < len(s) && s[i] == '(' { // optional column list
			next := skipBalanced(s, i)
			if next < 0 {
				return -1
			}
			i = skipIgnorable(s, next)
		}
		word, next := readWord(s, i)
		if word != "AS" {
			return -1
		}
		i = skipIgnorable(s, next)
		if word, next = readWord(s, i); word == "NOT" {
			materialized, afterMaterialized := readWord(s, skipIgnorable(s, next))
			if materialized != "MATERIALIZED" {
				return -1
			}
			i = skipIgnorable(s, afterMaterialized)
		}
		if i < 0 || i >= len(s) || s[i] != '(' {
			return -1
		}
		next = skipBalanced(s, i)
		if next < 0 {
			return -1
		}
		i = skipIgnorable(s, next)
		if i < 0 {
			return -1
		}
		if i < len(s) && s[i] == ',' {
			i++
			continue
		}
		return i
	}
}

// skipBalanced skips a balanced (...) group starting at i and returns the index
// after the closing parenthesis, or -1 when it is unbalanced.
func skipBalanced(s string, i int) int {
	if i < 0 || i >= len(s) || s[i] != '(' {
		return -1
	}
	depth := 0
	for i < len(s) {
		switch {
		case s[i] == '\'' || s[i] == '"' || s[i] == '`':
			i = skipQuoted(s, i)
		case s[i] == '-' && i+1 < len(s) && s[i+1] == '-' &&
			(i+2 == len(s) || isSpace(s[i+2])):
			i = skipLineComment(s, i)
		case s[i] == '#':
			i = skipLineComment(s, i)
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '*':
			i = skipBlockComment(s, i)
			if i < 0 {
				return -1
			}
		case s[i] == '(':
			depth++
			i++
		case s[i] == ')':
			depth--
			i++
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return -1
}

// skipQuoted skips a quoted run starting at the quote character at i and returns
// the index after the closing quote (len(s) when unterminated).
func skipQuoted(s string, i int) int {
	quote := s[i]
	i++
	for i < len(s) {
		switch {
		case s[i] == '\\' && quote != '`':
			i += 2
		case s[i] == quote:
			if i+1 < len(s) && s[i+1] == quote { // doubled quote
				i += 2
				continue
			}
			return i + 1
		default:
			i++
		}
	}
	return len(s)
}

// skipLineComment skips a "-- " or "#" comment up to and including its newline.
func skipLineComment(s string, i int) int {
	for i < len(s) && s[i] != '\n' {
		i++
	}
	return i
}

// skipBlockComment skips a /* ... */ comment and returns -1 when unterminated.
func skipBlockComment(s string, i int) int {
	for i < len(s) {
		if s[i] == '*' && i+1 < len(s) && s[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return -1
}

func isSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	}
	return false
}

func isWordChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '$'
}
