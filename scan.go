package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/siddontang/go/num"
	"github.com/tidwall/uhaha"
)

var (
	xScanGroup = scanCommandGroup{nilCursorLedis, parseXScanArgs}
	scanGroup  = scanCommandGroup{nilCursorRedis, parseScanArgs}
)

var (
	nilCursorLedis = []byte("")
	nilCursorRedis = []byte("0")
)

// globToRegex converts a Redis glob pattern to a regex pattern
// Redis glob patterns use * (any number of characters), ? (single character), [..] (character class)
func globToRegex(pattern string) string {
	if pattern == "" {
		return "^$"
	}

	var regex strings.Builder
	regex.WriteString("^")

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch c {
		case '*':
			regex.WriteString(".*")
		case '?':
			regex.WriteString(".")
		case '[':
			// A character class is only a class when it is closed and non-empty;
			// otherwise Redis matches the bracket literally.
			closing := -1
			for j := i + 1; j < len(pattern); j++ {
				if pattern[j] == ']' {
					closing = j
					break
				}
			}
			if closing <= i+1 {
				regex.WriteString(`\[`)
				continue
			}
			regex.WriteString("[")
			for j := i + 1; j < closing; j++ {
				regex.WriteByte(pattern[j])
			}
			regex.WriteString("]")
			i = closing
		case '\\':
			// Escape the next character; a trailing backslash is a literal one.
			if i+1 < len(pattern) {
				regex.WriteString(regexp.QuoteMeta(string(pattern[i+1])))
				i++
			} else {
				regex.WriteString(`\\`)
			}
		default:
			if isMetaChar(c) {
				regex.WriteString("\\")
			}
			regex.WriteByte(c)
		}
	}

	regex.WriteString("$")
	return regex.String()
}

// isMetaChar checks if a character is a regex metacharacter that needs escaping
func isMetaChar(c byte) bool {
	return c == '.' || c == '^' || c == '$' || c == '|' || c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' || c == '+' || c == '*'
}

func parseCursor(cursorStr string) ([]byte, error) {
	if cursorStr == "0" {
		return nilCursorLedis, nil
	}
	return []byte(cursorStr), nil
}

func init() {
	// Standard Redis SCAN commands
	conf.AddReadCommand("SCAN", scanGroup.cmdSCAN)
	conf.AddReadCommand("HSCAN", scanGroup.cmdHSCAN)
	conf.AddReadCommand("SSCAN", scanGroup.cmdSSCAN)
	conf.AddReadCommand("ZSCAN", scanGroup.cmdZSCAN)

	// IceFireDB extended XSCAN commands
	conf.AddReadCommand("XSCAN", xScanGroup.cmdXSCAN)
	conf.AddReadCommand("XHSCAN", xScanGroup.cmdXHSCAN)
	conf.AddReadCommand("XSSCAN", xScanGroup.cmdXSSCAN)
	conf.AddReadCommand("XZSCAN", xScanGroup.cmdXZSCAN)
}

func parseXScanArgs(args []string) (cursor []byte, match string, count int, desc bool, err error) {
	cursor = []byte(args[0])
	args = args[1:]
	count = 10
	desc = false
	for i := 0; i < len(args); {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			if i+1 >= len(args) {
				err = uhaha.ErrInvalid
				return
			}
			match = args[i+1]
			i++
		case "COUNT":
			if i+1 >= len(args) {
				err = uhaha.ErrInvalid
				return
			}
			count, err = strconv.Atoi(args[i+1])
			if err != nil || count <= 0 {
				err = uhaha.ErrInvalid
				return
			}
			if count > maxScanCount {
				count = maxScanCount
			}
			i++
		case "ASC":
			desc = false
		case "DESC":
			desc = true
		default:
			err = fmt.Errorf("invalid argument %s", args[i])
			return
		}
		i++
	}
	return
}

func parseScanArgs(args []string) (cursor []byte, match string, count int, desc bool, err error) {
	cursor, match, count, desc, err = parseXScanArgs(args)
	if bytes.Equal(cursor, nilCursorRedis) {
		cursor = nilCursorLedis
	}
	return
}

type scanCommandGroup struct {
	lastCursor []byte
	parseArgs  func(args []string) (cursor []byte, match string, count int, desc bool, err error)
}

// parseScanCount reads a COUNT argument and bounds it: COUNT is only a hint in
// Redis, and the value sizes allocations inside the storage layer, so an
// unbounded client value is a memory hazard rather than a page size.
func parseScanCount(arg string) (int, error) {
	n, err := strconv.Atoi(arg)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("ERR syntax error")
	}
	if n > maxScanCount {
		n = maxScanCount
	}
	return n, nil
}

// compileGlob turns a Redis glob into the regexp used to filter results in the
// handler rather than in the storage layer.
func compileGlob(pattern string) (*regexp.Regexp, error) {
	re, err := regexp.Compile(globToRegex(pattern))
	if err != nil {
		return nil, fmt.Errorf("ERR invalid pattern")
	}
	return re, nil
}

// maxScanCount caps the COUNT hint. Redis treats COUNT as a hint, and the value
// sizes allocations inside the engine, so an unbounded client value is a memory
// hazard rather than a query the caller can rely on.
const maxScanCount = 10000

// scanTypeOrder is the fixed order in which SCAN walks the engine's per-type
// keyspaces.
var scanTypeOrder = []struct {
	name     string
	dataType ledis.DataType
}{
	{"string", ledis.KV},
	{"hash", ledis.HASH},
	{"list", ledis.LIST},
	{"set", ledis.SET},
	{"zset", ledis.ZSET},
}

// scanTypeCursor is the resume point of one keyspace.
type scanTypeCursor struct {
	started   bool
	exhausted bool
	last      []byte
}

// scanCursor carries one sub-cursor per keyspace. The engine keeps every type in
// its own keyspace with no global key order, so one Redis cursor has to gather
// all of them; the encoding is opaque to clients.
type scanCursor struct {
	types []scanTypeCursor
}

func newScanCursor() scanCursor {
	return scanCursor{types: make([]scanTypeCursor, len(scanTypeOrder))}
}

func parseScanCursor(s string) (scanCursor, error) {
	c := newScanCursor()
	if s == "" || s == "0" {
		return c, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, fmt.Errorf("ERR invalid cursor")
	}
	fields := strings.Split(string(raw), "|")
	if len(fields) != len(c.types) {
		return c, fmt.Errorf("ERR invalid cursor")
	}
	for i, f := range fields {
		switch f {
		case "":
			// not started yet
		case "-":
			c.types[i].started, c.types[i].exhausted = true, true
		default:
			b, err := base64.RawURLEncoding.DecodeString(f)
			if err != nil {
				return c, fmt.Errorf("ERR invalid cursor")
			}
			c.types[i].started, c.types[i].last = true, b
		}
	}
	return c, nil
}

func (c scanCursor) done() bool {
	for _, t := range c.types {
		if !t.exhausted {
			return false
		}
	}
	return true
}

func (c scanCursor) String() string {
	if c.done() {
		return "0"
	}
	fields := make([]string, len(c.types))
	for i, t := range c.types {
		switch {
		case t.exhausted:
			fields[i] = "-"
		case t.started:
			fields[i] = base64.RawURLEncoding.EncodeToString(t.last)
		}
	}
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(fields, "|")))
}

// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
func (scg scanCommandGroup) cmdSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	cursor, err := parseScanCursor(args[1])
	if err != nil {
		return nil, err
	}

	match := "*"
	count := 10
	typeFilter := ""

	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			match = args[i+1]
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			if count, err = parseScanCount(args[i+1]); err != nil {
				return nil, err
			}
			i++
		case "TYPE":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			typeFilter = strings.ToLower(args[i+1])
			i++
		default:
			return nil, fmt.Errorf("ERR syntax error")
		}
	}

	re, err := compileGlob(match)
	if err != nil {
		return nil, err
	}

	keys := make([][]byte, 0, count)
	remaining := count
	for i, t := range scanTypeOrder {
		st := &cursor.types[i]
		if st.exhausted || remaining <= 0 {
			continue
		}
		// A TYPE filter finishes every other keyspace in place, so one cursor
		// format works with and without the filter.
		if typeFilter != "" && t.name != typeFilter {
			st.started, st.exhausted = true, true
			continue
		}
		// MATCH is applied here, not inside the storage layer: ledis filters before
		// counting, so a pattern that matches little would walk a whole keyspace per
		// call while the Raft read lock is held. Scanning only `remaining` raw keys
		// bounds the work, and the cursor advances past the last raw key, so no entry
		// is skipped.
		raw, err := ldb.Scan(t.dataType, st.last, remaining, false, "")
		if err != nil {
			return nil, err
		}
		st.started = true
		st.exhausted = len(raw) < remaining
		if len(raw) > 0 {
			st.last = raw[len(raw)-1]
		}
		for _, k := range raw {
			if re.Match(k) {
				keys = append(keys, k)
			}
		}
		remaining -= len(raw)
	}

	// Redis SCAN replies with a bulk cursor and the page of keys.
	return []interface{}{[]byte(cursor.String()), keys}, nil
}

// HSCAN key cursor [MATCH pattern] [COUNT count]
func (scg scanCommandGroup) cmdHSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	cursor, err := parseCursor(args[2])
	if err != nil {
		return nil, err
	}

	match := "*"
	count := 10

	for i := 3; i < len(args); {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			match = args[i+1]
			i += 2
		case "COUNT":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			if count, err = parseScanCount(args[i+1]); err != nil {
				return nil, err
			}
			i += 2
		default:
			return nil, uhaha.ErrWrongNumArgs
		}
	}

	// MATCH is applied here rather than inside the storage layer (see cmdSCAN).
	re, err := compileGlob(match)
	if err != nil {
		return nil, err
	}
	raw, err := ldb.HScan(key, cursor, count, false, "")
	if err != nil {
		return nil, err
	}
	fvPairs := make([]ledis.FVPair, 0, len(raw))
	for _, fv := range raw {
		if re.Match(fv.Field) {
			fvPairs = append(fvPairs, fv)
		}
	}

	newCursor := "0"
	if len(raw) >= count {
		newCursor = string(raw[len(raw)-1].Field)
	}

	// Redis HSCAN returns: [cursor, [field1, value1, field2, value2, ...]]
	values := make([]interface{}, len(fvPairs)*2)
	for i, fv := range fvPairs {
		values[i*2] = fv.Field
		values[i*2+1] = fv.Value
	}

	result := make([]interface{}, 2)
	result[0] = newCursor
	result[1] = values

	return result, nil
}

// SSCAN key cursor [MATCH pattern] [COUNT count]
func (scg scanCommandGroup) cmdSSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	cursor, err := parseCursor(args[2])
	if err != nil {
		return nil, err
	}

	match := "*"
	count := 10

	for i := 3; i < len(args); {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			match = args[i+1]
			i += 2
		case "COUNT":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			if count, err = parseScanCount(args[i+1]); err != nil {
				return nil, err
			}
			i += 2
		default:
			return nil, uhaha.ErrWrongNumArgs
		}
	}

	// MATCH is applied here rather than inside the storage layer (see cmdSCAN).
	re, err := compileGlob(match)
	if err != nil {
		return nil, err
	}
	raw, err := ldb.SScan(key, cursor, count, false, "")
	if err != nil {
		return nil, err
	}
	members := make([][]byte, 0, len(raw))
	for _, m := range raw {
		if re.Match(m) {
			members = append(members, m)
		}
	}

	newCursor := "0"
	if len(raw) >= count {
		newCursor = string(raw[len(raw)-1])
	}

	// Redis SSCAN returns: [cursor, [member1, member2, ...]]
	values := make([]interface{}, len(members))
	for i, member := range members {
		values[i] = member
	}

	result := make([]interface{}, 2)
	result[0] = newCursor
	result[1] = values

	return result, nil
}

// ZSCAN key cursor [MATCH pattern] [COUNT count]
func (scg scanCommandGroup) cmdZSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	cursor, err := parseCursor(args[2])
	if err != nil {
		return nil, err
	}

	match := "*"
	count := 10

	for i := 3; i < len(args); {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			match = args[i+1]
			i += 2
		case "COUNT":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			if count, err = parseScanCount(args[i+1]); err != nil {
				return nil, err
			}
			i += 2
		default:
			return nil, uhaha.ErrWrongNumArgs
		}
	}

	// MATCH is applied here rather than inside the storage layer (see cmdSCAN).
	re, err := compileGlob(match)
	if err != nil {
		return nil, err
	}
	raw, err := ldb.ZScan(key, cursor, count, false, "")
	if err != nil {
		return nil, err
	}
	scorePairs := make([]ledis.ScorePair, 0, len(raw))
	for _, sp := range raw {
		if re.Match(sp.Member) {
			scorePairs = append(scorePairs, sp)
		}
	}

	newCursor := "0"
	if len(raw) >= count {
		newCursor = string(raw[len(raw)-1].Member)
	}

	// Redis ZSCAN returns: [cursor, [member1, score1, member2, score2, ...]]
	values := make([]interface{}, len(scorePairs)*2)
	for i, sp := range scorePairs {
		values[i*2] = sp.Member
		values[i*2+1] = num.FormatInt64ToSlice(sp.Score)
	}

	result := make([]interface{}, 2)
	result[0] = newCursor
	result[1] = values

	return result, nil
}

// XSCAN type cursor [MATCH match] [COUNT count] [ASC|DESC]
func (scg scanCommandGroup) cmdXSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	var dataType ledis.DataType
	switch strings.ToUpper(args[1]) {
	case "KV":
		dataType = ledis.KV
	case "HASH":
		dataType = ledis.HASH
	case "LIST":
		dataType = ledis.LIST
	case "SET":
		dataType = ledis.SET
	case "ZSET":
		dataType = ledis.ZSET
	default:
		return nil, fmt.Errorf("invalid key type %s", args[1])
	}

	cursor, match, count, desc, err := scg.parseArgs(args[2:])
	if err != nil {
		return nil, err
	}

	// Convert glob pattern to regex
	// The X* family keeps the contract it had before this change: MATCH is handed
	// to the storage layer as a regular expression. Only the Redis-named scan
	// commands translate a Redis glob.
	matchRegex := match

	var ay [][]byte
	if !desc {
		ay, err = ldb.Scan(dataType, cursor, count, false, matchRegex)
	} else {
		ay, err = ldb.RevScan(dataType, cursor, count, false, matchRegex)
	}
	if err != nil {
		return nil, err
	}

	data := make([]interface{}, 2)
	if len(ay) < count {
		data[0] = scg.lastCursor
	} else {
		data[0] = ay[len(ay)-1]
	}

	// Convert [][]byte to []interface{}
	vv := make([]interface{}, len(ay))
	for i, v := range ay {
		vv[i] = v
	}

	data[1] = vv
	return data, nil
}

// XHSCAN key cursor [MATCH match] [COUNT count] [ASC|DESC]
func (scg scanCommandGroup) cmdXHSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}
	key := []byte(args[1])
	cursor, match, count, desc, err := scg.parseArgs(args[2:])
	if err != nil {
		return nil, err
	}

	// Convert glob pattern to regex
	// The X* family keeps the contract it had before this change: MATCH is handed
	// to the storage layer as a regular expression. Only the Redis-named scan
	// commands translate a Redis glob.
	matchRegex := match

	var ay []ledis.FVPair
	if !desc {
		ay, err = ldb.HScan(key, cursor, count, false, matchRegex)
	} else {
		ay, err = ldb.HRevScan(key, cursor, count, false, matchRegex)
	}

	if err != nil {
		return nil, err
	}

	data := make([]interface{}, 2)
	if len(ay) < count {
		data[0] = scg.lastCursor
	} else {
		data[0] = ay[len(ay)-1].Field
	}

	vv := make([]interface{}, 0, len(ay)*2)
	for _, v := range ay {
		vv = append(vv, v.Field, v.Value)
	}

	data[1] = vv
	return data, nil
}

// XSSCAN key cursor [MATCH match] [COUNT count] [ASC|DESC]
func (scg scanCommandGroup) cmdXSSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	cursor, match, count, desc, err := scg.parseArgs(args[2:])
	if err != nil {
		return nil, err
	}

	// Convert glob pattern to regex
	// The X* family keeps the contract it had before this change: MATCH is handed
	// to the storage layer as a regular expression. Only the Redis-named scan
	// commands translate a Redis glob.
	matchRegex := match

	var ay [][]byte
	if !desc {
		ay, err = ldb.SScan(key, cursor, count, false, matchRegex)
	} else {
		ay, err = ldb.SRevScan(key, cursor, count, false, matchRegex)
	}

	if err != nil {
		return nil, err
	}

	data := make([]interface{}, 2)
	if len(ay) < count {
		data[0] = scg.lastCursor
	} else {
		data[0] = ay[len(ay)-1]
	}

	vv := make([]interface{}, len(ay))
	for i, v := range ay {
		vv[i] = v
	}

	data[1] = vv

	return data, nil
}

// XZSCAN key cursor [MATCH match] [COUNT count] [ASC|DESC]
func (scg scanCommandGroup) cmdXZSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	cursor, match, count, desc, err := scg.parseArgs(args[2:])
	if err != nil {
		return nil, err
	}
	// Convert glob pattern to regex
	// The X* family keeps the contract it had before this change: MATCH is handed
	// to the storage layer as a regular expression. Only the Redis-named scan
	// commands translate a Redis glob.
	matchRegex := match
	var ay []ledis.ScorePair
	if !desc {
		ay, err = ldb.ZScan(key, cursor, count, false, matchRegex)
	} else {
		ay, err = ldb.ZRevScan(key, cursor, count, false, matchRegex)
	}
	if err != nil {
		return nil, err
	}

	data := make([]interface{}, 2)
	if len(ay) < count {
		data[0] = scg.lastCursor
	} else {
		data[0] = ay[len(ay)-1].Member
	}

	vv := make([]interface{}, 0, len(ay)*2)
	for _, v := range ay {
		vv = append(vv, v.Member, num.FormatInt64ToSlice(v.Score))
	}
	data[1] = vv
	return data, nil
}
