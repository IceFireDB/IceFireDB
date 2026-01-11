package main

import (
	"bytes"
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
		return ".*"
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
			regex.WriteString("[")
			// Handle character class [abc] or [a-z]
			for i++; i < len(pattern) && pattern[i] != ']'; i++ {
				regex.WriteByte(pattern[i])
			}
			regex.WriteString("]")
		case '\\':
			// Escape the next character
			if i+1 < len(pattern) {
				regex.WriteString(regexp.QuoteMeta(string(pattern[i+1])))
				i++
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
			if err != nil {
				err = uhaha.ErrInvalid
				return
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

// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
func (scg scanCommandGroup) cmdSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	cursor, err := parseCursor(args[1])
	if err != nil {
		return nil, err
	}

	match := "*"
	count := 10
	var dataType ledis.DataType // 0 means all types

	// Parse optional parameters
	for i := 2; i < len(args); {
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
			count, err = strconv.Atoi(args[i+1])
			if err != nil {
				return nil, uhaha.ErrInvalid
			}
			i += 2
		case "TYPE":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			switch strings.ToUpper(args[i+1]) {
			case "STRING":
				dataType = ledis.KV
			case "HASH":
				dataType = ledis.HASH
			case "LIST":
				dataType = ledis.LIST
			case "SET":
				dataType = ledis.SET
			case "ZSET":
				dataType = ledis.ZSET
			case "STREAM":
				return nil, fmt.Errorf("ERR unknown type %s", args[i+1])
			default:
				return nil, fmt.Errorf("ERR unknown type %s", args[i+1])
			}
			i += 2
		default:
			return nil, uhaha.ErrWrongNumArgs
		}
	}

	// Convert glob pattern to regex
	matchRegex := globToRegex(match)

	var keys [][]byte
	if dataType == 0 {
		// Scan all types - use KV scan for now (IceFireDB limitation)
		keys, err = ldb.Scan(ledis.KV, cursor, count, false, matchRegex)
		if err == nil && len(keys) < count {
			// Need to scan other types too
			// This is a simplified implementation
			// Full implementation would scan all types and merge results
		}
	} else {
		keys, err = ldb.Scan(dataType, cursor, count, false, matchRegex)
	}

	if err != nil {
		return nil, err
	}

	newCursor := "0"
	if len(keys) >= count {
		newCursor = string(keys[len(keys)-1])
	}

	// Redis SCAN returns: [cursor, [key1, key2, ...]]
	values := make([]interface{}, len(keys))
	for i, key := range keys {
		values[i] = key
	}

	result := make([]interface{}, 2)
	result[0] = newCursor
	result[1] = values

	return result, nil
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
			count, err = strconv.Atoi(args[i+1])
			if err != nil {
				return nil, uhaha.ErrInvalid
			}
			i += 2
		default:
			return nil, uhaha.ErrWrongNumArgs
		}
	}

	// Convert glob pattern to regex
	matchRegex := globToRegex(match)

	fvPairs, err := ldb.HScan(key, cursor, count, false, matchRegex)
	if err != nil {
		return nil, err
	}

	newCursor := "0"
	if len(fvPairs) >= count {
		newCursor = string(fvPairs[len(fvPairs)-1].Field)
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
			count, err = strconv.Atoi(args[i+1])
			if err != nil {
				return nil, uhaha.ErrInvalid
			}
			i += 2
		default:
			return nil, uhaha.ErrWrongNumArgs
		}
	}

	// Convert glob pattern to regex
	matchRegex := globToRegex(match)

	members, err := ldb.SScan(key, cursor, count, false, matchRegex)
	if err != nil {
		return nil, err
	}

	newCursor := "0"
	if len(members) >= count {
		newCursor = string(members[len(members)-1])
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
			count, err = strconv.Atoi(args[i+1])
			if err != nil {
				return nil, uhaha.ErrInvalid
			}
			i += 2
		default:
			return nil, uhaha.ErrWrongNumArgs
		}
	}

	// Convert glob pattern to regex
	matchRegex := globToRegex(match)

	scorePairs, err := ldb.ZScan(key, cursor, count, false, matchRegex)
	if err != nil {
		return nil, err
	}

	newCursor := "0"
	if len(scorePairs) >= count {
		newCursor = string(scorePairs[len(scorePairs)-1].Member)
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
	matchRegex := globToRegex(match)

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
	matchRegex := globToRegex(match)

	var ay []ledis.FVPair
	if !desc {
		ay, err = ldb.HScan(key, cursor, count, true, matchRegex)
	} else {
		ay, err = ldb.HRevScan(key, cursor, count, true, matchRegex)
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
	matchRegex := globToRegex(match)

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
	matchRegex := globToRegex(match)
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
