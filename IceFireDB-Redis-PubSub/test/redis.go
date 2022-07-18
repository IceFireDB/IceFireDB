package test

import (
	"errors"
	"fmt"
	"strings"
)

const (
	msgWrongType          = "WRONGTYPE Operation against a key holding the wrong kind of value"
	msgNotValidHllValue   = "WRONGTYPE Key is not a valid HyperLogLog string value."
	msgInvalidInt         = "ERR value is not an integer or out of range"
	msgInvalidFloat       = "ERR value is not a valid float"
	msgInvalidMinMax      = "ERR min or max is not a float"
	msgInvalidRangeItem   = "ERR min or max not valid string range item"
	msgInvalidTimeout     = "ERR timeout is not a float or out of range"
	msgSyntaxError        = "ERR syntax error"
	msgKeyNotFound        = "ERR no such key"
	msgOutOfRange         = "ERR index out of range"
	msgInvalidCursor      = "ERR invalid cursor"
	msgXXandNX            = "ERR XX and NX options at the same time are not compatible"
	msgNegTimeout         = "ERR timeout is negative"
	msgInvalidSETime      = "ERR invalid expire time in set"
	msgInvalidSETEXTime   = "ERR invalid expire time in setex"
	msgInvalidPSETEXTime  = "ERR invalid expire time in psetex"
	msgInvalidKeysNumber  = "ERR Number of keys can't be greater than number of args"
	msgNegativeKeysNumber = "ERR Number of keys can't be negative"
	msgFScriptUsage       = "ERR Unknown subcommand or wrong number of arguments for '%s'. Try SCRIPT HELP."
	msgFPubsubUsage       = "ERR Unknown subcommand or wrong number of arguments for '%s'. Try PUBSUB HELP."
	msgSingleElementPair  = "ERR INCR option supports a single increment-element pair"
	msgInvalidStreamID    = "ERR Invalid stream ID specified as stream command argument"
	msgStreamIDTooSmall   = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
	msgStreamIDZero       = "ERR The ID specified in XADD must be greater than 0-0"
	msgNoScriptFound      = "NOSCRIPT No matching script. Please use EVAL."
	msgUnsupportedUnit    = "ERR unsupported unit provided. please use m, km, ft, mi"
	msgNotFromScripts     = "This Redis command is not allowed from scripts"
	msgXreadUnbalanced    = "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified."
	msgXgroupKeyNotFound  = "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."
)

func errWrongNumber(cmd string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd))
}

func errLuaParseError(err error) string {
	return fmt.Sprintf("ERR Error compiling script (new function): %s", err.Error())
}

func errReadgroup(key, group string) error {
	return fmt.Errorf("NOGROUP No such key '%s' or consumer group '%s'", key, group)
}

func errXreadgroup(key, group string) error {
	return fmt.Errorf("NOGROUP No such key '%s' or consumer group '%s' in XREADGROUP with GROUP option", key, group)
}

var (
	// ErrKeyNotFound is returned when a key doesn't exist.
	ErrKeyNotFound = errors.New(msgKeyNotFound)

	// ErrWrongType when a key is not the right type.
	ErrWrongType = errors.New(msgWrongType)

	// ErrNotValidHllValue when a key is not a valid HyperLogLog string value.
	ErrNotValidHllValue = errors.New(msgNotValidHllValue)

	// ErrIntValueError can returned by INCRBY
	ErrIntValueError = errors.New(msgInvalidInt)

	// ErrFloatValueError can returned by INCRBYFLOAT
	ErrFloatValueError = errors.New(msgInvalidFloat)
)

// sliceBinOp applies an operator to all slice elements, with Redis string
// padding logic.
func sliceBinOp(f func(a, b byte) byte, a, b []byte) []byte {
	maxl := len(a)
	if len(b) > maxl {
		maxl = len(b)
	}
	lA := make([]byte, maxl)
	copy(lA, a)
	lB := make([]byte, maxl)
	copy(lB, b)
	res := make([]byte, maxl)
	for i := range res {
		res[i] = f(lA[i], lB[i])
	}
	return res
}
