package main

import (
	"bytes"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/siddontang/go/hack"
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
)

// Key-based mutex lock to ensure atomicity of RPOPLPUSH operations
const mutexCount = 256 // Sufficient locks to reduce contention
var listMutexes [mutexCount]sync.Mutex

// Global mutex for same source/destination RPOPLPUSH operations
var sameKeyMutex sync.Mutex

// keyMutexIndex calculates mutex index based on key
func keyMutexIndex(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32() % mutexCount
}

func init() {
	//All block type instructions of the queue need to be avoided, dangerous operation
	//conf.AddReadCommand("BLPOP", cmdBLPOP) //Danger here: If it is a Raft write command, Raft will perform command rollback => stuck Raft. If it is a Raft read command, then Raft will not be able to roll back the queue consumption log, and dirty data in the queue will appear.
	conf.AddWriteCommand("RPUSH", cmdRPUSH)
	conf.AddWriteCommand("LPOP", cmdLPOP)
	conf.AddReadCommand("LINDEX", cmdLINDEX)
	conf.AddWriteCommand("LPUSH", cmdLPUSH)
	conf.AddWriteCommand("RPOP", cmdRPOP)
	conf.AddReadCommand("LRANGE", cmdLRANGE)
	conf.AddWriteCommand("LSET", cmdLSET)
	conf.AddReadCommand("LLEN", cmdLLEN)
	conf.AddWriteCommand("RPOPLPUSH", cmdRPOPLPUSH)

	// IceFireDB special commands
	conf.AddWriteCommand("LCLEAR", cmdLCLEAR)
	conf.AddWriteCommand("LMCLEAR", cmdLMCLEAR)
	// Timeout instructions: be cautious, raft log rollback causes dirty data: timeout LEXPIRE => LEXPIREAT
	conf.AddWriteCommand("LEXPIRE", cmdLEXPIRE)
	conf.AddWriteCommand("LEXPIREAT", cmdLEXPIREAT)
	conf.AddReadCommand("LTTL", cmdLTTL)
	// conf.AddWriteCommand("LPERSIST", cmdLPERSIST)
	conf.AddReadCommand("LKEYEXISTS", cmdLKEYEXISTS)

	conf.AddWriteCommand("LTRIM", cmdLTRIM)
}

func cmdLTRIM(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	var start int64
	var stop int64
	var err error

	start, err = ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}
	stop, err = ledis.StrInt64([]byte(args[3]), nil)
	if err != nil {
		return nil, err
	}

	if err := ldb.LTrim([]byte(args[1]), start, stop); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func cmdLKEYEXISTS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.LKeyExists([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

// func cmdLPERSIST(m uhaha.Machine, args []string) (interface{}, error) {
// 	if len(args) != 2 {
// 		return nil, uhaha.ErrWrongNumArgs
// 	}

// 	n, err := ldb.LPersist([]byte(args[1]))
// 	if err != nil {
// 		return nil, err
// 	}
// 	return redcon.SimpleInt(n), nil
// }

func cmdLTTL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	v, err := ldb.LTTL([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func cmdLEXPIREAT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	timestamp, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	//If the timestamp is less than the current time, delete operation: There are boundary conditions here: because it is a queue, the rollback of the raft log is sequential
	if timestamp < time.Now().Unix() {
		_, err := ldb.LClear([]byte(args[1]))
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(0), nil
	}

	v, err := ldb.LExpireAt([]byte(args[1]), timestamp)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func cmdLEXPIRE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	duration, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	timestamp := m.Now().Unix() + duration
	//If the timestamp is less than the current time, delete operation
	if timestamp < time.Now().Unix() {
		_, err := ldb.LClear([]byte(args[1]))
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(0), nil
	}

	v, err := ldb.LExpireAt([]byte(args[1]), timestamp)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func cmdLMCLEAR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	keys := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
	}
	n, err := ldb.LMclear(keys...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdLCLEAR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.LClear([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdRPOPLPUSH(m uhaha.Machine, args []string) (interface{}, error) {

	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}
	source, dest := []byte(args[1]), []byte(args[2])

	var ttl int64 = -1
	var expireTime int64 = -1
	if bytes.Compare(source, dest) == 0 {
		var err error
		ttl, err = ldb.LTTL(source)
		if err != nil {
			return nil, err
		}
		if ttl != -1 {
			expireTime = m.Now().Unix() + ttl
		}
	}

	// Special handling for same source and destination (rotation)
	if bytes.Compare(source, dest) == 0 {
		// Use global mutex for same source/destination operations
		// to ensure absolute atomicity
		sameKeyMutex.Lock()
		defer sameKeyMutex.Unlock()

		// Get the last element
		data, err := ldb.RPop(source)
		if err != nil {
			return nil, err
		}

		if data == nil {
			return nil, nil
		}

		// Push it back to the front
		if _, err := ldb.LPush(source, data); err != nil {
			// If push fails, revert the pop
			ldb.RPush(source, data)
			return nil, err
		}

		//reset ttl using absolute time
		if expireTime != -1 {
			ldb.LExpireAt(source, expireTime)
		}

		return data, nil
	}

	// For different source and destination, we need to lock both keys
	// to prevent deadlocks, we always lock in a consistent order
	var firstKey, secondKey []byte
	if bytes.Compare(source, dest) < 0 {
		firstKey = source
		secondKey = dest
	} else {
		firstKey = dest
		secondKey = source
	}

	// Lock first key
	firstMutexIndex := keyMutexIndex(firstKey)
	listMutexes[firstMutexIndex].Lock()

	// Lock second key (if different from first key)
	secondMutexIndex := keyMutexIndex(secondKey)
	if firstMutexIndex != secondMutexIndex {
		listMutexes[secondMutexIndex].Lock()
	}

	// Ensure unlocks happen in reverse order
	defer func() {
		if firstMutexIndex != secondMutexIndex {
			listMutexes[secondMutexIndex].Unlock()
		}
		listMutexes[firstMutexIndex].Unlock()
	}()

	data, err := ldb.RPop(source)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	if _, err := ldb.LPush(dest, data); err != nil {
		ldb.RPush(source, data) //revert pop
		return nil, err
	}

	return data, nil
}

func cmdLLEN(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.LLen([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdLSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	index, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	if err := ldb.LSet([]byte(args[1]), int32(index), []byte(args[3])); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func cmdLRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	var start int64
	var stop int64
	var err error

	start, err = ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	stop, err = ledis.StrInt64([]byte(args[3]), nil)
	if err != nil {
		return nil, err
	}

	v, err := ldb.LRange([]byte(args[1]), int32(start), int32(stop))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdRPOP(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	v, err := ldb.RPop([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdLPUSH(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	argList := make([][]byte, len(args)-2)
	for i := 2; i < len(args); i++ {
		argList[i-2] = []byte(args[i])
	}
	n, err := ldb.LPush([]byte(args[1]), argList...)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

func cmdLINDEX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}
	index, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	v, err := ldb.LIndex([]byte(args[1]), int32(index))
	if err != nil {
		return nil, err
	}

	// return `(nil)` when out of range
	if len(v) == 0 {
		return nil, nil
	}

	return v, nil
}

func cmdLPOP(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	v, err := ldb.LPop([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdRPUSH(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	argList := make([][]byte, len(args)-2)
	for i := 2; i < len(args); i++ {
		argList[i-2] = []byte(args[i])
	}
	n, err := ldb.RPush([]byte(args[1]), argList...)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

//Danger here: If it is a Raft write command, Raft will perform command rollback => stuck Raft. If it is a Raft read command, then Raft will not be able to roll back the queue consumption log, and dirty data in the queue will appear.
// func cmdBLPOP(m uhaha.Machine, args []string) (interface{}, error) {
// 	if len(args) < 3 {
// 		return nil, uhaha.ErrWrongNumArgs
// 	}

// 	keys, timeout, err := lParseBPopArgs(args)

// 	if err != nil {
// 		return nil, err
// 	}

// 	ay, err := ldb.BLPop(keys, timeout)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return ay, nil
// }

func lParseBPopArgs(argsOrigin []string) (keys [][]byte, timeout time.Duration, err error) {
	if len(argsOrigin) < 3 {
		err = uhaha.ErrWrongNumArgs
		return
	}

	args := make([][]byte, len(argsOrigin)-1)
	for i := 1; i < len(argsOrigin); i++ {
		args[i-1] = []byte(argsOrigin[i])
	}
	var t float64
	if t, err = strconv.ParseFloat(hack.String(args[len(args)-1]), 64); err != nil {
		return
	}

	timeout = time.Duration(t * float64(time.Second))

	keys = args[0 : len(args)-1]
	return
}
