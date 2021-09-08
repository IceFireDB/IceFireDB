/*
 * @Author: gitsrc
 * @Date: 2021-03-08 17:57:04
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-08-20 10:43:33
 * @FilePath: /IceFireDB/lists.go
 */

package main

import (
	"bytes"
	"strconv"
	"time"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/siddontang/go/hack"
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
	rafthub "github.com/tidwall/uhaha"
)

func init() {
	// 队列的block类型指令均需要避免，危险操作
	// All block type instructions of the queue need to be avoided, dangerous operation
	// conf.AddReadCommand("BLPOP", cmdBLPOP) //此处危险：如果是raft写指令，则raft会进行指令回滚=>卡住raft，如果是raft读指令，则会因为raft无法回滚队列消费日志，出现队列脏数据
	conf.AddWriteCommand("RPUSH", cmdRPUSH)
	conf.AddWriteCommand("LPOP", cmdLPOP)
	conf.AddReadCommand("LINDEX", cmdLINDEX)
	conf.AddWriteCommand("LPUSH", cmdLPUSH)
	conf.AddWriteCommand("RPOP", cmdRPOP)
	conf.AddReadCommand("LRANGE", cmdLRANGE)
	conf.AddWriteCommand("LSET", cmdLSET)
	conf.AddReadCommand("LLEN", cmdLLEN)
	conf.AddWriteCommand("RPOPLPUSH", cmdRPOPLPUSH)

	// IceFireDB special command
	conf.AddWriteCommand("LCLEAR", cmdLCLEAR)
	conf.AddWriteCommand("LMCLEAR", cmdLMCLEAR)
	// Timeout instruction: be cautious, the raft log is rolled back, causing dirty data: timeout LEXPIRE => LEXPIREAT
	// conf.AddWriteCommand("LEXPIRE", cmdLEXPIRE) //超时时间指令：谨慎，raft日志回滚，造成脏数据:超时时间  LEXPIRE => LEXPIREAT
	conf.AddWriteCommand("LEXPIREAT", cmdLEXPIREAT)
	conf.AddReadCommand("LTTL", cmdLTTL)
	// conf.AddWriteCommand("LPERSIST", cmdLPERSIST)
	conf.AddReadCommand("LKEYEXISTS", cmdLKEYEXISTS)

	conf.AddWriteCommand("LTRIM", cmdLTRIM)
}

func cmdLTRIM(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
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

	if err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LTrim([]byte(args[1]), start, stop); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func cmdLKEYEXISTS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LKeyExists([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

// func cmdLPERSIST(m uhaha.Machine, args []string) (interface{}, error) {
// 	if len(args) != 2 {
// 		return nil, rafthub.ErrWrongNumArgs
// 	}

// 	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LPersist([]byte(args[1]))
// 	if err != nil {
// 		return nil, err
// 	}
// 	return redcon.SimpleInt(n), nil
// }

func cmdLTTL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LTTL([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func cmdLEXPIREAT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	when, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	// 如果时间戳小于当前时间，则进行删除操作 :此处有边界条件：因为是队列，raft 日志回滚是顺序的
	// If the timestamp is less than the current time, delete operation: There are boundary conditions here: because it is a queue, the rollback of the raft log is sequential
	if when < time.Now().Unix() {
		_, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LClear([]byte(args[1]))
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(0), nil
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LExpireAt([]byte(args[1]), when)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func cmdLEXPIRE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	duration, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LExpire([]byte(args[1]), duration)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func cmdLMCLEAR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	keys := make([][]byte, len(args)-1)
	var count int64 = 0
	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
		n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LClear([]byte(args[1]))
		if err != nil {
			return nil, err
		}
		count += n
	}
	//n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LMclear(keys...)
	//if err != nil {
	//	return nil, err
	//}
	return redcon.SimpleInt(count), nil
}

func cmdLCLEAR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LClear([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdRPOPLPUSH(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}
	source, dest := []byte(args[1]), []byte(args[2])

	var ttl int64 = -1
	if bytes.Compare(source, dest) == 0 {
		var err error
		ttl, err = ldb.GetDBForKeyUnsafe(source).LTTL(source)
		if err != nil {
			return nil, err
		}
	}

	data, err := ldb.GetDBForKeyUnsafe(source).RPop(source)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	if _, err := ldb.GetDBForKeyUnsafe([]byte(dest)).LPush(dest, data); err != nil {
		ldb.GetDBForKeyUnsafe([]byte(source)).RPush(source, data) // revert pop
		return nil, err
	}

	// reset ttl
	if ttl != -1 {
		ldb.GetDBForKeyUnsafe([]byte(source)).LExpire(source, ttl)
	}

	return data, nil
}

func cmdLLEN(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LLen([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdLSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	index, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	if err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LSet([]byte(args[1]), int32(index), []byte(args[3])); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func cmdLRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
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

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LRange([]byte(args[1]), int32(start), int32(stop))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdRPOP(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).RPop([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdLPUSH(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	argList := make([][]byte, len(args)-2)
	for i := 2; i < len(args); i++ {
		argList[i-2] = []byte(args[i])
	}
	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LPush([]byte(args[1]), argList...)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

func cmdLINDEX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}
	index, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LIndex([]byte(args[1]), int32(index))
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
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).LPop([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdRPUSH(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	argList := make([][]byte, len(args)-2)
	for i := 2; i < len(args); i++ {
		argList[i-2] = []byte(args[i])
	}
	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).RPush([]byte(args[1]), argList...)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

// 此处危险：如果是raft写指令，则raft会进行指令回滚=>卡住raft，如果是raft读指令，则会因为raft无法回滚队列消费日志，出现队列脏数据
// func cmdBLPOP(m uhaha.Machine, args []string) (interface{}, error) {
// 	if len(args) < 3 {
// 		return nil, rafthub.ErrWrongNumArgs
// 	}

// 	keys, timeout, err := lParseBPopArgs(args)

// 	if err != nil {
// 		return nil, err
// 	}

// 	ay, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).BLPop(keys, timeout)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return ay, nil
// }

func lParseBPopArgs(argsOrigin []string) (keys [][]byte, timeout time.Duration, err error) {
	if len(argsOrigin) < 3 {
		err = rafthub.ErrWrongNumArgs
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
