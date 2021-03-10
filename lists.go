/*
 * @Author: gitsrc
 * @Date: 2021-03-08 17:57:04
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-10 19:44:29
 * @FilePath: /IceFireDB/lists.go
 */

package main

import (
	"bytes"
	"strconv"
	"time"

	"github.com/gitsrc/IceFireDB/rafthub"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/siddontang/go/hack"
	"github.com/tidwall/redcon"
)

func init() {
	//队列的block类型指令均需要避免，危险操作
	//conf.AddReadCommand("BLPOP", cmdBLPOP) //此处危险：如果是raft写指令，则raft会进行指令回滚=>卡住raft，如果是raft读指令，则会因为raft无法回滚队列消费日志，出现队列脏数据
	conf.AddWriteCommand("RPUSH", cmdRPUSH)
	conf.AddWriteCommand("LPOP", cmdLPOP)
	conf.AddReadCommand("LINDEX", cmdLINDEX)
	conf.AddWriteCommand("LPUSH", cmdLPUSH)
	conf.AddWriteCommand("RPOP", cmdRPOP)
	conf.AddReadCommand("LRANGE", cmdLRANGE)
	conf.AddWriteCommand("LSET", cmdLSET)
	conf.AddReadCommand("LLEN", cmdLLEN)
	conf.AddWriteCommand("RPOPLPUSH", cmdRPOPLPUSH)

	//IceFireDB special command
	conf.AddWriteCommand("LCLEAR", cmdLCLEAR)
}

func cmdLCLEAR(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.LClear([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdRPOPLPUSH(m rafthub.Machine, args []string) (interface{}, error) {

	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}
	source, dest := []byte(args[1]), []byte(args[2])

	var ttl int64 = -1
	if bytes.Compare(source, dest) == 0 {
		var err error
		ttl, err = ldb.LTTL(source)
		if err != nil {
			return nil, err
		}
	}

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

	//reset ttl
	if ttl != -1 {
		ldb.LExpire(source, ttl)
	}

	return data, nil
}

func cmdLLEN(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.LLen([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdLSET(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
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

func cmdLRANGE(m rafthub.Machine, args []string) (interface{}, error) {
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

	v, err := ldb.LRange([]byte(args[1]), int32(start), int32(stop))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdRPOP(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.RPop([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdLPUSH(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
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

func cmdLINDEX(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}
	index, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	v, err := ldb.LIndex([]byte(args[1]), int32(index))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdLPOP(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.LPop([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdRPUSH(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
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

//此处危险：如果是raft写指令，则raft会进行指令回滚=>卡住raft，如果是raft读指令，则会因为raft无法回滚队列消费日志，出现队列脏数据
// func cmdBLPOP(m rafthub.Machine, args []string) (interface{}, error) {
// 	if len(args) < 3 {
// 		return nil, rafthub.ErrWrongNumArgs
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
