/*
 * @Author: gitsrc
 * @Date: 2021-03-08 18:20:04
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-08-23 18:27:04
 * @FilePath: /IceFireDB/strings.go
 */

package main

import (
	"strconv"
	"time"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/tidwall/uhaha"
	rafthub "github.com/tidwall/uhaha"

	"github.com/tidwall/redcon"
)

func init() {
	conf.AddWriteCommand("APPEND", cmdAPPEND)
	conf.AddReadCommand("BITCOUNT", cmdBITCOUNT)
	conf.AddWriteCommand("BITOP", cmdBITOP)
	conf.AddReadCommand("BITPOS", cmdBITPOS)
	conf.AddWriteCommand("DECR", cmdDECR)
	conf.AddWriteCommand("DECRBY", cmdDECRBY)
	conf.AddWriteCommand("DEL", cmdDEL)
	conf.AddReadCommand("EXISTS", cmdEXISTS)
	conf.AddReadCommand("GET", cmdGET)
	conf.AddReadCommand("GETBIT", cmdGETBIT)
	conf.AddWriteCommand("SETBIT", cmdSETBIT)
	conf.AddReadCommand("GETRANGE", cmdGETRANGE)
	conf.AddWriteCommand("GETSET", cmdGETSET)
	conf.AddWriteCommand("INCR", cmdINCR)
	conf.AddWriteCommand("INCRBY", cmdINCRBY)
	// conf.AddReadCommand("MGET", cmdMGET)
	// conf.AddWriteCommand("MSET", cmdMSET)
	conf.AddWriteCommand("SET", cmdSET)
	conf.AddWriteCommand("SETNX", cmdSETNX)
	// TODO SETEX => SETEXAT : 当raft节点宕机、日志回放时 还是回放了setex指令，所以需要在网络层拦截进行指令修改
	// 由于raft 日志回滚问题，所以推荐大家使用SETEXAT 指令,建议客户端写入时转换为SETEXAT指令
	conf.AddWriteCommand("SETEX", cmdSETEX)
	conf.AddWriteCommand("SETEXAT", cmdSETEXAT)
	conf.AddWriteCommand("SETRANGE", cmdSETRANGE)
	conf.AddReadCommand("STRLEN", cmdSTRLEN)
	// conf.AddWriteCommand("EXPIRE", cmdEXPIRE)     //EXPIRE => EXPIREAT
	conf.AddWriteCommand("EXPIREAT", cmdEXPIREAT) // Timeout command
	conf.AddReadCommand("TTL", cmdTTL)
	// conf.AddWriteCommand("PERSIST", cmdPERSIST) //Prohibition: time persistence
}

// func cmdPERSIST(m uhaha.Machine, args []string) (interface{}, error) {
// 	if len(args) != 2 {
// 		return nil, rafthub.ErrWrongNumArgs
// 	}

// 	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Persist([]byte(args[1]))
// 	if err != nil {
// 		return nil, err
// 	}
// 	return redcon.SimpleInt(n), nil
// }

func cmdEXPIREAT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	when, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	// 如果时间戳小于当前时间，则进行删除操作
	if when < time.Now().Unix() {
		keys := make([][]byte, 1)
		keys[0] = []byte(args[1])
		_, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Del(keys...)
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(1), nil
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ExpireAt([]byte(args[1]), when)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(v), nil
}

// func cmdEXPIRE(m uhaha.Machine, args []string) (interface{}, error) {
// 	if len(args) != 3 {
// 		return nil, rafthub.ErrWrongNumArgs
// 	}

// 	duration, err := ledis.StrInt64([]byte(args[2]), nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Expire([]byte(args[1]), duration)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return redcon.SimpleInt(v), nil
// }

func cmdSTRLEN(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).StrLen([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdSETRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	key := []byte(args[1])
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	value := args[3]

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).SetRange(key, offset, []byte(value))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdINCRBY(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	delta, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).IncrBy([]byte(args[1]), delta)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdINCR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Incr([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdGETSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).GetSet([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdGETRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	key := []byte(args[1])
	start, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	end, err := strconv.Atoi(args[3])
	if err != nil {
		return nil, err
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).GetRange(key, start, end)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdSETBIT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	key := args[1]
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	value, err := strconv.Atoi(args[3])
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).SetBit([]byte(key), offset, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdGETBIT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	key := []byte(args[1])
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).GetBit(key, offset)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

// 此处和redis标准有区别，当前只支持一个key的判断过程
func cmdEXISTS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Exists([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdDECRBY(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	delta, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).DecrBy([]byte(args[1]), delta)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdDECR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Decr([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdBITPOS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	key := []byte(args[1])
	bit, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}
	start, end, err := parseBitRange(args[3:])
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).BitPos(key, bit, start, end)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

func cmdBITOP(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	op := args[1]
	destKey := args[2]
	// srcKeys := args[3:]

	srcKeys := make([][]byte, len(args)-3)
	for i := 3; i < len(args); i++ {
		srcKeys[i-3] = []byte(args[i])
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).BitOP(op, []byte(destKey), srcKeys...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdAPPEND(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Append([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdBITCOUNT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 || len(args) > 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	key := []byte(args[1])
	start, end, err := parseBitRange(args[2:])
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).BitCount(key, start, end)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

// 此处和redis标准有区别,需要丰富算法，支撑更多原子指令
func cmdSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	if err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Set([]byte(args[1]), []byte(args[2])); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

// Setex is rewritten as setexat to avoid the exception of raft log playback
func cmdSETEX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	sec, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().Unix() + sec

	// 如果时间戳小于当前时间，则进行删除操作
	if timestamp < time.Now().Unix() {
		keys := make([][]byte, 1)
		keys[0] = []byte(args[1])
		_, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Del(keys...)
		if err != nil {
			return nil, err
		}
		return redcon.SimpleString("OK"), nil
	}

	if err := ldb.GetDBForKeyUnsafe([]byte(args[1])).SetEXAT([]byte(args[1]), timestamp, []byte(args[3])); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func cmdSETEXAT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}
	timestamp, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	// 如果时间戳小于当前时间，则进行删除操作
	if timestamp < time.Now().Unix() {
		keys := make([][]byte, 1)
		keys[0] = []byte(args[1])
		_, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Del(keys...)
		if err != nil {
			return nil, err
		}
		return redcon.SimpleString("OK"), nil
	}

	if err := ldb.GetDBForKeyUnsafe([]byte(args[1])).SetEXAT([]byte(args[1]), timestamp, []byte(args[3])); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func cmdSETNX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).SetNX([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

func cmdGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}
	/*count, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Exists([]byte(args[1]))
	if err != nil || count == 0 {
		return nil, nil
	}*/
	val, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).Get([]byte(args[1]))
	if err != nil {
		return nil, err
	}

	return val, nil
}

// 此处和redis标准有区别，为了事务一致性考虑，没有进行key存在判断
func cmdDEL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	keys := make([][]byte, len(args)-1)
	var count int64 = 0
	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
		n, err := ldb.GetDBForKeyUnsafe([]byte(args[i])).Del([]byte(args[i]))
		if err != nil {
			return nil, err
		}
		count += n
	}

	return redcon.SimpleInt(count), nil
}

func cmdMSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return nil, rafthub.ErrWrongNumArgs
	}

	kvPairCount := (len(args) - 1) / 2
	batch := make([]ledis.KVPair, kvPairCount)
	loopI := 0
	for i := 1; i < len(args); i += 2 {
		batch[loopI].Key = []byte(args[i])
		batch[loopI].Value = []byte(args[i+1])
		loopI++
	}

	if err := ldb.GetDBForKeyUnsafe([]byte(args[1])).MSet(batch...); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func cmdMGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	keys := make([][]byte, len(args)-1)

	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).MGet(keys...)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func cmdTTL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).TTL([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func parseBitRange(args []string) (start int, end int, err error) {
	start = 0
	end = -1
	if len(args) > 0 {
		if start, err = strconv.Atoi(args[0]); err != nil {
			return
		}
	}

	if len(args) == 2 {
		if end, err = strconv.Atoi(args[1]); err != nil {
			return
		}
	}
	return
}
