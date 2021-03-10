/*
 * @Author: gitsrc
 * @Date: 2021-03-08 17:57:04
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-10 16:09:23
 * @FilePath: /IceFireDB/strings.go
 */

package main

import (
	"strconv"

	"github.com/gitsrc/IceFireDB/rafthub"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/syndtr/goleveldb/leveldb"

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

	conf.AddWriteCommand("SET", cmdSET)
	conf.AddWriteCommand("SETEX", cmdSETEX)
	conf.AddWriteCommand("SETNX", cmdSETNX)
	conf.AddWriteCommand("MSET", cmdMSET)

	conf.AddReadCommand("TTL", cmdTTL)
	conf.AddReadCommand("MGET", cmdMGET)
	//conf.AddReadCommand("KEYS", cmdKEYS)

}

func cmdGETRANGE(m rafthub.Machine, args []string) (interface{}, error) {
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

	v, err := ldb.GetRange(key, start, end)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdSETBIT(m rafthub.Machine, args []string) (interface{}, error) {
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

	n, err := ldb.SetBit([]byte(key), offset, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdGETBIT(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	key := []byte(args[1])
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetBit(key, offset)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

//此处和redis标准有区别，当前只支持一个key的判断过程
func cmdEXISTS(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.Exists([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdDECRBY(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	delta, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	n, err := ldb.DecrBy([]byte(args[1]), delta)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdDECR(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.Decr([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdBITPOS(m rafthub.Machine, args []string) (interface{}, error) {
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

	n, err := ldb.BitPos(key, bit, start, end)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

func cmdBITOP(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	op := args[1]
	destKey := args[2]
	//srcKeys := args[3:]

	srcKeys := make([][]byte, len(args)-3)
	for i := 3; i < len(args); i++ {
		srcKeys[i-3] = []byte(args[i])
	}

	n, err := ldb.BitOP(op, []byte(destKey), srcKeys...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdAPPEND(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.Append([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdBITCOUNT(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 2 || len(args) > 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	key := []byte(args[1])
	start, end, err := parseBitRange(args[2:])
	if err != nil {
		return nil, err
	}

	n, err := ldb.BitCount(key, start, end)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdSET(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	//Direct processing for simple SET KEY VALUE commands
	// if len(args) == 3 {
	// 	if err := ldb.Set([]byte(args[1]), []byte(args[2])); err != nil {
	// 		return nil, err
	// 	}

	// 	return redcon.SimpleString("OK"), nil
	// }

	if err := ldb.Set([]byte(args[1]), []byte(args[2])); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func cmdSETEX(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}
	ttl, err := strconv.Atoi(args[3])

	if err != nil {
		return nil, err
	}

	if err := ldb.SetEX([]byte(args[1]), int64(ttl), []byte(args[2])); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func cmdSETNX(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.SetNX([]byte(args[1]), []byte(args[2]))

	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

func cmdGET(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}
	count, err := ldb.Exists([]byte(args[1]))
	if err != nil || count == 0 {
		return nil, nil
	}
	val, err := ldb.Get([]byte(args[1]))

	if err != nil {
		return nil, err
	}

	return val, nil
}

//此处和redis标准有区别，为了事务一致性考虑，没有进行key存在判断
func cmdDEL(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	keys := make([][]byte, len(args)-1)

	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
	}

	n, err := ldb.Del(keys...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdMSET(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return nil, rafthub.ErrWrongNumArgs
	}

	kvPairCount := (len(args) - 1) / 2
	batch := make([]ledis.KVPair, kvPairCount)
	loopI := 0
	for i := 1; i < len(args); i += 2 {

		//batch.Put([]byte(args[i]), []byte(args[i+1]))
		batch[loopI].Key = []byte(args[i])
		batch[loopI].Value = []byte(args[i+1])
		loopI++
	}

	if err := ldb.MSet(batch...); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func cmdMGET(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}
	var vals []interface{}
	for i := 1; i < len(args); i++ {
		val, err := ldb.Get([]byte(args[i]))
		if err != nil {
			if err == leveldb.ErrNotFound {
				vals = append(vals, nil)
				continue
			}
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, nil
}

func cmdTTL(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.TTL([]byte(args[1]))
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
