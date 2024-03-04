package main

import (
	"time"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
	rafthub "github.com/tidwall/uhaha"
)

func init() {
	conf.AddWriteCommand("HSET", cmdHSET)
	conf.AddReadCommand("HGET", cmdHGET)
	conf.AddWriteCommand("HDEL", cmdHDEL)
	conf.AddReadCommand("HEXISTS", cmdHEXISTS)
	conf.AddReadCommand("HGETALL", cmdHGETALL)
	conf.AddWriteCommand("HINCRBY", cmdHINCRBY)
	conf.AddReadCommand("HKEYS", cmdHKEYS)
	conf.AddReadCommand("HLEN", cmdHLEN)
	conf.AddReadCommand("HMGET", cmdHMGET)
	conf.AddWriteCommand("HMSET", cmdHMSET)
	conf.AddWriteCommand("HSETNX", cmdHSETNX)
	conf.AddReadCommand("HSTRLEN", cmdHSTRLEN)
	conf.AddReadCommand("HVALS", cmdHVALS)

	//IceFireDB special command
	conf.AddWriteCommand("HCLEAR", cmdHCLEAR)
	conf.AddWriteCommand("HMCLEAR", cmdHMCLEAR)
	conf.AddWriteCommand("HEXPIRE", cmdHEXPIRE)     //Timeout command HEXPIRE => HEXPIREAT
	conf.AddWriteCommand("HEXPIREAT", cmdHEXPIREAT) //Timeout command
	conf.AddReadCommand("HTTL", cmdHTTL)
	// conf.AddWriteCommand("HPERSIST", cmdHPERSIST)
	conf.AddReadCommand("HKEYEXISTS", cmdHKEYEXISTS)

}

func cmdHSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 || (len(args))%2 != 0 {
		return nil, rafthub.ErrWrongNumArgs
	}

	var n int64
	for i := 2; i < len(args); i += 2 {
		if r, err := ldb.HSet([]byte(args[1]), []byte(args[i]), []byte(args[i+1])); err == nil {
			n = n + r
		}
	}

	return redcon.SimpleInt(n), nil
}

func cmdHGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}
	val, err := ldb.HGet([]byte(args[1]), []byte(args[2]))

	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	if len(val) == 0 {
		return nil, nil
	}

	return redcon.SimpleString(val), nil
}

// HDEL key field [field ...]
func cmdHDEL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	var n int64
	argsData := make([][]byte, len(args)-2)
	for i := 2; i < len(args); i++ {
		argsData[i-2] = []byte(args[i])
	}

	if count, err := ldb.HDel([]byte(args[1]), argsData...); err == nil {
		n = count
	}

	return redcon.SimpleInt(n), nil
}

// HEXISTS key field
func cmdHEXISTS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	var n int64 = 1
	v, err := ldb.HGet([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}
	if v == nil {
		n = 0
	}

	return redcon.SimpleInt(n), nil
}

func cmdHGETALL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.HGetAll([]byte(args[1]))
	if err != nil {
		return nil, err
	}

	dataMap := make(map[string]string)
	for _, kv := range v {
		dataMap[string(kv.Field)] = string(kv.Value)
	}

	return dataMap, nil
}

func cmdHINCRBY(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	delta, err := ledis.StrInt64([]byte(args[3]), nil)
	if err != nil {
		return nil, err
	}

	n, err := ldb.HIncrBy([]byte(args[1]), []byte(args[2]), delta)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdHKEYS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.HKeys([]byte(args[1]))
	if err != nil {
		return nil, err
	}

	return v, nil
}

func cmdHLEN(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.HLen([]byte(args[1]))
	if err != nil {
		return nil, err
	}

	return n, nil
}

func cmdHMGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	keys := make([][]byte, len(args)-2)

	for i := 2; i < len(args); i++ {
		keys[i-2] = []byte(args[i])
	}

	v, err := ldb.HMget([]byte(args[1]), keys...)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func cmdHMSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	if len(args[2:])%2 != 0 {
		return nil, rafthub.ErrWrongNumArgs
	}

	key := args[1]

	args = args[2:]

	kvs := make([]ledis.FVPair, len(args)/2)
	for i := 0; i < len(kvs); i++ {
		kvs[i].Field = []byte(args[2*i])
		kvs[i].Value = []byte(args[2*i+1])
	}

	if err := ldb.HMset([]byte(key), kvs...); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func cmdHSETNX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	var n int64 = 1
	v, err := ldb.HGet([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}
	if v != nil { //Data exists, return zero
		n = 0
		return redcon.SimpleInt(n), nil
	}

	n, err = ldb.HSet([]byte(args[1]), []byte(args[2]), []byte(args[3]))
	if err != nil {
		return nil, err
	}

	return n, nil
}

func cmdHSTRLEN(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	val, err := ldb.HGet([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(len(val)), nil
}

func cmdHVALS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.HValues([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdHCLEAR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.HClear([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdHMCLEAR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	keys := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
	}
	n, err := ldb.HMclear(keys...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdHEXPIRE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	duration, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	timestamp := m.Now().Unix() + duration
	//If the timestamp is less than the current time, delete operation
	if timestamp < time.Now().Unix() {
		_, err := ldb.HClear([]byte(args[1]))
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(0), nil
	}

	v, err := ldb.HExpireAt([]byte(args[1]), timestamp)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func cmdHEXPIREAT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	timestamp, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	//If the timestamp is less than the current time, delete operation
	if timestamp < time.Now().Unix() {
		_, err := ldb.HClear([]byte(args[1]))
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(0), nil
	}

	v, err := ldb.HExpireAt([]byte(args[1]), timestamp)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func cmdHTTL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	v, err := ldb.HTTL([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

func cmdHPERSIST(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.HPersist([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdHKEYEXISTS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.HKeyExists([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}
