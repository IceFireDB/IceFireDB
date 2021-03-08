package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
)

func cmdHSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 || (len(args))%2 != 0 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n := 0
	for i := 2; i < len(args); i += 2 {
		if _, err := ldb.HSet([]byte(args[1]), []byte(args[i]), []byte(args[i+1])); err == nil {
			n++
		}
	}

	return redcon.SimpleInt(n), nil
}

func cmdHGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
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

//HDEL key field [field ...]
func cmdHDEL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
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
