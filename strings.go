/*
 * @Author: gitsrc
 * @Date: 2021-03-08 17:57:04
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-09 18:35:34
 * @FilePath: /IceFireDB/strings.go
 */

package main

import (
	"log"
	"strconv"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
)

func cmdSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
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

func cmdSETEX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, uhaha.ErrWrongNumArgs
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

func cmdSETNX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.SetNX([]byte(args[1]), []byte(args[2]))

	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

func cmdGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	count, err := ldb.Exists([]byte(args[1]))
	if err != nil || count == 0 {
		return nil, nil
	}
	val, err := ldb.Get([]byte(args[1]))
	log.Println("22222", err)
	ttl, error := ldb.TTL([]byte(args[1]))
	log.Println(ttl, error)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func cmdDEL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	var n int
	for i := 1; i < len(args); i++ {
		ok, err := ldb.Exists([]byte(args[i]))
		if err != nil {
			return nil, err
		}
		if ok > 0 { //if key exist
			_, err := ldb.Del([]byte(args[i]))
			if err != nil {
				return nil, err
			}
			n++
		}
	}
	return redcon.SimpleInt(n), nil
}

func cmdMSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return nil, uhaha.ErrWrongNumArgs
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

func cmdMGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
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

func cmdTTL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	v, err := ldb.TTL([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}
