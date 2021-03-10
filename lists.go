/*
 * @Author: gitsrc
 * @Date: 2021-03-08 17:57:04
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-10 17:46:01
 * @FilePath: /IceFireDB/lists.go
 */

package main

import (
	"strconv"
	"time"

	"github.com/gitsrc/IceFireDB/rafthub"
	"github.com/siddontang/go/hack"
)

func init() {
	conf.AddWriteCommand("BLPOP", cmdBLPOP)

}
func cmdBLPOP(m rafthub.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	keys, timeout, err := lParseBPopArgs(args)

	if err != nil {
		return nil, err
	}

	ay, err := ldb.BLPop(keys, timeout)
	if err != nil {
		return nil, err
	}
	return ay, nil
}

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
