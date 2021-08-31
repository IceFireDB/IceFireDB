package main

import (
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
)

func init() {
	conf.AddReadCommand("INFO", cmdINFO)
	conf.AddWriteCommand("FLUSHALL", cmdFLUSHALL)
}

func cmdINFO(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, uhaha.ErrWrongNumArgs
	}
	return serverInfo.Dump(""), nil
}

func cmdFLUSHALL(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, uhaha.ErrWrongNumArgs
	}
	n, err := ldb.FlushAll()
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}
