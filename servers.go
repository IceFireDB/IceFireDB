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
	var count int64 = 0
	for i := 0; i < slotNum; i++ {
		db, err := ldb.GetDB(i)
		if err != nil {
			return nil, err
		}
		n, err := db.FlushAll()
		if err != nil {
			return nil, err
		}
		count += n
	}
	return redcon.SimpleInt(count), nil
}

//func cmdSELECT(_ uhaha.Machine, args []string) (interface{}, error) {
//	if len(args) != 2 {
//		return nil, uhaha.ErrWrongNumArgs
//	}
//	switch db, err := strconv.Atoi(string(args[1])); {
//	case err != nil:
//		return nil, errors.New("ERR invalid DB index")
//	case db < 0 || db >= int(128):
//	//case db < 0 || db >= int(s.config.BackendNumberDatabases):
//		//return nil, fmt.Errorf("ERR invalid DB index, only accept DB [0,%d)", s.config.BackendNumberDatabases)
//		return nil, fmt.Errorf("ERR invalid DB index, only accept DB [0,%d)", 128)
//	default:
//		// todo ok & 切db
//		//r.Resp = RespOK
//		//s.database = int32(db)
//		return nil, err
//	}
//}
