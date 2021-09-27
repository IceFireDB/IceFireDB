package main

import (
	"fmt"
	"testing"
)

var data = []byte{0,3,113,113,113,6,0,245,178,245,78,19,104,128,126}

func TestMigrate(t *testing.T) {
	var err error
	_ = getTestConn()
	conn, err := getMigrateDBConn("127.0.0.1:11001")
	if err != nil {
		panic(err)
	}
	if res, err := conn.Do("restore", "uuu", 144*1e3, data); err != nil {
		panic(err)
	} else {
		fmt.Println(res)
	}
}
