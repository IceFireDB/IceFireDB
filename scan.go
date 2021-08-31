package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/uhaha"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/siddontang/go/num"
)

var (
	xScanGroup = scanCommandGroup{nilCursorLedis, parseXScanArgs}
	scanGroup  = scanCommandGroup{nilCursorRedis, parseScanArgs}
)

var (
	nilCursorLedis = []byte("")
	nilCursorRedis = []byte("0")
)

func init() {
	conf.AddReadCommand("HSCAN", scanGroup.cmdXHSCAN)
	conf.AddReadCommand("SSCAN", scanGroup.cmdXSSCAN)
	conf.AddReadCommand("ZSCAN", scanGroup.cmdXZSCAN)
	conf.AddReadCommand("XSCAN", xScanGroup.cmdXSCAN)
	conf.AddReadCommand("XHSCAN", xScanGroup.cmdXHSCAN)
	conf.AddReadCommand("XSSCAN", xScanGroup.cmdXSSCAN)
	conf.AddReadCommand("XZSCAN", xScanGroup.cmdXZSCAN)
}

func parseXScanArgs(args []string) (cursor []byte, match string, count int, desc bool, err error) {
	cursor = []byte(args[0])
	args = args[1:]
	count = 10
	desc = false
	for i := 0; i < len(args); {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			if i+1 >= len(args) {
				err = uhaha.ErrInvalid
				return
			}
			match = args[i+1]
			i++
		case "COUNT":
			if i+1 >= len(args) {
				err = uhaha.ErrInvalid
				return
			}
			count, err = strconv.Atoi(args[i+1])
			if err != nil {
				err = uhaha.ErrInvalid
				return
			}
			i++
		case "ASC":
			desc = false
		case "DESC":
			desc = true
		default:
			err = fmt.Errorf("invalid argument %s", args[i])
			return
		}
		i++
	}
	return
}

func parseScanArgs(args []string) (cursor []byte, match string, count int, desc bool, err error) {
	cursor, match, count, desc, err = parseXScanArgs(args)
	if bytes.Compare(cursor, nilCursorRedis) == 0 {
		cursor = nilCursorLedis
	}
	return
}

type scanCommandGroup struct {
	lastCursor []byte
	parseArgs  func(args []string) (cursor []byte, match string, count int, desc bool, err error)
}

// XSCAN type cursor [MATCH match] [COUNT count] [ASC|DESC]
func (scg scanCommandGroup) cmdXSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	var dataType ledis.DataType
	switch strings.ToUpper(args[1]) {
	case "KV":
		dataType = ledis.KV
	case "HASH":
		dataType = ledis.HASH
	case "LIST":
		dataType = ledis.LIST
	case "SET":
		dataType = ledis.SET
	case "ZSET":
		dataType = ledis.ZSET
	default:
		return nil, fmt.Errorf("invalid key type %s", args[1])
	}

	cursor, match, count, desc, err := scg.parseArgs(args[2:])
	if err != nil {
		return nil, err
	}

	var ay [][]byte
	if !desc {
		ay, err = ldb.Scan(dataType, cursor, count, false, match)
	} else {
		ay, err = ldb.RevScan(dataType, cursor, count, false, match)
	}
	if err != nil {
		return nil, err
	}

	data := make([]interface{}, 2)
	if len(ay) < count {
		data[0] = scg.lastCursor
	} else {
		data[0] = ay[len(ay)-1]
	}
	data[1] = ay
	return data, nil
}

// XHSCAN key cursor [MATCH match] [COUNT count] [ASC|DESC]
func (scg scanCommandGroup) cmdXHSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}
	key := []byte(args[1])
	cursor, match, count, desc, err := scg.parseArgs(args[2:])
	if err != nil {
		return nil, err
	}

	var ay []ledis.FVPair
	if !desc {
		ay, err = ldb.HScan(key, cursor, count, false, match)
	} else {
		ay, err = ldb.HRevScan(key, cursor, count, false, match)
	}

	if err != nil {
		return nil, err
	}

	data := make([]interface{}, 2)
	if len(ay) < count {
		data[0] = scg.lastCursor
	} else {
		data[0] = ay[len(ay)-1].Field
	}

	vv := make([][]byte, 0, len(ay)*2)
	for _, v := range ay {
		vv = append(vv, v.Field, v.Value)
	}

	data[1] = vv
	return data, nil
}

// XSSCAN key cursor [MATCH match] [COUNT count] [ASC|DESC]
func (scg scanCommandGroup) cmdXSSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	cursor, match, count, desc, err := scg.parseArgs(args[2:])
	if err != nil {
		return nil, err
	}

	var ay [][]byte
	if !desc {
		ay, err = ldb.SScan(key, cursor, count, false, match)
	} else {
		ay, err = ldb.SRevScan(key, cursor, count, false, match)
	}

	if err != nil {
		return nil, err
	}

	data := make([]interface{}, 2)
	if len(ay) < count {
		data[0] = scg.lastCursor
	} else {
		data[0] = ay[len(ay)-1]
	}

	data[1] = ay

	return data, nil
}

// XZSCAN key cursor [MATCH match] [COUNT count] [ASC|DESC]
func (scg scanCommandGroup) cmdXZSCAN(_ uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	cursor, match, count, desc, err := scg.parseArgs(args[2:])
	if err != nil {
		return nil, err
	}
	var ay []ledis.ScorePair
	if !desc {
		ay, err = ldb.ZScan(key, cursor, count, false, match)
	} else {
		ay, err = ldb.ZRevScan(key, cursor, count, false, match)
	}
	if err != nil {
		return nil, err
	}

	data := make([]interface{}, 2)
	if len(ay) < count {
		data[0] = scg.lastCursor
	} else {
		data[0] = ay[len(ay)-1].Member
	}

	vv := make([][]byte, 0, len(ay)*2)
	for _, v := range ay {
		vv = append(vv, v.Member, num.FormatInt64ToSlice(v.Score))
	}
	data[1] = vv
	return data, nil
}
