/*
 * @Author: gitsrc
 * @Date: 2021-08-23 18:12:43
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-08-23 18:26:42
 * @FilePath: /IceFireDB/sorted_sets.go
 */

package main

import (
	"strconv"
	"strings"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
	rafthub "github.com/tidwall/uhaha"
)

func init() {
	conf.AddWriteCommand("ZADD", cmdZADD)
	conf.AddWriteCommand("ZREM", cmdZREM)
	conf.AddWriteCommand("ZCLEAR", cmdZCLEAR)

	// Read command
	conf.AddReadCommand("ZCARD", cmdZCARD)
	conf.AddReadCommand("ZCOUNT", cmdZCOUNT)
	conf.AddReadCommand("ZRANK", cmdZRANK)
	conf.AddReadCommand("ZRANGE", cmdZRANGE)
}

func cmdZCOUNT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	min, err := strconv.Atoi(string(args[2]))

	if err != nil {
		return nil, err
	}

	max, err := strconv.Atoi(string(args[3]))

	if err != nil {
		return nil, err
	}

	count, err := ldb.ZCount([]byte(args[1]), int64(min), int64(max))

	if err != nil {
		return nil, err
	}

	return count, nil
}

func cmdZCARD(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.ZCard([]byte(args[1]))

	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdZRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	min, err := strconv.Atoi(string(args[2]))

	if err != nil {
		return nil, err
	}

	max, err := strconv.Atoi(string(args[3]))

	if err != nil {
		return nil, err
	}

	ScorePair, err := ldb.ZRange([]byte(args[1]), min, max)

	if err != nil {
		return nil, err
	}

	//WITHSCORES
	if strings.ToUpper(args[len(args)-1]) == "WITHSCORES" {
		ret := make([][]byte, len(ScorePair)*2)

		for index, item := range ScorePair {
			ret[index*2] = item.Member
			ret[index*2+1] = []byte(strconv.Itoa(int(item.Score)))
		}
		return ret, nil
	}

	ret := make([][]byte, len(ScorePair))

	for index, item := range ScorePair {
		ret[index] = item.Member
	}

	return ret, nil
}

func cmdZRANK(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.ZRank([]byte(args[1]), []byte(args[2]))

	if err != nil {
		return nil, err
	}
	if n == -1 {
		return nil, nil
	}
	return redcon.SimpleInt(n), nil
}

func cmdZCLEAR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.ZClear([]byte(args[1]))

	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdZREM(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	members := make([][]byte, len(args)-2)

	for i := 2; i < len(args); i++ {
		members[i-2] = []byte(args[i])
	}

	n, err := ldb.ZRem([]byte(args[1]), members...)

	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdZADD(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	ScorePair := make([]ledis.ScorePair, (len(args)-2)/2)

	index := 0
	for i := 2; i < len(args); i += 2 {
		if score, err := strconv.Atoi(string(args[i])); err == nil {
			ScorePair[index].Score = int64(score)
			ScorePair[index].Member = []byte(args[i+1])
			index++
		}
	}

	n, err := ldb.ZAdd([]byte(args[1]), ScorePair...)

	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}
