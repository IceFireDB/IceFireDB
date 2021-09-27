/*
 * @Author: gitsrc
 * @Date: 2021-08-23 18:12:43
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-08-23 18:26:42
 * @FilePath: /IceFireDB/sorted_sets.go
 */

package main

import (
	"math"
	"strconv"
	"strings"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/siddontang/go/hack"
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
	conf.AddReadCommand("ZREVRANGE", cmdZREVRANGE)
	conf.AddReadCommand("ZSCORE", cmdZSCORE)
	conf.AddReadCommand("ZINCRBY", cmdZINCRBY)
	conf.AddReadCommand("ZREVRANK", cmdZREVRANK)
	conf.AddReadCommand("ZRANGEBYSCORE", cmdZRANGEBYSCORE)
	conf.AddReadCommand("ZREVRANGEBYSCORE", cmdZREVRANGEBYSCORE)
	conf.AddReadCommand("ZREMRANGEBYSCORE", cmdZREMRANGEBYSCORE)
	conf.AddReadCommand("ZREMRANGEBYRANK", cmdZREMRANGEBYRANK)
}

func zparseRange(a1 string, a2 string) (start int, stop int, err error) {
	if start, err = strconv.Atoi(a1); err != nil {
		return
	}
	if stop, err = strconv.Atoi(a2); err != nil {
		return
	}
	return
}

func zparseScoreRange(minBuf []byte, maxBuf []byte) (min int64, max int64, err error) {
	if strings.ToLower(hack.String(minBuf)) == "-inf" {
		min = math.MinInt64
	} else {

		if len(minBuf) == 0 {
			err = uhaha.ErrWrongNumArgs
			return
		}

		var lopen bool = false
		if minBuf[0] == '(' {
			lopen = true
			minBuf = minBuf[1:]
		}

		min, err = ledis.StrInt64(minBuf, nil)
		if err != nil {
			err = uhaha.ErrInvalid
			return
		}

		if min <= ledis.MinScore || min >= ledis.MaxScore {
			err = uhaha.ErrWrongNumArgs
			return
		}

		if lopen {
			min++
		}
	}

	if strings.ToLower(hack.String(maxBuf)) == "+inf" {
		max = math.MaxInt64
	} else {
		ropen := false

		if len(maxBuf) == 0 {
			err = uhaha.ErrWrongNumArgs
			return
		}
		if maxBuf[0] == '(' {
			ropen = true
			maxBuf = maxBuf[1:]
		}

		if maxBuf[0] == '(' {
			ropen = true
			maxBuf = maxBuf[1:]
		}

		max, err = ledis.StrInt64(maxBuf, nil)
		if err != nil {
			err = uhaha.ErrWrongNumArgs
			return
		}

		if max <= ledis.MinScore || max >= ledis.MaxScore {
			err = uhaha.ErrWrongNumArgs
			return
		}

		if ropen {
			max--
		}
	}

	return
}

func cmdZCOUNT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	min, max, err := zparseScoreRange([]byte(args[2]), []byte(args[3]))
	if err != nil {
		return nil, err
	}

	count, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZCount([]byte(args[1]), int64(min), int64(max))
	if err != nil {
		return nil, err
	}

	return count, nil
}

func cmdZCARD(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZCard([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdZRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	min, max, err := zparseRange(args[2], args[3])
	if err != nil {
		return nil, err
	}

	ScorePair, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZRange([]byte(args[1]), int(min), int(max))
	if err != nil {
		return nil, err
	}

	var withScores bool
	args = args[4:]
	if len(args) > 0 {
		if len(args) != 1 {
			return nil, rafthub.ErrWrongNumArgs
		}
		if strings.ToLower(args[0]) == "withscores" {
			withScores = true
		} else {
			return nil, uhaha.ErrSyntax
		}
	}

	if withScores {
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

func cmdZREVRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	min, max, err := zparseRange(args[2], args[3])
	if err != nil {
		return nil, err
	}

	ScorePair, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZRevRange([]byte(args[1]), int(min), int(max))
	if err != nil {
		return nil, err
	}

	var withScores bool
	args = args[4:]
	if len(args) > 0 {
		if len(args) != 1 {
			return nil, rafthub.ErrWrongNumArgs
		}
		if strings.ToLower(args[0]) == "withscores" {
			withScores = true
		} else {
			return nil, uhaha.ErrSyntax
		}
	}

	if withScores {
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

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZRank([]byte(args[1]), []byte(args[2]))
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

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZClear([]byte(args[1]))
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

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZRem([]byte(args[1]), members...)
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

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZAdd([]byte(args[1]), ScorePair...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdZSCORE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZScore([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdZINCRBY(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	delta, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZIncrBy([]byte(args[1]), int64(delta), []byte(args[3]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdZREVRANK(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, rafthub.ErrWrongNumArgs
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZRevRank([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return nil, nil
	}

	return redcon.SimpleInt(n), nil
}

func cmdZRANGEBYSCORE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	min, max, err := zparseScoreRange([]byte(args[2]), []byte(args[3]))
	if err != nil {
		return nil, err
	}

	key := args[1]

	args = args[4:]
	var withScores bool
	if len(args) > 0 {
		if strings.ToLower(args[0]) == "withscores" {
			withScores = true
			args = args[1:]
		}
	}

	var offset int
	count := -1
	if len(args) > 0 {
		if len(args) != 3 {
			return nil, uhaha.ErrWrongNumArgs
		}

		if strings.ToLower(args[0]) != "limit" {
			return nil, uhaha.ErrWrongNumArgs
		}

		if offset, err = strconv.Atoi(args[1]); err != nil {
			return nil, uhaha.ErrWrongNumArgs
		}

		if count, err = strconv.Atoi(args[2]); err != nil {
			return nil, uhaha.ErrWrongNumArgs
		}
	}

	if offset < 0 {
		// for ledis, if offset < 0, a empty will return
		// so here we directly return a empty array
		return [][]byte{}, nil
	}

	scorePair, err := ldb.GetDBForKeyUnsafe([]byte(key)).ZRangeByScore([]byte(key), min, max, offset, count)
	if err != nil {
		return nil, err
	}

	if withScores {
		ret := make([][]byte, len(scorePair)*2)
		for index, item := range scorePair {
			ret[index*2] = item.Member
			ret[index*2+1] = []byte(strconv.Itoa(int(item.Score)))
		}
		return ret, nil
	}

	ret := make([][]byte, len(scorePair))
	for index, item := range scorePair {
		ret[index] = item.Member
	}

	return ret, nil
}

func cmdZREVRANGEBYSCORE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	min, max, err := zparseScoreRange([]byte(args[3]), []byte(args[2]))
	if err != nil {
		return nil, err
	}

	key := args[1]

	args = args[4:]
	var withScores bool
	if len(args) > 0 {
		if strings.ToLower(args[0]) == "withscores" {
			withScores = true
			args = args[1:]
		}
	}

	var offset int
	count := -1
	if len(args) > 0 {
		if len(args) != 3 {
			return nil, uhaha.ErrWrongNumArgs
		}

		if strings.ToLower(args[0]) != "limit" {
			return nil, uhaha.ErrWrongNumArgs
		}

		if offset, err = strconv.Atoi(args[1]); err != nil {
			return nil, uhaha.ErrWrongNumArgs
		}

		if count, err = strconv.Atoi(args[2]); err != nil {
			return nil, uhaha.ErrWrongNumArgs
		}
	}

	if offset < 0 {
		// for ledis, if offset < 0, a empty will return
		// so here we directly return a empty array
		return [][]byte{}, nil
	}

	scorePair, err := ldb.GetDBForKeyUnsafe([]byte(key)).ZRangeByScoreGeneric([]byte(key), min, max, offset, count, true)
	if err != nil {
		return nil, err
	}

	if withScores {
		ret := make([][]byte, len(scorePair)*2)
		for index, item := range scorePair {
			ret[index*2] = item.Member
			ret[index*2+1] = []byte(strconv.Itoa(int(item.Score)))
		}
		return ret, nil
	}

	ret := make([][]byte, len(scorePair))
	for index, item := range scorePair {
		ret[index] = item.Member
	}

	return ret, nil
}

func cmdZREMRANGEBYSCORE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	min, max, err := zparseScoreRange([]byte(args[2]), []byte(args[3]))
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZRemRangeByScore([]byte(args[1]), min, max)
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return nil, nil
	}

	return redcon.SimpleInt(n), nil
}

func cmdZREMRANGEBYRANK(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, rafthub.ErrWrongNumArgs
	}

	start, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	stop, err := strconv.Atoi(args[3])
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).ZRemRangeByRank([]byte(args[1]), start, stop)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}
