package main

import (
	"strconv"
	"time"

	"github.com/ledisdb/ledisdb/ledis"

	"github.com/tidwall/uhaha"

	"github.com/tidwall/redcon"
)

func init() {
	conf.AddWriteCommand("APPEND", cmdAPPEND)
	conf.AddReadCommand("BITCOUNT", cmdBITCOUNT)
	conf.AddWriteCommand("BITOP", cmdBITOP)
	conf.AddReadCommand("BITPOS", cmdBITPOS)
	conf.AddWriteCommand("DECR", cmdDECR)
	conf.AddWriteCommand("DECRBY", cmdDECRBY)
	conf.AddWriteCommand("DEL", cmdDEL)
	conf.AddReadCommand("EXISTS", cmdEXISTS)
	conf.AddReadCommand("GET", cmdGET)
	conf.AddReadCommand("GETBIT", cmdGETBIT)
	conf.AddWriteCommand("SETBIT", cmdSETBIT)
	conf.AddReadCommand("GETRANGE", cmdGETRANGE)
	conf.AddWriteCommand("GETSET", cmdGETSET)
	conf.AddWriteCommand("INCR", cmdINCR)
	conf.AddWriteCommand("INCRBY", cmdINCRBY)
	conf.AddReadCommand("MGET", cmdMGET)
	conf.AddWriteCommand("MSET", cmdMSET)
	conf.AddWriteCommand("SET", cmdSET)
	conf.AddWriteCommand("SETNX", cmdSETNX)
	//SETEX => SETEXAT : When the raft node is down and the log is played back, the setex command is still played back, so it needs to be intercepted at the network layer to modify the command
	conf.AddWriteCommand("SETEX", cmdSETEX)
	conf.AddWriteCommand("SETEXAT", cmdSETEXAT)
	conf.AddWriteCommand("SETRANGE", cmdSETRANGE)
	conf.AddReadCommand("STRLEN", cmdSTRLEN)
	conf.AddWriteCommand("EXPIRE", cmdEXPIRE)     //EXPIRE => EXPIREAT
	conf.AddWriteCommand("EXPIREAT", cmdEXPIREAT) // Timeout command
	conf.AddReadCommand("TTL", cmdTTL)
	// conf.AddWriteCommand("PERSIST", cmdPERSIST) //Prohibition: time persistence
}

// cmdEXPIREAT sets an expiration timestamp for a key.
func cmdEXPIREAT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	timestamp, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	// If the timestamp is less than the current time, delete the key
	if timestamp < time.Now().Unix() {
		keys := [][]byte{[]byte(args[1])}
		_, err := ldb.Del(keys...)
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(1), nil
	}

	v, err := ldb.ExpireAt([]byte(args[1]), timestamp)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(v), nil
}

// cmdEXPIRE sets an expiration time for a key.
func cmdEXPIRE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	duration, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}
	timestamp := m.Now().Unix() + duration

	// If the timestamp is less than the current time, delete the key
	if timestamp < time.Now().Unix() {
		keys := [][]byte{[]byte(args[1])}
		_, err := ldb.Del(keys...)
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(1), nil
	}

	v, err := ldb.ExpireAt([]byte(args[1]), timestamp)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(v), nil
}

// cmdSTRLEN returns the length of the string value stored at key.
func cmdSTRLEN(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.StrLen([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

// cmdSETRANGE overwrites part of the string stored at key, starting at the specified offset,
// for the entire length of the value. If the offset is larger than the current length of the string,
// the string is padded with zero-bytes to make offset fit. Non-existing keys are treated as empty strings.
func cmdSETRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	// Check the number of arguments
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	// Parse the key and offset
	key := []byte(args[1])
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	// Parse the value to be set
	value := []byte(args[3])

	// Perform the SetRange operation
	n, err := ldb.SetRange(key, offset, value)
	if err != nil {
		return nil, err
	}

	// Return the length of the string after the operation
	return redcon.SimpleInt(n), nil
}

// cmdINCRBY increments the number stored at key by delta. If the key does not exist,
// it is set to 0 before performing the operation. An error is returned if the key
// contains a value of the wrong type or contains a string that can not be represented
// as an integer.
func cmdINCRBY(m uhaha.Machine, args []string) (interface{}, error) {
	// Check the number of arguments
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	// Parse the delta value
	delta, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	// Perform the IncrBy operation
	n, err := ldb.IncrBy([]byte(args[1]), delta)
	if err != nil {
		return nil, err
	}

	// Return the new value after increment
	return redcon.SimpleInt(n), nil
}

// cmdINCR increments the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. An error is returned if the key
// contains a value of the wrong type or contains a string that can not be represented
// as an integer.
func cmdINCR(m uhaha.Machine, args []string) (interface{}, error) {
	// Check the number of arguments
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	// Perform the Incr operation
	n, err := ldb.Incr([]byte(args[1]))
	if err != nil {
		return nil, err
	}

	// Return the new value after increment
	return redcon.SimpleInt(n), nil
}

// cmdGETSET sets the string value of a key and returns its old value. If the key does not exist,
// it returns nil. An error is returned if the key contains a value of the wrong type.
func cmdGETSET(m uhaha.Machine, args []string) (interface{}, error) {
	// Check the number of arguments
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	// Perform the GetSet operation
	v, err := ldb.GetSet([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}

	// Return the old value
	return v, nil
}

func cmdGETRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	start, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	end, err := strconv.Atoi(args[3])
	if err != nil {
		return nil, err
	}

	v, err := ldb.GetRange(key, start, end)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func cmdSETBIT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := args[1]
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	value, err := strconv.Atoi(args[3])
	if err != nil {
		return nil, err
	}

	n, err := ldb.SetBit([]byte(key), offset, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdGETBIT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	n, err := ldb.GetBit(key, offset)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdEXISTS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	var counter int
	for _, key := range args[1:] {
		n, err := ldb.Exists([]byte(key))
		if err != nil {
			return nil, err
		}

		// exists
		if n > 0 {
			counter++
		}
	}

	return redcon.SimpleInt(counter), nil
}

func cmdDECRBY(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	delta, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	n, err := ldb.DecrBy([]byte(args[1]), delta)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdDECR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.Decr([]byte(args[1]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdBITPOS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	bit, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}
	start, end, err := parseBitRange(args[3:])
	if err != nil {
		return nil, err
	}

	n, err := ldb.BitPos(key, bit, start, end)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil
}

func cmdBITOP(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	op := args[1]
	destKey := args[2]
	// srcKeys := args[3:]

	srcKeys := make([][]byte, len(args)-3)
	for i := 3; i < len(args); i++ {
		srcKeys[i-3] = []byte(args[i])
	}

	n, err := ldb.BitOP(op, []byte(destKey), srcKeys...)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdAPPEND(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.Append([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

func cmdBITCOUNT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 || len(args) > 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	start, end, err := parseBitRange(args[2:])
	if err != nil {
		return nil, err
	}

	n, err := ldb.BitCount(key, start, end)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(n), nil
}

// This is different from the redis standard. It needs to enrich the algorithm to support more atomic instructions.
func cmdSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	if err := ldb.Set([]byte(args[1]), []byte(args[2])); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

// Setex is rewritten as setexat to avoid the exception of raft log playback
func cmdSETEX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	duration, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}
	timestamp := m.Now().Unix() + duration

	//If the timestamp is less than the current time, delete operation
	if timestamp < time.Now().Unix() {
		keys := make([][]byte, 1)
		keys[0] = []byte(args[1])
		_, err := ldb.Del(keys...)
		if err != nil {
			return nil, err
		}
		return redcon.SimpleString("OK"), nil
	}

	if err := ldb.SetEXAT([]byte(args[1]), timestamp, []byte(args[3])); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func cmdSETEXAT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, uhaha.ErrWrongNumArgs
	}
	timestamp, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}

	//If the timestamp is less than the current time, delete operation
	if timestamp < time.Now().Unix() {
		keys := make([][]byte, 1)
		keys[0] = []byte(args[1])
		_, err := ldb.Del(keys...)
		if err != nil {
			return nil, err
		}
		return redcon.SimpleString("OK"), nil
	}

	if err := ldb.SetEXAT([]byte(args[1]), timestamp, []byte(args[3])); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func cmdSETNX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
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
	/*count, err := ldb.Exists([]byte(args[1]))
	if err != nil || count == 0 {
		return nil, nil
	}*/
	val, err := ldb.Get([]byte(args[1]))
	if err != nil {
		return nil, err
	}

	return val, nil
}

// This is different from the redis standard. For the sake of transaction consistency, there is no key existence judgment.
func cmdDEL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	keys := make([][]byte, len(args)-1)

	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
	}

	n, err := ldb.Del(keys...)
	if err != nil {
		return nil, err
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

	keys := make([][]byte, len(args)-1)

	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
	}

	v, err := ldb.MGet(keys...)
	if err != nil {
		return nil, err
	}

	return v, nil
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

func parseBitRange(args []string) (start int, end int, err error) {
	start = 0
	end = -1
	if len(args) > 0 {
		if start, err = strconv.Atoi(args[0]); err != nil {
			return
		}
	}

	if len(args) == 2 {
		if end, err = strconv.Atoi(args[1]); err != nil {
			return
		}
	}
	return
}
