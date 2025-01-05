package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ledisdb/ledisdb/ledis"

	"github.com/tidwall/uhaha"

	"github.com/tidwall/redcon"
)

func init() {
	// Read Commands
	conf.AddReadCommand("BITCOUNT", cmdBITCOUNT)
	conf.AddReadCommand("BITPOS", cmdBITPOS)
	conf.AddReadCommand("EXISTS", cmdEXISTS)
	conf.AddReadCommand("GET", cmdGET)
	conf.AddReadCommand("GETBIT", cmdGETBIT)
	conf.AddReadCommand("GETRANGE", cmdGETRANGE)
	conf.AddReadCommand("MGET", cmdMGET)
	conf.AddReadCommand("STRLEN", cmdSTRLEN)
	conf.AddReadCommand("TTL", cmdTTL)

	// Write Commands
	conf.AddWriteCommand("APPEND", cmdAPPEND)
	conf.AddWriteCommand("BITOP", cmdBITOP)
	conf.AddWriteCommand("DECR", cmdDECR)
	conf.AddWriteCommand("DECRBY", cmdDECRBY)
	conf.AddWriteCommand("DEL", cmdDEL)
	conf.AddWriteCommand("INCR", cmdINCR)
	conf.AddWriteCommand("INCRBY", cmdINCRBY)
	conf.AddWriteCommand("MSET", cmdMSET)
	conf.AddWriteCommand("SET", cmdSET)
	conf.AddWriteCommand("SETBIT", cmdSETBIT)
	conf.AddWriteCommand("GETSET", cmdGETSET)
	conf.AddWriteCommand("SETNX", cmdSETNX)
	conf.AddWriteCommand("SETEX", cmdSETEX)
	conf.AddWriteCommand("SETEXAT", cmdSETEXAT)
	conf.AddWriteCommand("SETRANGE", cmdSETRANGE)
	conf.AddWriteCommand("EXPIRE", cmdEXPIRE)
	conf.AddWriteCommand("EXPIREAT", cmdEXPIREAT)
	// conf.AddWriteCommand("PERSIST", cmdPERSIST) // Prohibition: time persistence
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
	// Check if the number of arguments is correct
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])

	// Parse start index, returns error if not a valid integer
	start, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	// Parse end index, returns error if not a valid integer
	end, err := strconv.Atoi(args[3])
	if err != nil {
		return nil, err
	}

	// Call underlying GetRange implementation
	// Assumes ldb.GetRange handles:
	// 1. Negative index conversion
	// 2. Boundary checking
	// 3. Non-existent key cases
	v, err := ldb.GetRange(key, start, end)
	if err != nil {
		return nil, err
	}

	// Return empty string for nil value to match RESP behavior
	if v == nil {
		return "", nil
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

// Retrieves the bit value at a specified offset in the string stored at the given key.
// Returns:
//
//	The bit value (0 or 1), or an error if the number of arguments is incorrect or the offset is not a valid non-negative integer.
//
// Behavior:
//
//	Retrieves the bit value at the specified offset:
//	  - Returns 0 if the key doesn't exist or the offset is out of range.
//	  - Retrieves the bit at the specified offset if the key exists and the offset is within range.
//	Uses ldb.GetBit to handle bit retrieval, which manages cases where the key doesn't exist or the offset exceeds the string length.
//
// Error Handling:
//   - Returns an error for incorrect number of arguments.
//   - Returns an error if the offset is not a valid non-negative integer.
func cmdGETBIT(m uhaha.Machine, args []string) (interface{}, error) {
	// Check if the number of arguments is correct; requires 3 arguments: key and offset
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1]) // Retrieve the key
	offsetStr := args[2]   // Retrieve the offset string

	// Convert the offset string to a non-negative integer
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		return nil, errors.New("offset must be a non-negative integer")
	}
	if offset < 0 {
		return nil, errors.New("offset must be a non-negative integer")
	}

	// Retrieve the bit value at the specified offset from the database
	// If the key does not exist or the offset exceeds the string length, ldb.GetBit should return 0
	n, err := ldb.GetBit(key, offset)
	if err != nil {
		return nil, err
	}

	// Return the bit value, which is either 0 or 1
	return redcon.SimpleInt(n), nil
}

// cmdEXISTS checks the existence of keys. no concurrent
// Syntax: EXISTS key [key ...]
// Time complexity: O(N) where N is the number of keys to check.
// Returns the number of keys that exist from those specified as arguments.
// If the same existing key is mentioned multiple times in the arguments, it will be counted multiple times.
/*
func cmdEXISTS(m uhaha.Machine, args []string) (interface{}, error) {
	// Check if the number of arguments is correct
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	// Initialize a counter to keep track of existing keys
	var counter int

	// Iterate over all keys provided as arguments
	for _, key := range args[1:] {
		n, err := ldb.Exists([]byte(key))
		if err != nil {
			return nil, err
		}

		// If the key exists, increment the counter
		if n > 0 {
			counter++
		}
	}

	return redcon.SimpleInt(counter), nil
}
*/

// cmdEXISTS checks the existence of keys using concurrent checks for improved performance.
// Syntax: EXISTS key [key ...]
// Time complexity: O(N) where N is the number of keys to check.
// Returns the number of keys that exist from those specified as arguments.
// If the same existing key is mentioned multiple times in the arguments, it will be counted multiple times.
// The function uses a concurrency limit to check keys in parallel, which can reduce the overall execution time.
func cmdEXISTS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	keys := args[1:]
	n := len(keys)

	if n == 0 {
		return redcon.SimpleInt(0), nil
	}

	results := make(chan bool, n)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errret error

	for _, key := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			exists, err := existsKey(key)
			if err != nil {
				mu.Lock()
				if errret == nil {
					errret = err
				}
				mu.Unlock()
				return
			}
			results <- exists
		}(key)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	counter := 0
	for res := range results {
		if res {
			counter++
		}
	}

	if errret != nil {
		return nil, errret
	}

	return redcon.SimpleInt(counter), nil
}

func existsKey(key string) (bool, error) {
	n, err := ldb.Exists([]byte(key))
	if err != nil {
		return false, err
	}
	return n > 0, nil
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
	// Validate the number of arguments provided by the client.
	if len(args) < 3 || len(args) > 6 {
		return nil, uhaha.ErrWrongNumArgs
	}

	// Convert the key from string to byte slice for database operations.
	key := []byte(args[1])

	// Parse the bit value to search for.
	bit, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	// Initialize default values for start, end, and bitMode.
	start := 0
	end := -1
	bitMode := "BYTE" // Default to BYTE mode

	// Parse optional start and end parameters.
	if len(args) >= 4 {
		start, err = strconv.Atoi(args[3])
		if err != nil {
			return nil, err
		}
	}
	if len(args) >= 5 {
		end, err = strconv.Atoi(args[4])
		if err != nil {
			return nil, err
		}
	}
	if len(args) == 6 {
		bitMode = strings.ToUpper(args[5])
		if bitMode != "BYTE" && bitMode != "BIT" {
			return nil, fmt.Errorf("invalid bit mode: %s", bitMode)
		}
	}

	// Call the underlying BitPos function with the parsed parameters.
	n, err := ldb.BitPos(key, bit, start, end, bitMode)
	if err != nil {
		return nil, err
	}

	// Return the result as a simple integer.
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

// cmdBITCOUNT handles the BITCOUNT command, which counts the number of set bits (1s) in a string.
// The command can optionally take start and end indices to limit the range of bits to count,
// and an additional argument to specify whether the indices are in bytes or bits.
func cmdBITCOUNT(m uhaha.Machine, args []string) (interface{}, error) {
	// Validate the number of arguments provided by the client.
	if len(args) < 2 || len(args) > 5 {
		return nil, uhaha.ErrWrongNumArgs
	}

	// Convert the key from string to byte slice for database operations.
	key := []byte(args[1])

	// Parse the optional start and end indices and the optional BYTE/BIT argument.
	start, end, bitMode, err := parseBitRange(args[2:])
	if err != nil {
		return nil, err
	}

	// Count the number of set bits within the specified range using the ldb.BitCount function.
	n, err := ldb.BitCount(key, start, end, bitMode)
	if err != nil {
		return nil, err
	}

	// Return the count as a simple integer response.
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

	// Retrieve the value associated with the key
	val, err := ldb.Get([]byte(args[1]))
	if err != nil {
		return nil, err
	}

	// If val is nil, the key does not exist
	if val == nil {
		return nil, nil
	}

	// Return the value if the key exists
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
	// Check the number of arguments; at least one key is required
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	// Extract all keys from the arguments
	keys := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
	}

	// Retrieve the values for all specified keys
	values, err := ldb.MGet(keys...)
	if err != nil {
		return nil, err
	}

	// Convert the result into a slice of interfaces for RESP compatibility
	result := make([]interface{}, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = nil // If the value is nil, the key does not exist or is not a string
		} else {
			result[i] = v
		}
	}

	return result, nil
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

// parseBitRange parses the optional start and end indices and the optional BYTE/BIT argument from the command arguments.
// It returns the parsed indices, the bit mode (BYTE or BIT), and any error encountered during parsing.
func parseBitRange(args []string) (start int, end int, bitMode string, err error) {
	// Default values for start and end indices.
	start = 0
	end = -1
	bitMode = "BYTE" // Default to byte mode

	// If only one additional argument is provided, it is treated as the start index.
	if len(args) > 0 {
		if start, err = strconv.Atoi(args[0]); err != nil {
			return 0, 0, "", err
		}
	}

	// If two additional arguments are provided, they are treated as the start and end indices.
	if len(args) > 1 {
		if end, err = strconv.Atoi(args[1]); err != nil {
			return 0, 0, "", err
		}
	}

	// If a third additional argument is provided, it specifies the bit mode (BYTE or BIT).
	if len(args) == 3 {
		bitMode = strings.ToUpper(args[2])
		if bitMode != "BYTE" && bitMode != "BIT" {
			return 0, 0, "", fmt.Errorf("ERR syntax error")
		}
	}

	// If more than three additional arguments are provided, return an error.
	if len(args) > 3 {
		return 0, 0, "", uhaha.ErrWrongNumArgs
	}

	return start, end, bitMode, nil
}
