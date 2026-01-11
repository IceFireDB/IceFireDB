# String Type Detailed Audit

## Key Findings

### 🔴 Most Critical Issue: SET Command Missing Standard Options

**Redis Standard SET Command**:
```
SET key value [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | NX | XX | KEEPTTL]
```

**Current Implementation**:
```go
func cmdSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	if err := ldb.Set([]byte(args[1]), []byte(args[2])); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}
```

**Issues**:
- ❌ Only supports `SET key value` format
- ❌ Does not support EX option (set second-level expiration time)
- ❌ Does not support PX option (set millisecond-level expiration time)
- ❌ Does not support NX option (set only when key does not exist)
- ❌ Does not support XX option (set only when key exists)
- ❌ Does not support EXAT/PXAT options (set absolute expiration time)
- ❌ Does not support KEEPTTL option (keep existing TTL)

**Impact**:
- This is the most frequently used Redis command
- Incompatibility with Redis standard makes application migration difficult
- Unable to implement common conditional setting and expiration time setting

**Priority**: 🔴🔴🔴 Highest

---

## Other String Commands Audit

### ✅ Correctly Implemented Commands

#### 1. GET Command
```go
func cmdGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	val, err := ldb.Get([]byte(args[1]))
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil  // ✅ Correctly returns nil to indicate key does not exist
	}

	return val, nil  // ✅ Correctly returns bulk string
}
```
- ✅ RESP protocol correct
- ✅ Returns nil when key does not exist

#### 2. GETSET Command
```go
func cmdGETSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	v, err := ldb.GetSet([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}

	return v, nil  // ✅ Correctly returns bulk string (old value)
}
```
- ✅ RESP protocol correct

#### 3. GETRANGE Command
```go
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

	if v == nil {
		return "", nil  // ✅ Returns empty string when key does not exist
	}

	return v, nil  // ✅ Correctly returns bulk string
}
```
- ✅ Relies on underlying implementation to handle negative indices and boundaries
- ✅ Returns empty string instead of nil (complies with Redis specification)

#### 4. MGET Command
```go
func cmdMGET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	keys := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = []byte(args[i])
	}

	values, err := ldb.MGet(keys...)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = nil  // ✅ Returns nil for non-existent keys
		} else {
			result[i] = v  // ✅ Returns bulk string for existing keys
		}
	}

	return result, nil
}
```
- ✅ Correctly handles nil values
- ✅ Returns RESP array

#### 5. MSET Command
```go
func cmdMSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return nil, uhaha.ErrWrongNumArgs
	}

	kvPairs := make([]ledis.KVPair, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		kvPairs[(i-1)/2] = ledis.KVPair{
			Key:   []byte(args[i]),
			Value: []byte(args[i+1]),
		}
	}

	if err := ldb.MSet(kvPairs...); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil  // ✅ Correctly returns "OK"
}
```
- ✅ Parameter validation correct
- ✅ Returns simple string "OK"

#### 6. SETNX Command
```go
func cmdSETNX(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.SetNX([]byte(args[1]), []byte(args[2]))
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil  // ✅ Returns 1 (set successfully) or 0 (key already exists)
}
```
- ✅ Atomic operation correct
- ✅ Return value correct

#### 7. STRLEN Command
```go
func cmdSTRLEN(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])

	exists, err := ldb.Exists(key)
	if exists == 0 || err != nil {
		return redcon.SimpleInt(0), err  // ✅ Returns 0 when key does not exist
	}

	n, err := ldb.StrLen(key)
	if err != nil {
		return redcon.SimpleInt(0), err
	}

	return redcon.SimpleInt(n), nil  // ✅ Returns string length
}
```
- ✅ Correctly handles key non-existence case

#### 8. APPEND Command
```go
func cmdAPPEND(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	value := []byte(args[2])

	n, err := ldb.Append(key, value)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil  // ✅ Returns new length
}
```
- ✅ Correctly implemented

#### 9. SETRANGE Command
```go
func cmdSETRANGE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	value := []byte(args[3])

	n, err := ldb.SetRange(key, offset, value)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil  // ✅ Returns new length
}
```
- ✅ Correctly implemented

#### 10. INCR/DECR/INCRBY/DECRBY Commands
```go
func cmdINCR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.Incr([]byte(args[1]))
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil  // ✅ Returns new value
}
```
- ✅ Automatically initializes non-existent keys to 0
- ✅ Returns new value

#### 11. TTL Command
```go
func cmdTTL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])

	exists, err := ldb.Exists(key)
	if err != nil {
		return nil, err
	}

	if exists == 0 {
		return redcon.SimpleInt(-2), nil  // ✅ Returns -2 when key does not exist
	}

	ttl, err := ldb.TTL(key)
	if err != nil {
		return nil, err
	}

	if ttl == -1 {
		return redcon.SimpleInt(-1), nil  // ✅ Returns -1 when no expiration time
	}

	return redcon.SimpleInt(ttl), nil  // ✅ Returns remaining TTL
}
```
- ✅ Correctly handles all TTL return values

#### 12. EXISTS Command
```go
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

	return redcon.SimpleInt(counter), nil  // ✅ Returns number of existing keys
}
```
- ✅ Concurrent optimization improves performance
- ✅ Returns count of existing keys

#### 13. SETBIT/GETBIT Commands
```go
func cmdSETBIT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])

	offset, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	if offset < 0 {
		return nil, errors.New("offset must be a non-negative integer")
	}

	value, err := strconv.Atoi(args[3])
	if err != nil {
		return nil, err
	}

	if value != 0 && value != 1 {
		return nil, errors.New("value must be 0 or 1")
	}

	n, err := ldb.SetBit(key, offset, value)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil  // ✅ Returns original bit value
}
```
- ✅ Complete parameter validation
- ✅ Returns original bit value

#### 14. BITOP Command
```go
func cmdBITOP(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	op := strings.ToUpper(args[1])
	if op != "AND" && op != "OR" && op != "XOR" && op != "NOT" {
		return nil, fmt.Errorf("ERR syntax error, invalid operation: %s", op)
	}

	destKey := []byte(args[2])

	srcKeys := make([][]byte, 0)
	if op == "NOT" {
		// NOT operation requires exactly one source key
		if len(args) != 3 {
			return nil, uhaha.ErrWrongNumArgs
		}
		srcKeys = append(srcKeys, []byte(args[2]))
	} else {
		// AND, OR, XOR operations require at least one source key
		if len(args) < 4 {
			return nil, uhaha.ErrWrongNumArgs
		}
		for i := 3; i < len(args); i++ {
			srcKeys = append(srcKeys, []byte(args[i]))
		}
	}

	n, err := ldb.BitOP(op, destKey, srcKeys...)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil  // ✅ Returns result string length
}
```
- ✅ Supports all bit operations
- ✅ Parameter validation correct

#### 15. BITCOUNT Command
```go
func cmdBITCOUNT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 || len(args) > 5 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])

	start, end, bitMode, err := parseBitRange(args[2:])
	if err != nil {
		return nil, err
	}

	n, err := ldb.BitCount(key, start, end, bitMode)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil  // ✅ Returns bit count
}
```
- ✅ Supports BYTE/BIT modes
- ✅ Supports range

#### 16. BITPOS Command
```go
func cmdBITPOS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 || len(args) > 6 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])

	bit, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, err
	}

	start := 0
	end := -1
	bitMode := "BYTE" // Default to BYTE mode

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

	n, err := ldb.BitPos(key, bit, start, end, bitMode)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(n), nil  // ✅ Returns bit position
}
```
- ✅ Supports range and mode
- ✅ Returns found position

#### 17. DEL Command
```go
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

	return redcon.SimpleInt(n), nil  // ✅ Returns actual number of deleted keys
}
```
- ✅ Correctly implemented
- ⚠️ Code comments indicate difference from Redis standard: no key existence check (for consistency)

---

## Missing String Commands

| Command | Redis Standard | Importance | Notes |
|---------|----------------|------------|-------|
| **SET** (Full version) | `SET key value [EX\|PX ...]` | 🔴 Highest | Currently only basic version supported |
| PSETEX | `PSETEX key milliseconds value` | High | Millisecond expiration (can use SET PX instead) |
| GETEX | `GETEX key [EX seconds\|PX milliseconds\|EXAT\|PXAT ...]` | Medium | Added in Redis 6.2 |
| SETRANGE | ✅ Implemented | - | - |
| SUBSTR | `SUBSTR key start end` | Low | Alias for GETRANGE, deprecated |

---

## Suggested Improvement Priority

### 🔴 Highest Priority
1. **Enhance SET command** - Support NX/XX options (conditional setting)
2. **Enhance SET command** - Support EX/PX options (expiration time)

### High Priority
3. **Enhance SET command** - Support KEEPTTL option
4. **Enhance SET command** - Support EXAT/PXAT options (absolute time)

### Medium Priority
5. **Implement PSETEX command** - Although can use SET PX instead, maintain compatibility
6. **Implement GETEX command** - Redis 6.2+ feature

### Low Priority
7. Implement SUBSTR command (GETRANGE alias)

---

## SET Command Enhancement Plan

### Plan 1: Progressive Implementation
```go
func cmdSET(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	value := []byte(args[2])

	// Parse options
	var exSeconds int64 = -1
	var pxMilliseconds int64 = -1
	var nx, xx, keepttl bool = false, false, false

	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "KEEPTTL":
			keepttl = true
		case "EX":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			sec, err := ledis.StrInt64([]byte(args[i+1]), nil)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid expire time")
			}
			exSeconds = sec
			i++
		case "PX":
			if i+1 >= len(args) {
				return nil, uhaha.ErrWrongNumArgs
			}
			ms, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid expire time")
			}
			pxMilliseconds = ms
			i++
		case "EXAT":
			// Implement absolute time
			i++
		case "PXAT":
			// Implement absolute time (milliseconds)
			i++
		default:
			return nil, fmt.Errorf("ERR syntax error")
		}
	}

	// Validate conflicting options
	if nx && xx {
		return nil, errors.New("ERR syntax error")
	}
	if keepttl && (exSeconds >= 0 || pxMilliseconds >= 0) {
		return nil, errors.New("ERR syntax error")
	}

	// Check NX/XX conditions
	exists, _ := ldb.Exists(key)
	if nx && exists > 0 {
		return nil, nil  // Key exists, NX failed
	}
	if xx && exists == 0 {
		return nil, nil  // Key doesn't exist, XX failed
	}

	// Set value
	if err := ldb.Set(key, value); err != nil {
		return nil, err
	}

	// Handle expiration
	if exSeconds >= 0 {
		timestamp := m.Now().Unix() + exSeconds
		if _, err := ldb.ExpireAt(key, timestamp); err != nil {
			return nil, err
		}
	} else if pxMilliseconds >= 0 {
		timestamp := m.Now().UnixNano()/1e6 + pxMilliseconds
		if _, err := ldb.ExpireAt(key, timestamp); err != nil {
			return nil, err
		}
	}

	return redcon.SimpleString("OK"), nil
}
```

### Plan 2: Keep Simple Version
Given the Raft architecture and differences noted in code comments, this may be intentionally simplified. Suggestion:
1. Clearly document the limitations of the SET command
2. Provide alternative approaches using SETEX/SETEXAT
3. Consider supporting NX/XX options in future versions

---

## Summary

### ✅ Correctly Implemented Commands (21)
- BITCOUNT, BITOP, BITPOS, DEL, DECR, DECRBY, EXISTS, GET, GETBIT, GETRANGE, GETSET, INCR, INCRBY, MGET, MSET, SETBIT, SETNX, SETRANGE, STRLEN, APPEND, SETEX, SETEXAT, EXPIRE, EXPIREAT, TTL

### ❌ Commands Needing Improvement (1)
- **SET** - Missing standard option support

**Suggested Priority**: 🔴🔴🔴 Highest priority to improve SET command
