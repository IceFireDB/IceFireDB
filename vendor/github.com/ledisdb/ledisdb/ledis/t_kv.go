package ledis

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ledisdb/ledisdb/store"
	"github.com/siddontang/go/num"
)

// KVPair is the pair of key-value.
type KVPair struct {
	Key   []byte
	Value []byte
}

var errKVKey = errors.New("invalid encode kv key")

func checkKeySize(key []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	}
	return nil
}

func checkValueSize(value []byte) error {
	if len(value) > MaxValueSize {
		return errValueSize
	}

	return nil
}

func (db *DB) encodeKVKey(key []byte) []byte {
	ek := make([]byte, len(key)+1+len(db.indexVarBuf))
	pos := copy(ek, db.indexVarBuf)
	ek[pos] = KVType
	pos++
	copy(ek[pos:], key)
	return ek
}

func (db *DB) decodeKVKey(ek []byte) ([]byte, error) {
	pos, err := db.checkKeyIndex(ek)
	if err != nil {
		return nil, err
	}
	if pos+1 > len(ek) || ek[pos] != KVType {
		return nil, errKVKey
	}

	pos++

	return ek[pos:], nil
}

func (db *DB) encodeKVMinKey() []byte {
	ek := db.encodeKVKey(nil)
	return ek
}

func (db *DB) encodeKVMaxKey() []byte {
	ek := db.encodeKVKey(nil)
	ek[len(ek)-1] = KVType + 1
	return ek
}

func (db *DB) incr(key []byte, delta int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	var err error
	key = db.encodeKVKey(key)

	t := db.kvBatch

	t.Lock()
	defer t.Unlock()

	var n int64
	n, err = StrInt64(db.bucket.Get(key))
	if err != nil {
		return 0, err
	}

	n += delta

	t.Put(key, num.FormatInt64ToSlice(n))

	err = t.Commit()
	return n, err
}

// ps : here just focus on deleting the key-value data,
//
//	any other likes expire is ignore.
func (db *DB) delete(t *batch, key []byte) int64 {
	key = db.encodeKVKey(key)
	t.Delete(key)
	return 1
}

func (db *DB) setExpireAt(key []byte, when int64) (int64, error) {
	t := db.kvBatch
	t.Lock()
	defer t.Unlock()

	if exist, err := db.Exists(key); err != nil || exist == 0 {
		return 0, err
	}

	db.expireAt(t, KVType, key, when)
	if err := t.Commit(); err != nil {
		return 0, err
	}

	return 1, nil
}

// Decr decreases the data.
func (db *DB) Decr(key []byte) (int64, error) {
	return db.incr(key, -1)
}

// DecrBy decreases the data by decrement.
func (db *DB) DecrBy(key []byte, decrement int64) (int64, error) {
	return db.incr(key, -decrement)
}

// Del deletes the data.
func (db *DB) Del(keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	codedKeys := make([][]byte, len(keys))
	for i, k := range keys {
		codedKeys[i] = db.encodeKVKey(k)
	}

	t := db.kvBatch
	t.Lock()
	defer t.Unlock()

	for i, k := range keys {
		t.Delete(codedKeys[i])
		db.rmExpire(t, KVType, k)
	}

	err := t.Commit()
	return int64(len(keys)), err
}

// Exists check data exists or not.
func (db *DB) Exists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	var err error
	key = db.encodeKVKey(key)

	var v []byte
	v, err = db.bucket.Get(key)
	if v != nil && err == nil {
		return 1, nil
	}

	return 0, err
}

// Get gets the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	key = db.encodeKVKey(key)

	return db.bucket.Get(key)
}

// GetSlice gets the slice of the data.
func (db *DB) GetSlice(key []byte) (store.Slice, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	key = db.encodeKVKey(key)

	return db.bucket.GetSlice(key)
}

// GetSet gets the value and sets new value.
func (db *DB) GetSet(key []byte, value []byte) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	} else if err := checkValueSize(value); err != nil {
		return nil, err
	}

	key = db.encodeKVKey(key)

	t := db.kvBatch

	t.Lock()
	defer t.Unlock()

	oldValue, err := db.bucket.Get(key)
	if err != nil {
		return nil, err
	}

	t.Put(key, value)

	err = t.Commit()

	return oldValue, err
}

// Incr increases the data.
func (db *DB) Incr(key []byte) (int64, error) {
	return db.incr(key, 1)
}

// IncrBy increases the data by increment.
func (db *DB) IncrBy(key []byte, increment int64) (int64, error) {
	return db.incr(key, increment)
}

// MGet gets multi data.
func (db *DB) MGet(keys ...[]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))

	it := db.bucket.NewIterator()
	defer it.Close()

	for i := range keys {
		if err := checkKeySize(keys[i]); err != nil {
			return nil, err
		}

		values[i] = it.Find(db.encodeKVKey(keys[i]))
	}

	return values, nil
}

// MSet sets multi data.
func (db *DB) MSet(args ...KVPair) error {
	if len(args) == 0 {
		return nil
	}

	t := db.kvBatch

	var err error
	var key []byte
	var value []byte

	t.Lock()
	defer t.Unlock()

	for i := 0; i < len(args); i++ {
		if err := checkKeySize(args[i].Key); err != nil {
			return err
		} else if err := checkValueSize(args[i].Value); err != nil {
			return err
		}

		key = db.encodeKVKey(args[i].Key)

		value = args[i].Value

		t.Put(key, value)

	}

	err = t.Commit()
	return err
}

// Set sets the data.
func (db *DB) Set(key []byte, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	} else if err := checkValueSize(value); err != nil {
		return err
	}

	var err error
	orginKey := key
	key = db.encodeKVKey(key)

	t := db.kvBatch
	t.Lock()
	defer t.Unlock()

	t.Put(key, value)

	_, err = db.rmExpire(t, KVType, orginKey) //remove key ttl info
	if err != nil {
		return err
	}

	err = t.Commit()

	return err
}

// SetNX sets the data if not existed.
func (db *DB) SetNX(key []byte, value []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	} else if err := checkValueSize(value); err != nil {
		return 0, err
	}

	var err error
	key = db.encodeKVKey(key)

	var n int64 = 1

	t := db.kvBatch

	t.Lock()
	defer t.Unlock()

	if v, err := db.bucket.Get(key); err != nil {
		return 0, err
	} else if v != nil {
		n = 0
	} else {
		t.Put(key, value)

		err = t.Commit()
	}

	return n, err
}

// SetEX sets the data with a TTL.
func (db *DB) SetEX(key []byte, duration int64, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	} else if err := checkValueSize(value); err != nil {
		return err
	} else if duration <= 0 {
		return errExpireValue
	}

	ek := db.encodeKVKey(key)

	t := db.kvBatch

	t.Lock()
	defer t.Unlock()

	t.Put(ek, value)
	db.expireAt(t, KVType, key, time.Now().Unix()+duration)

	return t.Commit()
}

// SetEXAT sets the data with a timestamp.
func (db *DB) SetEXAT(key []byte, timestamp int64, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	} else if err := checkValueSize(value); err != nil {
		return err
	} else if timestamp <= time.Now().Unix() {
		return errExpireValue
	}

	ek := db.encodeKVKey(key)

	t := db.kvBatch

	t.Lock()
	defer t.Unlock()

	t.Put(ek, value)
	db.expireAt(t, KVType, key, timestamp)

	return t.Commit()
}

func (db *DB) flush() (drop int64, err error) {
	t := db.kvBatch
	t.Lock()
	defer t.Unlock()
	return db.flushType(t, KVType)
}

// Expire expires the data.
func (db *DB) Expire(key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, errExpireValue
	}

	return db.setExpireAt(key, time.Now().Unix()+duration)
}

// ExpireAt expires the data at when.
func (db *DB) ExpireAt(key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, errExpireValue
	}

	return db.setExpireAt(key, when)
}

// TTL returns the TTL of the data.
func (db *DB) TTL(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	return db.ttl(KVType, key)
}

// Persist removes the TTL of the data.
func (db *DB) Persist(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	t := db.kvBatch
	t.Lock()
	defer t.Unlock()
	n, err := db.rmExpire(t, KVType, key)
	if err != nil {
		return 0, err
	}

	err = t.Commit()
	return n, err
}

// SetRange sets the data with new value from offset.
func (db *DB) SetRange(key []byte, offset int, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	if err := checkKeySize(key); err != nil {
		return 0, err
	} else if len(value)+offset > MaxValueSize {
		return 0, errValueSize
	}

	key = db.encodeKVKey(key)

	t := db.kvBatch

	t.Lock()
	defer t.Unlock()

	oldValue, err := db.bucket.Get(key)
	if err != nil {
		return 0, err
	}

	extra := offset + len(value) - len(oldValue)
	if extra > 0 {
		oldValue = append(oldValue, make([]byte, extra)...)
	}

	copy(oldValue[offset:], value)

	t.Put(key, oldValue)

	if err := t.Commit(); err != nil {
		return 0, err
	}

	return int64(len(oldValue)), nil
}

// getBitRange adjusts start and end bit indices to handle negative values and ensures they are within bounds.
func getBitRange(start, end, bitLength int) (int, int) {
	if start < 0 {
		start = bitLength + start
	}
	if end < 0 {
		end = bitLength + end
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	if end >= bitLength {
		end = bitLength - 1
	}
	return start, end
}

func getRange(start, end, valLen int) (int, int) {
	if start < 0 {
		start = valLen + start
	}
	if end < 0 {
		end = valLen + end
	}
	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	if end >= valLen {
		end = valLen - 1
	}
	return start, end
}

// GetRange gets the range of the data.
func (db *DB) GetRange(key []byte, start int, end int) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}
	key = db.encodeKVKey(key)

	value, err := db.bucket.Get(key)
	if err != nil {
		return nil, err
	}

	valLen := len(value)

	start, end = getRange(start, end, valLen)

	if start > end {
		return nil, nil
	}

	return value[start : end+1], nil
}

// StrLen returns the length of the data.
func (db *DB) StrLen(key []byte) (int64, error) {
	s, err := db.GetSlice(key)
	if err != nil {
		return 0, err
	}
	n := 0
	if s != nil {
		n = s.Size()
		s.Free()
	}
	return int64(n), nil
}

// Append appends the value to the data.
func (db *DB) Append(key []byte, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	key = db.encodeKVKey(key)

	t := db.kvBatch

	t.Lock()
	defer t.Unlock()

	oldValue, err := db.bucket.Get(key)
	if err != nil {
		return 0, err
	}

	if len(oldValue)+len(value) > MaxValueSize {
		return 0, errValueSize
	}

	oldValue = append(oldValue, value...)

	t.Put(key, oldValue)

	if err := t.Commit(); err != nil {
		return 0, nil
	}

	return int64(len(oldValue)), nil
}

// BitOP does the bit operations in data.
func (db *DB) BitOP(op string, destKey []byte, srcKeys ...[]byte) (int64, error) {
	if err := checkKeySize(destKey); err != nil {
		return 0, err
	}

	op = strings.ToLower(op)
	if len(srcKeys) == 0 {
		return 0, nil
	} else if op == BitNot && len(srcKeys) > 1 {
		return 0, fmt.Errorf("BITOP NOT has only one srckey")
	} else if len(srcKeys) < 2 {
		return 0, nil
	}

	key := db.encodeKVKey(srcKeys[0])

	value, err := db.bucket.Get(key)
	if err != nil {
		return 0, err
	}

	if op == BitNot {
		for i := 0; i < len(value); i++ {
			value[i] = ^value[i]
		}
	} else {
		for j := 1; j < len(srcKeys); j++ {
			if err := checkKeySize(srcKeys[j]); err != nil {
				return 0, err
			}

			key = db.encodeKVKey(srcKeys[j])
			ovalue, err := db.bucket.Get(key)
			if err != nil {
				return 0, err
			}

			if len(value) < len(ovalue) {
				value, ovalue = ovalue, value
			}

			for i := 0; i < len(ovalue); i++ {
				switch op {
				case BitAND:
					value[i] &= ovalue[i]
				case BitOR:
					value[i] |= ovalue[i]
				case BitXOR:
					value[i] ^= ovalue[i]
				default:
					return 0, fmt.Errorf("invalid op type: %s", op)
				}
			}

			for i := len(ovalue); i < len(value); i++ {
				switch op {
				case BitAND:
					value[i] &= 0
				case BitOR:
					value[i] |= 0
				case BitXOR:
					value[i] ^= 0
				}
			}
		}
	}

	key = db.encodeKVKey(destKey)

	t := db.kvBatch

	t.Lock()
	defer t.Unlock()

	t.Put(key, value)

	if err := t.Commit(); err != nil {
		return 0, err
	}

	return int64(len(value)), nil
}

var bitsInByte = [256]int32{0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3,
	4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3,
	3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4,
	5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
	3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
	5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2,
	2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3,
	4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4,
	5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
	6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5,
	6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8}

func numberBitCount(i uint32) uint32 {
	i = i - ((i >> 1) & 0x55555555)
	i = (i & 0x33333333) + ((i >> 2) & 0x33333333)
	return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24
}

// validateRange
func validateRange(start, end, length int) (int, int, error) {
	// Check if start or end exceed length before dealing with negative index
	if start >= length {
		return 0, 0, fmt.Errorf("byte range out of bounds")
	}
	if end >= length {
		return 0, 0, fmt.Errorf("byte range out of bounds")
	}

	// deal with negative index
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// check range
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return 0, 0, fmt.Errorf("byte invalid range: start > end")
	}

	return start, end, nil
}

// validateBitRange
func validateBitRange(start, end, bitLength int) (int, int, error) {
	// Check if start or end exceed bitLength before dealing with negative index
	if start >= bitLength {
		return 0, 0, fmt.Errorf("bit range out of bounds")
	}
	if end >= bitLength {
		return 0, 0, fmt.Errorf("bit range out of bounds")
	}

	// deal with negative index
	if start < 0 {
		start = bitLength + start
	}
	if end < 0 {
		end = bitLength + end
	}

	// check range
	if start < 0 {
		start = 0
	}
	if end >= bitLength {
		end = bitLength - 1
	}
	if start > end {
		return 0, 0, fmt.Errorf("ERR invalid bit range: start > end")
	}

	return start, end, nil
}

// BitCount counts the number of set bits (1s) in a string within the specified range.
// The range can be specified in bytes or bits, and supports negative indices.
func (db *DB) BitCount(key []byte, start int, end int, bitMode string) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	key = db.encodeKVKey(key)
	value, err := db.bucket.Get(key)
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, nil
	}

	// check bitMode
	bitMode = strings.ToUpper(bitMode)
	if bitMode != "BYTE" && bitMode != "BIT" {
		return 0, fmt.Errorf("ERR invalid bit mode")
	}

	valueLen := len(value)
	// Adjust range according to mode
	if bitMode == "BYTE" {
		if start, end, err = validateRange(start, end, valueLen); err != nil {
			return 0, err
		}
		value = value[start : end+1]
	} else {
		bitLen := valueLen * 8
		if start, end, err = validateBitRange(start, end, bitLen); err != nil {
			return 0, err
		}
		startByte, startBit := start/8, start%8
		endByte, endBit := end/8, end%8

		if startByte >= valueLen || endByte >= valueLen {
			return 0, fmt.Errorf("ERR bit range out of bounds")
		}

		value = value[startByte : endByte+1]
		if startBit > 0 {
			value[0] &= (0xFF >> uint(startBit))
		}
		if endBit < 7 {
			value[len(value)-1] &= (0xFF << uint(7-endBit))
		}
	}

	var n int64
	pos := 0
	for ; pos+4 <= len(value); pos += 4 {
		n += int64(numberBitCount(binary.BigEndian.Uint32(value[pos : pos+4])))
	}

	for ; pos < len(value); pos++ {
		n += int64(bitsInByte[value[pos]])
	}

	return n, nil
}

// BitPos returns the position of the first occurrence of the specified bit in the key's value.
func (db *DB) BitPos(key []byte, on int, start int, end int, bitMode string) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	if on != 0 && on != 1 {
		return 0, fmt.Errorf("bit must be 0 or 1, not %d", on)
	}

	key = db.encodeKVKey(key)
	value, err := db.bucket.Get(key)
	if err != nil {
		return 0, err
	}

	var startByte, endByte, startBit, endBit int
	if bitMode == "BIT" {
		bitLen := len(value) * 8
		start, end = getBitRange(start, end, bitLen)
		startByte = start / 8
		startBit = start % 8
		endByte = end / 8
		endBit = end % 8
	} else {
		byteLen := len(value)
		start, end = getRange(start, end, byteLen)
		startByte = start
		startBit = 0
		endByte = end
		endBit = 7
	}

	if startByte > endByte {
		return -1, nil
	}

	value = value[startByte : endByte+1]

	for byteIdx, v := range value {
		byteOffset := byteIdx + startByte
		var bitStart, bitEnd int
		if byteOffset == startByte {
			bitStart = startBit
			bitEnd = 7
		} else if byteOffset == endByte {
			bitStart = 0
			bitEnd = endBit
		} else {
			bitStart = 0
			bitEnd = 7
		}
		for bitIdx := bitStart; bitIdx <= bitEnd; bitIdx++ {
			shift := uint(7 - bitIdx)
			bitVal := (v >> shift) & 1
			if int(bitVal) == on {
				return int64(byteOffset*8 + bitIdx), nil
			}
		}
	}

	return -1, nil
}

// SetBit sets the bit to the data.
func (db *DB) SetBit(key []byte, offset int, on int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	if (on & ^1) != 0 {
		return 0, fmt.Errorf("bit must be 0 or 1, not %d", on)
	}

	t := db.kvBatch

	t.Lock()
	defer t.Unlock()

	key = db.encodeKVKey(key)
	value, err := db.bucket.Get(key)
	if err != nil {
		return 0, err
	}

	byteOffset := int(uint32(offset) >> 3)
	extra := byteOffset + 1 - len(value)
	if extra > 0 {
		value = append(value, make([]byte, extra)...)
	}

	byteVal := value[byteOffset]
	bit := 7 - uint8(uint32(offset)&0x7)
	bitVal := byteVal & (1 << bit)

	byteVal &= ^(1 << bit)
	byteVal |= (uint8(on&0x1) << bit)

	value[byteOffset] = byteVal

	t.Put(key, value)
	if err := t.Commit(); err != nil {
		return 0, err
	}

	if bitVal > 0 {
		return 1, nil
	}

	return 0, nil
}

// GetBit gets the bit of data at offset.
func (db *DB) GetBit(key []byte, offset int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	key = db.encodeKVKey(key)

	value, err := db.bucket.Get(key)
	if err != nil {
		return 0, err
	}

	byteOffset := uint32(offset) >> 3
	bit := 7 - uint8(uint32(offset)&0x7)

	if byteOffset >= uint32(len(value)) {
		return 0, nil
	}

	bitVal := value[byteOffset] & (1 << bit)
	if bitVal > 0 {
		return 1, nil
	}

	return 0, nil
}
