package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/ledisdb/ledisdb/ledis"
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
)

func init() {
	conf.AddWriteCommand("SADD", cmdSADD)
	conf.AddReadCommand("SCARD", cmdSCARD)
	conf.AddReadCommand("SDIFF", cmdSDIFF)
	conf.AddWriteCommand("SDIFFSTORE", cmdSDIFFSTORE)
	conf.AddReadCommand("SINTER", cmdSINTER)
	conf.AddWriteCommand("SINTERSTORE", cmdSINTERSTORE)
	conf.AddReadCommand("SISMEMBER", cmdSISMEMBER)
	conf.AddReadCommand("SMEMBERS", cmdSMEMBERS)
	conf.AddWriteCommand("SREM", cmdSREM)
	conf.AddReadCommand("SUNION", cmdSUNION)
	conf.AddWriteCommand("SUNIONSTORE", cmdSUNIONSTORE)
	conf.AddWriteCommand("SCLEAR", cmdSCLEAR)
	conf.AddWriteCommand("SMCLEAR", cmdSMCLEAR)
	conf.AddWriteCommand("SEXPIRE", cmdSEXPIRE)
	conf.AddWriteCommand("SEXPIREAT", cmdSEXPIREAT)
	conf.AddReadCommand("STTL", cmdSTTL)
	conf.AddWriteCommand("SPERSIST", cmdSPERSIST)
	conf.AddReadCommand("SKEYEXISTS", cmdSKEYEXISTS)
	conf.AddWriteCommand("SMOVE", cmdSMOVE)
	conf.AddWriteCommand("SPOP", cmdSPOP)
	conf.AddReadCommand("SRANDMEMBER", cmdSRANDMEMBER)
}

func cmdSADD(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}
	n, err := ldb.SAdd([]byte(args[1]), stringSliceToBytes(args[2:])...)
	return redcon.SimpleInt(n), err
}

func soptGeneric(args [][]byte, optType byte) ([][]byte, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	var v [][]byte
	var err error
	switch optType {
	case ledis.UnionType:
		v, err = ldb.SUnion(args[1:]...)
	case ledis.DiffType:
		v, err = ldb.SDiff(args[1:]...)
	case ledis.InterType:
		v, err = ldb.SInter(args[1:]...)
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

func soptStoreGeneric(args [][]byte, optType byte) (interface{}, error) {
	if len(args) < 3 {
		return 0, uhaha.ErrWrongNumArgs
	}

	var n int64
	var err error

	switch optType {
	case ledis.UnionType:
		n, err = ldb.SUnionStore(args[1], args[2:]...)
	case ledis.DiffType:
		n, err = ldb.SDiffStore(args[1], args[2:]...)
	case ledis.InterType:
		n, err = ldb.SInterStore(args[1], args[2:]...)
	}
	return redcon.SimpleInt(n), err
}

func cmdSCARD(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.SCard([]byte(args[1]))
	return redcon.SimpleInt(n), err
}

func cmdSDIFF(m uhaha.Machine, args []string) (interface{}, error) {
	return soptGeneric(stringSliceToBytes(args), ledis.DiffType)
}

func cmdSDIFFSTORE(m uhaha.Machine, args []string) (interface{}, error) {
	return soptStoreGeneric(stringSliceToBytes(args), ledis.DiffType)
}

func cmdSINTER(m uhaha.Machine, args []string) (interface{}, error) {
	return soptGeneric(stringSliceToBytes(args), ledis.InterType)
}

func cmdSINTERSTORE(m uhaha.Machine, args []string) (interface{}, error) {
	return soptStoreGeneric(stringSliceToBytes(args), ledis.InterType)
}

func cmdSISMEMBER(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}
	n, err := ldb.SIsMember([]byte(args[1]), []byte(args[2]))

	return redcon.SimpleInt(n), err
}

func cmdSMEMBERS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	return ldb.SMembers([]byte(args[1]))
}

func cmdSREM(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.SRem([]byte(args[1]), stringSliceToBytes(args[2:])...)
	return redcon.SimpleInt(n), err
}

func cmdSUNION(m uhaha.Machine, args []string) (interface{}, error) {
	return soptGeneric(stringSliceToBytes(args), ledis.UnionType)
}

func cmdSUNIONSTORE(m uhaha.Machine, args []string) (interface{}, error) {
	return soptStoreGeneric(stringSliceToBytes(args), ledis.UnionType)
}

func cmdSCLEAR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.SClear([]byte(args[1]))

	return redcon.SimpleInt(n), err
}

func cmdSMCLEAR(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.SMclear(stringSliceToBytes(args[1:])...)

	return redcon.SimpleInt(n), err
}

func cmdSEXPIRE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	duration, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, uhaha.ErrInvalid
	}

	timestamp := m.Now().Unix() + duration
	if timestamp < time.Now().Unix() {
		_, err := ldb.SClear([]byte(args[1]))
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(0), nil
	}

	v, err := ldb.SExpireAt([]byte(args[1]), timestamp)
	return redcon.SimpleInt(v), err
}

func cmdSEXPIREAT(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	timestamp, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, uhaha.ErrInvalid
	}
	if timestamp < time.Now().Unix() {
		_, err := ldb.SClear([]byte(args[1]))
		if err != nil {
			return nil, err
		}
		return redcon.SimpleInt(0), nil
	}

	v, err := ldb.SExpireAt([]byte(args[1]), timestamp)
	return redcon.SimpleInt(v), err
}

func cmdSTTL(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	v, err := ldb.STTL([]byte(args[1]))
	return redcon.SimpleInt(v), err
}

func cmdSPERSIST(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.SPersist([]byte(args[1]))
	return redcon.SimpleInt(n), err
}

func cmdSKEYEXISTS(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	n, err := ldb.SKeyExists([]byte(args[1]))
	return redcon.SimpleInt(n), err
}

func cmdSMOVE(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	source := []byte(args[1])
	destination := []byte(args[2])
	member := []byte(args[3])

	n, err := ldb.SIsMember(source, member)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return redcon.SimpleInt(0), nil
	}

	// A move onto the same key is a no-op in Redis; removing and re-adding would
	// delete an emptied set (and with it the key's TTL) before recreating it.
	if bytes.Equal(source, destination) {
		return redcon.SimpleInt(1), nil
	}

	if _, err := ldb.SRem(source, member); err != nil {
		return nil, err
	}
	if _, err := ldb.SAdd(destination, member); err != nil {
		return nil, err
	}

	// Redis reports 1 for a move even when the destination already held the member.
	return redcon.SimpleInt(1), nil
}

func cmdSPOP(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])

	members, err := ldb.SMembers(key)
	if err != nil {
		return nil, err
	}

	// SPOP is a write command: every replica replays it, so the member it picks
	// must be identical everywhere. Two things make that hold: the members are
	// sorted first, so the draw cannot depend on the backend's iteration order,
	// and the index comes from the machine's generator, which uhaha seeds
	// identically across the cluster - never math/rand, a per-process source that
	// would diverge the state machines.
	sort.Slice(members, func(i, j int) bool { return bytes.Compare(members[i], members[j]) < 0 })
	rnd := m.Rand()
	draw := func(n int) int { return int(rnd.Uint64() % uint64(n)) }

	// Without COUNT Redis replies with the member itself, with COUNT an array.
	if len(args) == 2 {
		if len(members) == 0 {
			return nil, nil
		}
		popped := members[draw(len(members))]
		if _, err := ldb.SRem(key, popped); err != nil {
			return nil, err
		}
		return popped, nil
	}

	count, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, fmt.Errorf("ERR value is not an integer or out of range")
	}
	if count < 0 {
		return nil, fmt.Errorf("ERR value is out of range, must be positive")
	}
	if count == 0 || len(members) == 0 {
		return [][]byte{}, nil
	}
	if count > int64(len(members)) {
		count = int64(len(members))
	}

	// Partial Fisher-Yates: deterministic for a given generator, and it only
	// shuffles the slots it is about to return.
	for i := 0; i < int(count); i++ {
		j := i + draw(len(members)-i)
		members[i], members[j] = members[j], members[i]
	}
	popped := members[:count]
	if _, err := ldb.SRem(key, popped...); err != nil {
		return nil, err
	}

	return popped, nil
}

// maxRandMemberCount bounds a client-supplied |count| so one read command cannot
// ask the server to materialise an unbounded reply.
const maxRandMemberCount = 1 << 20

func cmdSRANDMEMBER(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])

	members, err := ldb.SMembers(key)
	if err != nil {
		return nil, err
	}

	// Without COUNT Redis replies with the member itself, with COUNT an array.
	if len(args) == 2 {
		if len(members) == 0 {
			return nil, nil
		}
		return members[rand.Intn(len(members))], nil
	}

	count, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, fmt.Errorf("ERR value is not an integer or out of range")
	}
	if count > maxRandMemberCount || count < -maxRandMemberCount {
		return nil, fmt.Errorf("ERR value is out of range")
	}
	if count == 0 || len(members) == 0 {
		return [][]byte{}, nil
	}

	// A read command is not replayed, so plain randomness is correct here; the
	// machine generator would hand back a frozen series for reads.
	if count > 0 {
		rand.Shuffle(len(members), func(i, j int) {
			members[i], members[j] = members[j], members[i]
		})
		if count > int64(len(members)) {
			count = int64(len(members))
		}
		return members[:count], nil
	}

	// A negative count may repeat members, as in Redis.
	result := make([][]byte, 0, -count)
	for i := int64(0); i < -count; i++ {
		result = append(result, members[rand.Intn(len(members))])
	}

	return result, nil
}

func stringSliceToBytes(args []string) [][]byte {
	bs := make([][]byte, len(args))
	for k, v := range args {
		bs[k] = []byte(v)
	}
	return bs
}
