package main

import (
	"fmt"
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/siddontang/go/log"
	"github.com/siddontang/goredis"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/tidwall/redcon"
	"github.com/tidwall/uhaha"
	"strings"
	"time"
)

var (
	errNoKey          = errors.New("migrate key is not exists")
	errKeyInMigrating = errors.New("key is in migrating yet")
)

func init() {
	conf.AddWriteCommand("MIGRATE", cmdMigrate)
	conf.AddReadCommand("MIGRATEDB", cmdMigratedb)
	conf.AddWriteCommand("RESTORE", restoreCommand)
}

// migrate tohost toport key slotid migratetimeout
func cmdMigrate(m uhaha.Machine, args []string) (interface{}, error) {
	//n, err := ldb.GetDBForKeyUnsafe([]byte(args[1])).SAdd([]byte(args[1]), stringSliceToBytes(args[2:])...)
	//return redcon.SimpleInt(n), err

	if len(args) != 6 {
		return nil, uhaha.ErrWrongNumArgs
	}

	addr := fmt.Sprintf("%s:%s", string(args[1]), string(args[2]))
	if addr == fmt.Sprintf("%s:%d", announceIP, announcePort) {
		//same server， can not migrate
		return nil, fmt.Errorf("migrate in same server is not allowed")
	}

	tp := strings.ToUpper(string(args[3]))
	key := args[4]

	timeout, err := ledis.StrInt64([]byte(args[5]), nil)
	if err != nil {
		return nil, fmt.Errorf("invalid timeout %s, err: %w", args[5], err)
	} else if timeout < 0 {
		return nil, fmt.Errorf("invalid timeout %d", timeout)
	}

	conn, err := getMigrateDBConn(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// if key is in migrating, we will wait 500ms and retry again
	for i := 0; i < 10; i++ {
		if tp == "ALL" {
			// if tp is ALL, we will migrate the key in all types
			// this feature is useful for xcodis RESTORE or other commands that we don't know the data type exactly
			err = migrateAllTypeKeys(conn, []byte(key), timeout)
		} else {
			err = migrateKey(conn, tp, []byte(key), timeout)
		}

		if err != errKeyInMigrating {
			break
		} else {
			log.Infof("%s key %s is in migrating, wait 500ms and retry", tp, key)
			time.Sleep(500 * time.Millisecond)
		}
	}

	if err != nil {
		if err == errNoKey {
			return redcon.SimpleString("NOKEY"), nil
		}
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

//MIGRATEDB host port tp count db timeout
//select count tp type keys and migrate
//will block any other write operations
//maybe only for xcodis
func cmdMigratedb(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 7 {
		return nil, uhaha.ErrWrongNumArgs
	}

	addr := fmt.Sprintf("%s:%s", string(args[1]), string(args[2]))
	if addr == fmt.Sprintf("%s:%d", announceIP, announcePort) {
		//same server， can not migrate
		return nil, fmt.Errorf("migrate in same server is not allowed")
	}

	tp := strings.ToUpper(string(args[3]))

	count, err := ledis.StrInt64([]byte(args[4]), nil)
	if err != nil {
		return nil, err
	} else if count <= 0 {
		count = 10
	}

	dbNum, err := parseMigrateDB([]byte(args[5]))
	if err != nil {
		return nil, err
	}

	timeout, err := ledis.StrInt64([]byte(args[6]), nil)
	if err != nil {
		return nil, err
	} else if timeout < 0 {
		return nil, fmt.Errorf("invalid timeout %d", timeout)
	}

	db, err := ldb.GetDB(int(dbNum))
	if err != nil {
		return nil, err
	}
	keys, err := xscan(db, tp, int(count))
	if err != nil {
		return nil, err
	} else if len(keys) == 0 {
		return redcon.SimpleInt(0), nil
	}

	conn, err := getMigrateDBConn(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	migrateNum := int64(0)
	for _, key := range keys {
		err = migrateKey(conn, tp, key, timeout)
		if err != nil {
			if err == errNoKey || err == errKeyInMigrating {
				continue
			} else {
				return nil, err
			}
		}

		migrateNum++
	}

	return redcon.SimpleInt(migrateNum), nil
}

func xscan(db *ledis.DB, tp string, count int) ([][]byte, error) {
	switch strings.ToUpper(tp) {
	case KVName:
		return db.Scan(KV, nil, count, false, "")
	case HashName:
		return db.Scan(HASH, nil, count, false, "")
	case ListName:
		return db.Scan(LIST, nil, count, false, "")
	case SetName:
		return db.Scan(SET, nil, count, false, "")
	case ZSetName:
		return db.Scan(ZSET, nil, count, false, "")
	default:
		return nil, fmt.Errorf("invalid key type %s", tp)
	}
}

func parseMigrateDB(arg []byte) (uint64, error) {
	db, err := ledis.StrUint64(arg, nil)
	if err != nil {
		return 0, err
	} else if db >= uint64(slotNum) {
		return 0, fmt.Errorf("invalid db index %d, must < %d", db, slotNum)
	}
	return db, nil
}

func getMigrateDBConn(addr string) (*goredis.PoolConn, error) {
	mc := app.getMigrateClient(addr)

	conn, err := mc.Get()
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func migrateKey(conn *goredis.PoolConn, tp string, key []byte, timeout int64) error {
	if !app.migrateKeyLock(tp, key) {
		// other may also migrate this key, skip it
		return errKeyInMigrating
	}

	defer app.migrateKeyUnlock(tp, key)

	db := ldb.GetDBForKeyUnsafe(key)
	data, err := xdump(db, tp, key)
	if err != nil {
		return err
	} else if data == nil {
		return errNoKey
	}

	ttl, err := xttl(db, tp, key)
	if err != nil {
		return err
	}

	//timeout is milliseconds
	t := time.Duration(timeout) * time.Millisecond

	conn.SetReadDeadline(time.Now().Add(t))

	//ttl is second, but restore need millisecond
	if _, err = conn.Do("restore", key, ttl*1e3, data); err != nil {
		return err
	}

	if err = xdel(db, tp, key); err != nil {
		return err
	}

	return nil
}

func migrateAllTypeKeys(conn *goredis.PoolConn, key []byte, timeout int64) error {
	for _, tp := range TypeNames {
		err := migrateKey(conn, tp, key, timeout)
		if err != nil {
			if err == errNoKey {
				continue
			} else {
				return err
			}
		}
	}

	return nil
}

// unlike redis, restore will try to delete old key first
func restoreCommand(m uhaha.Machine, args []string) (interface{}, error) {
	if len(args) != 4 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := args[1]
	ttl, err := ledis.StrInt64([]byte(args[2]), nil)
	if err != nil {
		return nil, err
	}
	data := args[3]
	db := ldb.GetDBForKeyUnsafe([]byte(key))

	if err = db.Restore([]byte(key), ttl, []byte(data)); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func xdump(db *ledis.DB, tp string, key []byte) ([]byte, error) {
	var err error
	var data []byte
	switch strings.ToUpper(tp) {
	case KVName:
		data, err = db.Dump(key)
	case HashName:
		data, err = db.HDump(key)
	case ListName:
		data, err = db.LDump(key)
	case SetName:
		data, err = db.SDump(key)
	case ZSetName:
		data, err = db.ZDump(key)
	default:
		err = fmt.Errorf("invalid key type %s", tp)
	}
	return data, err
}

func xdel(db *ledis.DB, tp string, key []byte) error {
	var err error
	switch strings.ToUpper(tp) {
	case KVName:
		_, err = db.Del(key)
	case HashName:
		_, err = db.HClear(key)
	case ListName:
		_, err = db.LClear(key)
	case SetName:
		_, err = db.SClear(key)
	case ZSetName:
		_, err = db.ZClear(key)
	default:
		err = fmt.Errorf("invalid key type %s", tp)
	}
	return err
}

func xttl(db *ledis.DB, tp string, key []byte) (int64, error) {
	switch strings.ToUpper(tp) {
	case KVName:
		return db.TTL(key)
	case HashName:
		return db.HTTL(key)
	case ListName:
		return db.LTTL(key)
	case SetName:
		return db.STTL(key)
	case ZSetName:
		return db.ZTTL(key)
	default:
		return 0, fmt.Errorf("invalid key type %s", tp)
	}
}
