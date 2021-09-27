package main

import (
	"fmt"

	"github.com/IceFireDB/kit/pkg/router"
	"github.com/ledisdb/ledisdb/ledis"
)

type ledisDBs struct {
	le      *ledis.Ledis
	slotNum int
	dbs     []*ledis.DB
}

func NewLedisDBs(le *ledis.Ledis, slotNum int) (l *ledisDBs, err error) {
	l = &ledisDBs{
		le:      le,
		slotNum: slotNum,
	}
	l.dbs = make([]*ledis.DB, slotNum)
	for i := 0; i < slotNum; i++ {
		l.dbs[i], err = l.le.Select(i)
		if err != nil {
			return nil, err
		}
	}
	return l, nil
}

func (l *ledisDBs) GetDBForKey(key []byte) (db *ledis.DB, err error) {
	slot := router.MapKey2Slot(key, l.slotNum)
	if slot < 0 || slot >= l.slotNum {
		return nil, fmt.Errorf("invalid db index %d, must in [0, %d]", slot, l.slotNum-1)
	}
	return l.dbs[slot], nil
}

func (l *ledisDBs) GetDBForKeyUnsafe(key []byte) *ledis.DB {
	db, _ := l.GetDBForKey(key)
	return db
}

func (l *ledisDBs) GetDB(slot int) (*ledis.DB, error) {
	if slot < 0 || slot >= l.slotNum {
		return nil, fmt.Errorf("invalid db index %d, must in [0, %d]", slot, l.slotNum-1)
	}
	return l.dbs[slot], nil
}
