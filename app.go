package main

import (
	"github.com/ledisdb/ledisdb/ledis"
	"github.com/siddontang/go/hack"
	"github.com/siddontang/goredis"
	"strings"
	"sync"
)

const (
	KV   ledis.DataType = ledis.KV
	LIST                = ledis.LIST
	HASH                = ledis.HASH
	SET                 = ledis.SET
	ZSET                = ledis.ZSET
)

const (
	KVName   = ledis.KVName
	ListName = ledis.ListName
	HashName = ledis.HashName
	SetName  = ledis.SetName
	ZSetName = ledis.ZSetName
)

var TypeNames = []string{KVName, ListName, HashName, SetName, ZSetName}

type App struct {
	migrateM          sync.Mutex
	migrateClients    map[string]*goredis.Client
	migrateKeyLockers map[string]*migrateKeyLocker
}

func (app *App) getMigrateClient(addr string) *goredis.Client {
	app.migrateM.Lock()

	mc, ok := app.migrateClients[addr]
	if !ok {
		mc = goredis.NewClient(addr, "")
		app.migrateClients[addr] = mc
	}

	app.migrateM.Unlock()

	return mc
}

func NewApp() (*App, error) {
	app := new(App)
	app.migrateClients = make(map[string]*goredis.Client)
	app.newMigrateKeyLockers()

	return app, nil
}

func (app *App) Close() {
}

type migrateKeyLocker struct {
	m sync.Mutex

	locks map[string]struct{}
}

func (m *migrateKeyLocker) Lock(key []byte) bool {
	m.m.Lock()
	defer m.m.Unlock()

	k := hack.String(key)
	_, ok := m.locks[k]
	if ok {
		return false
	}
	m.locks[k] = struct{}{}
	return true
}

func (m *migrateKeyLocker) Unlock(key []byte) {
	m.m.Lock()
	defer m.m.Unlock()

	delete(m.locks, hack.String(key))
}

func newMigrateKeyLocker() *migrateKeyLocker {
	m := new(migrateKeyLocker)

	m.locks = make(map[string]struct{})

	return m
}

func (app *App) newMigrateKeyLockers() {
	app.migrateKeyLockers = make(map[string]*migrateKeyLocker)

	app.migrateKeyLockers[KVName] = newMigrateKeyLocker()
	app.migrateKeyLockers[HashName] = newMigrateKeyLocker()
	app.migrateKeyLockers[ListName] = newMigrateKeyLocker()
	app.migrateKeyLockers[SetName] = newMigrateKeyLocker()
	app.migrateKeyLockers[ZSetName] = newMigrateKeyLocker()
}

func (app *App) migrateKeyLock(tp string, key []byte) bool {
	l, ok := app.migrateKeyLockers[strings.ToUpper(tp)]
	if !ok {
		return false
	}

	return l.Lock(key)
}

func (app *App) migrateKeyUnlock(tp string, key []byte) {
	l, ok := app.migrateKeyLockers[strings.ToUpper(tp)]
	if !ok {
		return
	}

	l.Unlock(key)
}
