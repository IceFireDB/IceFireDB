package main

import (
	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/iface"
	"context"
	orbitdb2 "github.com/gitsrc/IceFireDB/driver/orbitdb"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
	"time"

	//mock "github.com/ipfs/go-ipfs/core/mock"
	//iface "github.com/ipfs/interface-go-ipfs-core"
)


func testingCreateTempDir(t *testing.T, name string) (string, func()) {  //建立临时目录。
	t.Helper()

	path, err := ioutil.TempDir("", name)
	require.NoError(t, err)

	cleanup := func() { os.RemoveAll(path) }
	return path, cleanup
}


func TestIPFSOrbitKeyValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*70)
	defer cancel()

	t.Run("test create mock net ", func(t *testing.T) {
		mock := createMockNet(ctx)
		require.NotNil(t, mock)
	})

	t.Run("test create ipfs node ", func(t *testing.T) {
		node := orbitdb2.CreateIPFSNode(ctx)
		require.NotNil(t, node)
	})

	t.Run("test create api interface ", func(t *testing.T) {
		node := orbitdb2.CreateIPFSNode(ctx)
		ipfs := orbitdb2.CreateCoreAPI(node)
		require.NotNil(t, ipfs)
	})
}

func createMockNet(ctx context.Context) mocknet.Mocknet {
	return mocknet.New(ctx)
}


func TestCreateOpenOrbitDbDriver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//mocknet := createMockNet(ctx)
	node:= orbitdb2.CreateIPFSNode(ctx)
	ipfs := orbitdb2.CreateCoreAPI(node)

	// setup
	var (
		orbit  iface.OrbitDB
		dbPath string
	)
	setup := func(t *testing.T) func() {
		t.Helper()
		var dbPathClean func()
		dbPath, dbPathClean = testingCreateTempDir(t, "db")
		var err error
		orbit, err = orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath})   //实例 句柄。
		require.NoError(t, err)
		cleanup := func() {
			orbit.Close()
			dbPathClean()
		}
		return cleanup
	}

	t.Run("throws an error if  key value  database create failed ", func(t *testing.T) {
		defer setup(t)()
		replicate := false

		db1, err := orbit.KeyValue(ctx, "orbkeyvalue", &orbitdb.CreateDBOptions{Replicate: &replicate})
		require.NoError(t, err)
		require.NotNil(t, db1)

		defer db1.Drop()
		defer db1.Close()

	})

	t.Run("put ", func(t *testing.T) {
		defer setup(t)()
		replicate := false

		db1, err := orbit.KeyValue(ctx, "orbkeyvalue", &orbitdb.CreateDBOptions{Replicate: &replicate})
		require.NoError(t, err)
		require.NotNil(t, db1)

		defer db1.Drop()
		defer db1.Close()

		_, err1 := db1.Put(ctx, "key1", []byte("hello1"))
		require.NoError(t, err1)
	})

	t.Run("get", func(t *testing.T) {

		defer setup(t)()
		replicate := false

		db1, err := orbit.KeyValue(ctx, "orbkeyvalue", &orbitdb.CreateDBOptions{Replicate: &replicate})
		require.NoError(t, err)
		require.NotNil(t, db1)

		defer db1.Drop()
		defer db1.Close()

		_, err1 := db1.Put(ctx, "key1", []byte("hello1"))
		require.NoError(t, err1)

		value, err2 := db1.Get(ctx, "key1")
		require.NoError(t, err2)
		require.Equal(t, string(value), "hello1")
	})
}