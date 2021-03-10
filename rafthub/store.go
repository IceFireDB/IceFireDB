/*
 * @Author: gitsrc
 * @Date: 2020-12-23 13:51:57
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 13:52:24
 * @FilePath: /RaftHub/store.go
 */

package rafthub

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	raftleveldb "github.com/tidwall/raft-leveldb"
	"github.com/tidwall/redlog/v2"
)

type restoreData struct {
	data  interface{}
	ts    int64
	seed  int64
	start int64
}

func dataDirInit(conf Config, log *redlog.Logger) (string, *restoreData) {
	var rdata *restoreData
	dir := filepath.Join(conf.DataDir, conf.Name, conf.NodeID)
	if conf.BackupPath != "" {
		_, err := os.Stat(dir)
		if err == nil {
			log.Warningf("backup restore ignored: "+
				"data directory already exists: path=%s", dir)
			return dir, nil
		}
		log.Printf("restoring backup: path=%s", conf.BackupPath)
		if !os.IsNotExist(err) {
			log.Fatal(err)
		}
		rdata, err = dataDirRestoreBackup(conf, dir, log)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("recovery successful")
	} else {
		if err := os.MkdirAll(dir, 0777); err != nil {
			log.Fatal(err)
		}
	}
	if conf.DataDirReady != nil {
		conf.DataDirReady(dir)
	}
	return dir, rdata
}

func dataDirRestoreBackup(conf Config, dir string, log *redlog.Logger,
) (rdata *restoreData, err error) {
	rdata = new(restoreData)
	f, err := os.Open(conf.BackupPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	rdata.start, rdata.ts, rdata.seed, err = readSnapHead(gr)
	if err != nil {
		return nil, err
	}
	if conf.Restore != nil {
		rdata.data, err = conf.Restore(gr)
		if err != nil {
			return nil, err
		}
	} else if conf.jsonSnaps {
		rdata.data, err = func(rd io.Reader) (data interface{}, err error) {
			return jsonRestore(rd, conf.jsonType)
		}(gr)
		if err != nil {
			return nil, err
		}
	} else {
		rdata.data = conf.InitialData
	}
	return rdata, nil
}

func storeInit(conf Config, dir string, log *redlog.Logger,
) (raft.LogStore, raft.StableStore) {
	switch conf.Backend {
	case Bolt:
		store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "store"))
		if err != nil {
			log.Fatalf("bolt store open: %s", err)
		}
		return store, store
	case LevelDB:
		dur := raftleveldb.High
		if conf.NoSync {
			dur = raftleveldb.Medium
		}
		store, err := raftleveldb.NewLevelDBStore(
			filepath.Join(dir, "store"), dur)
		if err != nil {
			log.Fatalf("leveldb store open: %s", err)
		}
		return store, store
	default:
		log.Fatalf("invalid backend")
	}
	return nil, nil
}
