/*
 * @Author: gitsrc
 * @Date: 2020-12-23 13:50:54
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:36:35
 * @FilePath: /RaftHub/snapshot.go
 */

package rafthub

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tidwall/redlog/v2"
)

// A Snapshot is an interface that allows for Raft snapshots to be taken.
type Snapshot interface {
	Persist(io.Writer) error
	Done(path string)
}

type fsmSnap struct {
	id    string
	dir   string
	snap  Snapshot
	ts    int64
	seed  int64
	start int64
}

func (s *fsmSnap) Persist(sink raft.SnapshotSink) error {
	s.id = sink.ID()
	gw := gzip.NewWriter(sink)
	var head [32]byte
	copy(head[:], "SNAP0001")
	binary.LittleEndian.PutUint64(head[8:], uint64(s.start))
	binary.LittleEndian.PutUint64(head[16:], uint64(s.ts))
	binary.LittleEndian.PutUint64(head[24:], uint64(s.seed))
	n, err := gw.Write(head[:])
	if err != nil {
		return err
	}
	if n != 32 {
		return errors.New("invalid write")
	}
	if err := s.snap.Persist(gw); err != nil {
		return err
	}
	return gw.Close()
}

func (s *fsmSnap) Release() {
	path := filepath.Join(s.dir, "snapshots", s.id, "state.bin")
	if _, err := readSnapInfo(s.id, path); err != nil {
		path = ""
	}
	s.snap.Done(path)
}

type jsonSnapshotType struct{ jsdata []byte }

func (s *jsonSnapshotType) Done(path string) {}
func (s *jsonSnapshotType) Persist(wr io.Writer) error {
	_, err := wr.Write(s.jsdata)
	return err
}
func jsonSnapshot(data interface{}) (Snapshot, error) {
	if data == nil {
		return &jsonSnapshotType{}, nil
	}
	jsdata, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &jsonSnapshotType{jsdata: jsdata}, nil
}

func jsonRestore(rd io.Reader, typ reflect.Type) (interface{}, error) {
	jsdata, err := ioutil.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	if typ == nil {
		return nil, nil
	}
	data := reflect.New(typ).Interface()
	if err = json.Unmarshal(jsdata, data); err != nil {
		return nil, err
	}
	return data, err
}

func readSnapHead(r io.Reader) (start, ts, seed int64, err error) {
	var head [32]byte
	n, err := io.ReadFull(r, head[:])
	if err != nil {
		return 0, 0, 0, err
	}
	if n != 32 {
		return 0, 0, 0, errors.New("invalid read")
	}
	if string(head[:8]) != "SNAP0001" {
		return 0, 0, 0, errors.New("invalid snapshot signature")
	}
	start = int64(binary.LittleEndian.Uint64(head[8:]))
	ts = int64(binary.LittleEndian.Uint64(head[16:]))
	seed = int64(binary.LittleEndian.Uint64(head[24:]))
	return start, ts, seed, nil
}

func snapshotInit(conf Config, dir string, m *machine, hclogger hclog.Logger,
	log *redlog.Logger,
) raft.SnapshotStore {
	snaps, err := raft.NewFileSnapshotStoreWithLogger(dir, 3, hclogger)
	if err != nil {
		log.Fatal(err)
	}
	m.snaps = snaps
	return snaps
}

func readSnapInfo(id, path string) (map[string]string, error) {
	status := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	_, ts, _, err := readSnapHead(gr)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	status["timestamp"] = fmt.Sprint(ts)
	status["id"] = id
	status["size"] = fmt.Sprint(fi.Size())
	return status, nil
}
