package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	deb "runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledisdb/ledisdb/ledis"
)

const (
	GB uint64 = 1024 * 1024 * 1024
	MB uint64 = 1024 * 1024
	KB uint64 = 1024
)

var Delims = []byte("\r\n")

type ExtInfoFunc func() (tit string, metrics []map[string]interface{})

type info struct {
	sync.Mutex

	Server struct {
		OS         string
		ProceessID int
	}
	ExtInfo []ExtInfoFunc
}

func init() {
	serverInfo = new(info)

	serverInfo.Server.OS = runtime.GOOS
	serverInfo.Server.ProceessID = os.Getpid()
	return
}

func (i *info) RegisterExtInfo(f ExtInfoFunc) {
	i.ExtInfo = append(i.ExtInfo, f)
}

func (i *info) Close() {
}

func getMemoryHuman(m uint64) string {
	if m > GB {
		return fmt.Sprintf("%0.3fG", float64(m)/float64(GB))
	} else if m > MB {
		return fmt.Sprintf("%0.3fM", float64(m)/float64(MB))
	} else if m > KB {
		return fmt.Sprintf("%0.3fK", float64(m)/float64(KB))
	} else {
		return fmt.Sprintf("%d", m)
	}
}

func (i *info) Dump(section string) []byte {
	buf := &bytes.Buffer{}
	switch strings.ToLower(section) {
	case "":
		i.dumpAll(buf)
	case "server":
		i.dumpServer(buf)
	case "mem":
		i.dumpMem(buf)
	case "gc":
		i.dumpGC(buf)
	case "store":
		i.dumpStore(buf)
	default:
		buf.WriteString(fmt.Sprintf("# %s\r\n", section))
	}
	// add extension info data
	for _, ifn := range i.ExtInfo {
		tit, metrics := ifn()
		buf.WriteString(fmt.Sprintf("# %s\r\n", tit))
		pairs := make([]infoPair, len(metrics))
		j := 0
		for _, v := range metrics {
			for key, val := range v {
				pairs[j] = infoPair{
					Key:   key,
					Value: val,
				}
				j++
			}
		}
		i.dumpPairs(buf, pairs...)
	}
	return buf.Bytes()
}

type infoPair struct {
	Key   string
	Value interface{}
}

func (i *info) dumpAll(buf *bytes.Buffer) {
	i.dumpServer(buf)
	buf.Write(Delims)
	i.dumpStore(buf)
	buf.Write(Delims)
	i.dumpMem(buf)
	buf.Write(Delims)
	i.dumpGC(buf)
	buf.Write(Delims)
	// i.dumpReplication(buf)
}

func (i *info) dumpServer(buf *bytes.Buffer) {
	buf.WriteString("# Server\r\n")

	i.dumpPairs(buf, infoPair{"os", i.Server.OS},
		infoPair{"process_id", i.Server.ProceessID},
		infoPair{"addr", conf.Addr},
		infoPair{"http_addr", ldsCfg.HttpAddr},
		infoPair{"readonly", ldsCfg.Readonly},
		infoPair{"goroutine_num", runtime.NumGoroutine()},
		infoPair{"cgo_call_num", runtime.NumCgoCall()},
		infoPair{"resp_client_num", atomic.LoadInt64(&respClientNum)},
		infoPair{"ledisdb_version", ledis.Version},
	)
}

func (i *info) dumpMem(buf *bytes.Buffer) {
	buf.WriteString("# Mem\r\n")

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	i.dumpPairs(buf, infoPair{"mem_alloc", getMemoryHuman(mem.Alloc)},
		infoPair{"mem_sys", getMemoryHuman(mem.Sys)},
		infoPair{"mem_looksups", getMemoryHuman(mem.Lookups)},
		infoPair{"mem_mallocs", getMemoryHuman(mem.Mallocs)},
		infoPair{"mem_frees", getMemoryHuman(mem.Frees)},
		infoPair{"mem_total", getMemoryHuman(mem.TotalAlloc)},
		infoPair{"mem_heap_alloc", getMemoryHuman(mem.HeapAlloc)},
		infoPair{"mem_heap_sys", getMemoryHuman(mem.HeapSys)},
		infoPair{"mem_head_idle", getMemoryHuman(mem.HeapIdle)},
		infoPair{"mem_head_inuse", getMemoryHuman(mem.HeapInuse)},
		infoPair{"mem_head_released", getMemoryHuman(mem.HeapReleased)},
		infoPair{"mem_head_objects", mem.HeapObjects},
	)
}

const (
	gcTimeFormat = "2006/01/02 15:04:05.000"
)

func (i *info) dumpGC(buf *bytes.Buffer) {
	buf.WriteString("# GC\r\n")

	count := 5

	var st deb.GCStats
	st.Pause = make([]time.Duration, count)
	// st.PauseQuantiles = make([]time.Duration, count)
	deb.ReadGCStats(&st)

	h := make([]string, 0, count)

	for i := 0; i < count && i < len(st.Pause); i++ {
		h = append(h, st.Pause[i].String())
	}

	i.dumpPairs(buf, infoPair{"gc_last_time", st.LastGC.Format(gcTimeFormat)},
		infoPair{"gc_num", st.NumGC},
		infoPair{"gc_pause_total", st.PauseTotal.String()},
		infoPair{"gc_pause_history", strings.Join(h, ",")},
	)
}

func (i *info) dumpStore(buf *bytes.Buffer) {
	buf.WriteString("# Store\r\n")
	s := le.StoreStat()

	// getNum := s.GetNum.Get()
	// getTotalTime := s.GetTotalTime.Get()

	// gt := int64(0)
	// if getNum > 0 {
	// 	gt = getTotalTime.Nanoseconds() / (getNum * 1e3)
	// }

	// commitNum := s.BatchCommitNum.Get()
	// commitTotalTime := s.BatchCommitTotalTime.Get()

	// ct := int64(0)
	// if commitNum > 0 {
	// 	ct = commitTotalTime.Nanoseconds() / (commitNum * 1e3)
	// }

	i.dumpPairs(buf, infoPair{"name", ldsCfg.DBName},
		infoPair{"get", s.GetNum},
		infoPair{"get_missing", s.GetMissingNum},
		infoPair{"put", s.PutNum},
		infoPair{"delete", s.DeleteNum},
		infoPair{"get_total_time", s.GetTotalTime.Get().String()},
		infoPair{"iter", s.IterNum},
		infoPair{"iter_seek", s.IterSeekNum},
		infoPair{"iter_close", s.IterCloseNum},
		infoPair{"batch_commit", s.BatchCommitNum},
		infoPair{"batch_commit_total_time", s.BatchCommitTotalTime.Get().String()},
	)
}

func (i *info) dumpPairs(buf *bytes.Buffer, pairs ...infoPair) {
	for _, v := range pairs {
		buf.WriteString(fmt.Sprintf("%s:%v\r\n", v.Key, v.Value))
	}
}
