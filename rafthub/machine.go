// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/hashicorp/raft"
	"github.com/tidwall/redlog/v2"
)

// The Machine interface is passed to every command. It includes the user data
// and various utilities that should be used from Write, Read, and Intermediate
// commands.
//
// It's important to note that the Data(), Now(), and Rand() functions can be
// used safely for Write and Read commands, but are not available for
// Intermediate commands. The Context() is ONLY available for Intermediate
// commands.
//
// A call to Rand() and Now() from inside of a Read command will always return
// back the same last known value of it's respective type. While, from a Write
// command, you'll get freshly generated values. This is to ensure that
// the every single command ALWAYS generates the same series of data on every
// server.
type Machine interface {
	// Data is the original user data interface that was assigned at startup.
	// It's safe to alter the data in this interface while inside a Write
	// command, but it's only safe to read from this interface for Read
	// commands.
	// Returns nil for Intermediate Commands.
	Data() interface{}
	// Now generates a stable timestamp that is synced with internet time
	// and for Write commands is always monotonical increasing. It's made to
	// be a trusted source of time for performing operations on the user data.
	// Always use this function instead of the builtin time.Now().
	// Returns nil for Intermediate Commands.
	Now() time.Time
	// Rand is a random number generator that must be used instead of the
	// standard Go packages `crypto/rand` and `math/rand`. For Write commands
	// the values returned from this generator are crypto seeded, guaranteed
	// to be reproduced in exact order when the server restarts, and identical
	// across all machines in the cluster. The underlying implementation is
	// PCG. Check out http://www.pcg-random.org/ for more information.
	// Returns nil for Intermediate Commands.
	Rand() Rand
	// Utility logger for printing information to the local server log.
	Log() Logger
	// Context returns the connection context that was defined in from the
	// Config.ConnOpened callback. Only available for Intermediate commands.
	// Returns nil for Read and Write Commands.
	Context() interface{}
}

type machine struct {
	snapshot   func(data interface{}) (Snapshot, error)
	restore    func(rd io.Reader) (data interface{}, err error)
	connOpened func(addr string) (context interface{}, accept bool)
	connClosed func(context interface{}, addr string)
	jsonSnaps  bool               //
	jsonType   reflect.Type       //
	snaps      raft.SnapshotStore //
	dir        string             //
	vers       string             // version line
	tick       func(m Machine)    //
	created    int64              // machine instance created timestamp
	commands   map[string]command // command table
	catchall   command            // catchall command
	log        Logger             // shared logger
	openReads  bool               // open reads on by default
	tickDelay  time.Duration      // ticker delay

	mu           sync.RWMutex // protect all things in group
	firstIndex   uint64       // first applied index
	appliedIndex uint64       // last applied index (stable state)
	readers      int32        // (atomic counter) number of current readers
	tickedIndex  uint64       // index of last tick
	tickedTerm   uint64       // term of last tick
	tickedSig    *sync.Cond   // signal when ticked
	logPercent   float64      // percentage of log loaded
	logRemain    uint64       // non-applied log entries
	logLoaded    int32        // (atomic bool) log is loaded, allow open reads
	snap         bool         // snapshot in progress
	start        int64        // !! PERSISTED !! first non-zero timestamp
	ts           int64        // !! PERSISTED !! current timestamp
	seed         int64        // !! PERSISTED !! current seed
	data         interface{}  // !! PERSISTED !! user data

	wrC chan *writeRequestFuture
}

func (m *machine) Apply(l *raft.Log) interface{} {
	packet, err := snappy.Decode(nil, l.Data)
	if err != nil {
		m.log.Panic(err)
	}
	m.mu.Lock()
	defer func() {
		m.appliedIndex = l.Index
		if m.firstIndex == 0 {
			m.firstIndex = m.appliedIndex
		}
		m.mu.Unlock()
	}()
	numReqs, n := binary.Uvarint(packet)
	if n <= 0 {
		m.log.Panic("invalid apply")
	}
	packet = packet[n:]
	resps := make([]applyResp, numReqs)
	for i := 0; i < int(numReqs); i++ {
		numArgs, n := binary.Uvarint(packet)
		if n <= 0 {
			m.log.Panic("invalid apply")
		}
		packet = packet[n:]
		args := make([]string, numArgs)
		for i := 0; i < len(args); i++ {
			argLen, n := binary.Uvarint(packet)
			if n <= 0 {
				m.log.Panic("invalid apply")
			}
			packet = packet[n:]
			args[i] = string(packet[:argLen])
			packet = packet[argLen:]
		}
		if len(args) == 0 {
			resps[i] = applyResp{nil, 0, nil}
		} else {
			cmdName := strings.ToLower(string(args[0]))
			cmd := m.commands[cmdName]
			if cmd.kind != 'w' {
				m.log.Panicf("invalid apply '%c', command: '%s'",
					cmd.kind, cmdName)
			}
			tick := cmdName == "tick"
			if m.start == 0 && !tick {
				// This is in fact the leader, but because the machine has yet
				// to receive a valid tick command, we'll treat this as if the
				// server *is not* the leader.
				resps[i] = applyResp{nil, 0, raft.ErrNotLeader}
			} else {
				start := time.Now()
				res, err := cmd.fn(m, nil, args)
				if tick {
					// return only the index and term
					res = raft.Log{Index: l.Index, Term: l.Term}
				}
				resps[i] = applyResp{res, time.Since(start), err}
			}
		}
	}
	return resps
}

func (m *machine) Data() interface{} {
	return m.data
}

func (m *machine) Log() Logger {
	return m.log
}

func (m *machine) Rand() Rand {
	return m
}

func (m *machine) Uint32() uint32 {
	seed := rincr(rincr(m.seed)) // twice called intentionally
	x := rgen(seed)
	if atomic.LoadInt32(&m.readers) == 0 {
		m.seed = seed
	}
	return x
}

func (m *machine) Uint64() uint64 {
	return (uint64(m.Uint32()) << 32) | uint64(m.Uint32())
}

func (m *machine) Int() int {
	return int(m.Uint64() << 1 >> 1)
}

func (m *machine) Float64() float64 {
	return float64(m.Uint32()) / 4294967296.0
}

func (m *machine) Read(p []byte) (n int, err error) {
	seed := rincr(m.seed)
	for len(p) >= 4 {
		seed = rincr(seed)
		binary.LittleEndian.PutUint32(p, rgen(seed))
		p = p[4:]
	}
	if len(p) > 0 {
		var last [4]byte
		seed = rincr(seed)
		binary.LittleEndian.PutUint32(last[:], rgen(seed))
		for i := 0; i < len(p); i++ {
			p[i] = last[i]
		}
	}
	if atomic.LoadInt32(&m.readers) == 0 {
		m.seed = seed
	}
	return len(p), nil
}

var _ Machine = &machine{}

func machineInit(conf Config, dir string, rdata *restoreData,
	log *redlog.Logger,
) *machine {
	m := new(machine)
	m.dir = dir
	m.vers = versline(conf)
	m.tickedSig = sync.NewCond(&m.mu)
	m.created = time.Now().UnixNano()
	m.wrC = make(chan *writeRequestFuture, 1024)
	m.tickDelay = conf.TickDelay
	m.openReads = conf.OpenReads
	if rdata != nil {
		m.data = rdata.data
		m.start = rdata.start
		m.seed = rdata.seed
		m.ts = rdata.ts
	} else {
		m.data = conf.InitialData
	}
	m.log = log
	m.connClosed = conf.ConnClosed
	m.connOpened = conf.ConnOpened
	m.snapshot = conf.Snapshot
	m.restore = conf.Restore
	m.jsonSnaps = conf.jsonSnaps
	m.jsonType = conf.jsonType
	m.tick = conf.Tick
	m.commands = map[string]command{
		"tick":    command{'w', cmdTICK},
		"raft":    command{'s', cmdRAFT},
		"cluster": command{'s', cmdCLUSTER},
		"machine": command{'r', cmdMACHINE},
		"version": command{'s', cmdVERSION},
	}
	if conf.TryErrors {
		delete(m.commands, "cluster")
	}
	for k, v := range conf.cmds {
		if _, ok := m.commands[k]; !ok {
			m.commands[k] = v
		}
	}
	m.catchall = conf.catchall
	return m
}

func (m *machine) Now() time.Time {
	ts := m.ts
	if atomic.LoadInt32(&m.readers) == 0 {
		m.ts++
	}
	return time.Unix(0, ts).UTC()
}

// intermediateMachine wraps the machine in a connection context
type intermediateMachine struct {
	context interface{}
	m       *machine
}

var _ Machine = intermediateMachine{}

func (m intermediateMachine) Now() time.Time       { return time.Time{} }
func (m intermediateMachine) Context() interface{} { return m.context }
func (m intermediateMachine) Log() Logger          { return m.m.log }
func (m intermediateMachine) Rand() Rand           { return nil }
func (m intermediateMachine) Data() interface{}    { return nil }

func getBaseMachine(m Machine) *machine {
	switch m := m.(type) {
	case intermediateMachine:
		return m.m
	case *machine:
		return m
	default:
		return nil
	}
}

func (m *machine) Restore(rc io.ReadCloser) error {
	restore := m.restore
	if restore == nil {
		if m.jsonSnaps {
			restore = func(rd io.Reader) (data interface{}, err error) {
				return jsonRestore(rd, m.jsonType)
			}
		} else {
			return errors.New("snapshot restoring is disabled")
		}
	}
	gr, err := gzip.NewReader(rc)
	if err != nil {
		return err
	}
	start, ts, seed, err := readSnapHead(gr)
	if err != nil {
		return err
	}
	m.start = start
	m.ts = ts
	m.seed = seed
	m.data, err = restore(gr)
	return err
}

func (m *machine) Snapshot() (raft.FSMSnapshot, error) {
	snapshot := m.snapshot
	if snapshot == nil {
		if m.jsonSnaps {
			snapshot = jsonSnapshot
		} else {
			return nil, errors.New("snapshots are disabled")
		}
	}
	usnap, err := snapshot(m.Data())
	if err != nil {
		return nil, err
	}
	snap := &fsmSnap{
		dir:   m.dir,
		snap:  usnap,
		seed:  m.seed,
		ts:    m.ts,
		start: m.start,
	}
	return snap, nil
}

func (m *machine) Context() interface{} {
	return nil
}
