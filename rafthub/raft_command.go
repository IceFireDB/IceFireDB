// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

var raftCommands = map[string]command{
	"help":     command{'s', cmdRAFTHELP},
	"info":     command{'s', cmdRAFTINFO},
	"leader":   command{'s', cmdRAFTLEADER},
	"snapshot": command{'s', cmdRAFTSNAPSHOT},
	"server":   command{'s', cmdRAFTSERVER},
}

func errUnknownRaftCommand(args []string) error {
	var cmd string
	for _, arg := range args {
		cmd += arg + " "
	}
	return fmt.Errorf("unknown raft command '%s', try RAFT HELP",
		strings.TrimSpace(cmd))
}

// RAFT HELP
// help: returns the valid RAFT related commands; []string
func cmdRAFTHELP(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumArgsRaft
	}
	lines := []redcon.SimpleString{
		"RAFT LEADER",
		"RAFT INFO [pattern]",

		"RAFT SERVER LIST",
		"RAFT SERVER ADD id address",
		"RAFT SERVER REMOVE id",

		"RAFT SNAPSHOT NOW",
		"RAFT SNAPSHOT LIST",
		"RAFT SNAPSHOT FILE id",
		"RAFT SNAPSHOT READ id [RANGE start end]",
	}
	return lines, nil
}

// VERSION
func cmdVERSION(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, ErrWrongNumArgs
	}
	return getBaseMachine(um).vers, nil
}

// MACHINE [HUMAN]
func cmdMACHINE(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	var human bool
	switch len(args) {
	case 1:
	case 2:
		arg := strings.ToLower(args[1])
		if arg == "human" || arg == "h" {
			human = true
		} else {
			return false, ErrSyntax
		}
	default:
		return false, ErrWrongNumArgs
	}
	status := make(map[string]string)
	now := m.Now().UnixNano()
	uptime := now - m.start
	boottime := m.start
	if human {
		status["now"] = time.Unix(0, now).UTC().Format(time.RFC3339Nano)
		status["uptime"] = time.Duration(uptime).String()
		status["boottime"] = time.Unix(0, boottime).UTC().Format(
			time.RFC3339Nano)
	} else {
		status["now"] = fmt.Sprint(now)
		status["uptime"] = fmt.Sprint(uptime)
		status["boottime"] = fmt.Sprint(boottime)
	}
	return status, nil
}

// TICK timestamp-int64 random-int64
// help: updates the machine timestamp and random seed. It's not possible to
//       directly call this from a client service. It can only be called by
//       its own internal server instance.
func cmdTICK(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) != 3 {
		return nil, ErrWrongNumArgs
	}
	ts, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, err
	}
	if ts < 0 || ts <= m.ts {
		return nil, errors.New("timestamp is not monotonic")
	}
	seed, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, err
	}
	if seed == m.seed {
		return nil, errors.New("random number has not changed")
	}
	m.seed = seed
	m.ts = ts
	if m.start == 0 {
		m.start = m.ts
	}
	if m.tick != nil {
		// call the user defined tick function
		m.tick(m)
	}
	// Do not returns anything of value because it will be overwritten by the
	// Apply() function.
	return nil, nil
}

// CLUSTER subcommand args...
// help: calls a system-level cluster operation.
func cmdCLUSTER(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) < 2 {
		return nil, errWrongNumArgsCluster
	}
	args[1] = strings.ToLower(args[1])
	rcmd, ok := clusterCommands[args[1]]
	if !ok {
		return nil, errUnknownClusterCommand(args[:2])
	}
	return rcmd.fn(m, ra, args)
}

// RAFT subcommand args...
// help: calls a system-level raft operation.
func cmdRAFT(um Machine, ra *raftWrap, args []string) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) < 2 {
		return nil, errWrongNumArgsRaft
	}
	args[1] = strings.ToLower(args[1])
	rcmd, ok := raftCommands[args[1]]
	if !ok {
		return nil, errUnknownRaftCommand(args[:2])
	}
	return rcmd.fn(m, ra, args)
}

// RAFT LEADER
// help: returns the current leader; string
func cmdRAFTLEADER(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumArgsRaft
	}
	return getLeaderAdvertiseAddr(ra), nil
}

// RAFT SERVER subcommand args...
func cmdRAFTSERVER(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) < 3 {
		return nil, errWrongNumArgsRaft
	}
	switch strings.ToLower(args[2]) {
	case "list":
		return cmdRAFTSERVERLIST(m, ra, args)
	case "add":
		return cmdRAFTSERVERADD(m, ra, args)
	case "remove":
		return cmdRAFTSERVERREMOVE(m, ra, args)
	default:
		return nil, fmt.Errorf("unknown raft command '%s', try RAFT HELP",
			args[1])
	}
}

// RAFT SERVER LIST
// help: returns a list of the servers in the cluster
func cmdRAFTSERVERLIST(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 3 {
		return nil, errWrongNumArgsRaft
	}
	servers, err := ra.getServerList()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	var res [][]string
	for _, s := range servers {
		res = append(res, []string{
			"id", s.id,
			"address", s.address,
			"leader", fmt.Sprint(s.leader),
		})
	}
	return res, nil
}

// RAFT SERVER REMOVE id
// help: removes a server from the cluster; bool
func cmdRAFTSERVERREMOVE(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 4 {
		return nil, errWrongNumArgsRaft
	}
	f := ra.RemoveServer(raft.ServerID(string(args[3])), 0, 0)
	err := f.Error()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	return true, nil
}

// RAFT SERVER ADD id address
// help: Returns true if server added, or error; bool
func cmdRAFTSERVERADD(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 5 {
		return nil, errWrongNumArgsRaft
	}
	err := ra.AddVoter(raft.ServerID(args[3]), raft.ServerAddress(args[4]),
		0, 0).Error()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	return true, nil
}

// RAFT INFO [pattern]
// help: returns various raft related info; map[string]string
func cmdRAFTINFO(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	pattern := "*"
	switch len(args) {
	case 2:
	case 3:
		pattern = args[2]
	default:
		return nil, errWrongNumArgsRaft
	}
	if pattern == "state" {
		// Fast path to avoid locks. Under the hood there's only a single
		// atomic load
		return []string{"state", ra.State().String()}, nil
	}

	stats := ra.Stats()
	m.mu.RLock()
	behind := m.logRemain
	percent := m.logPercent
	m.mu.RUnlock()
	stats["logs_behind"] = fmt.Sprint(behind)
	stats["logs_loaded_percent"] = fmt.Sprintf("%0.1f", percent*100)
	final := make(map[string]string)
	for key, value := range stats {
		if match.Match(key, pattern) {
			final[key] = value
		}
	}
	return final, nil
}

// RAFT SNAPSHOT subcommand args...
func cmdRAFTSNAPSHOT(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	m := getBaseMachine(um)
	if m == nil {
		return nil, ErrInvalid
	}
	if len(args) < 3 {
		return nil, errWrongNumArgsRaft
	}
	switch strings.ToLower(args[2]) {
	case "now":
		return cmdRAFTSNAPSHOTNOW(m, ra, args)
	case "list":
		return cmdRAFTSNAPSHOTLIST(m, ra, args)
	case "read":
		return cmdRAFTSNAPSHOTREAD(m, ra, args)
	case "file":
		return cmdRAFTSNAPSHOTFILE(m, ra, args)
	default:
		return nil, fmt.Errorf("unknown raft command '%s', try RAFT HELP",
			args[1])
	}
}

// RAFT SNAPSHOT NOW
// help: takes a snapshot of the data and returns information relating to the
//       resulting snapshot; map[string]string
func cmdRAFTSNAPSHOTNOW(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 3 {
		return nil, errWrongNumArgsRaft
	}
	m.mu.Lock()
	if m.snap {
		m.mu.Unlock()
		return nil, errors.New("in progress")
	}
	m.snap = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.snap = false
		m.mu.Unlock()
	}()
	f := ra.Snapshot()
	err := f.Error()
	if err != nil {
		return nil, err
	}
	meta, rd, err := f.Open()
	if err != nil {
		return nil, err
	}
	if err := rd.Close(); err != nil {
		return nil, err
	}
	path := filepath.Join(m.dir, "snapshots", meta.ID, "state.bin")
	info, err := readSnapInfo(meta.ID, path)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// RAFT SNAPSHOT LIST
// help: returns a list of the current snapshots on disk. []map[string]string
func cmdRAFTSNAPSHOTLIST(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 3 {
		return nil, errWrongNumArgsRaft
	}
	list, err := m.snaps.List()
	if err != nil {
		return nil, err
	}
	var snaps []map[string]string
	for _, meta := range list {
		path := filepath.Join(m.dir, "snapshots", meta.ID, "state.bin")
		info, err := readSnapInfo(meta.ID, path)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, info)
	}
	return snaps, nil
}

// RAFT SNAPSHOT FILE id
// help: returns the path to the snapshot file; string
func cmdRAFTSNAPSHOTFILE(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 4 {
		return nil, errWrongNumArgsRaft
	}
	var err error
	path := filepath.Join(m.dir, "snapshots", args[3], "state.bin")
	if path, err = filepath.Abs(path); err != nil {
		return nil, err
	}
	return path, nil
}

// RAFT SNAPSHOT READ id [RANGE offset limit]
// help: reads the contents of a snapshot file; []byte
func cmdRAFTSNAPSHOTREAD(m *machine, ra *raftWrap, args []string,
) (interface{}, error) {
	var id string
	var offset, limit int64
	var allBytes bool
	switch len(args) {
	case 4:
		allBytes = true
	case 7:
		if strings.ToLower(args[4]) != "range" {
			return nil, ErrSyntax
		}
		var err error
		offset, err = strconv.ParseInt(args[5], 10, 64)
		if err != nil {
			return nil, ErrSyntax
		}
		limit, err = strconv.ParseInt(args[6], 10, 64)
		if err != nil {
			return nil, ErrSyntax
		}
		if offset < 0 || limit <= 0 {
			return nil, ErrSyntax
		}
	default:
		return nil, errWrongNumArgsRaft
	}
	id = args[3]
	path := filepath.Join(m.dir, "snapshots", id, "state.bin")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var bytes []byte
	if allBytes {
		bytes, err = ioutil.ReadAll(f)
		if err != nil {
			return nil, err
		}
	} else {
		if _, err := f.Seek(offset, 0); err != nil {
			return nil, err
		}
		packet := make([]byte, 4096)
		for int64(len(bytes)) < limit {
			n, err := f.Read(packet)
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			bytes = append(bytes, packet[:n]...)
		}
		if int64(len(bytes)) > limit {
			bytes = bytes[:limit]
		}
	}
	return bytes, nil
}
