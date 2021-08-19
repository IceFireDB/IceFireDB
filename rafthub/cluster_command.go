// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

var clusterCommands = map[string]command{
	"help":  command{'s', cmdCLUSTERHELP},
	"info":  command{'s', cmdCLUSTERINFO},
	"slots": command{'s', cmdCLUSTERSLOTS},
	"nodes": command{'s', cmdCLUSTERNODES},
}

// CLUSTER HELP
// help: returns the valid RAFT related commands; []string
func cmdCLUSTERHELP(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	if len(args) != 2 {
		return nil, errWrongNumArgsRaft
	}
	lines := []redcon.SimpleString{
		"CLUSTER INFO",
		"CLUSTER NODES",
		"CLUSTER SLOTS",
	}
	return lines, nil
}

// CLUSTER INFO
// help: returns various redis cluster info; string
func cmdCLUSTERINFO(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	slist, err := ra.getServerList()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	size := len(slist)
	epoch := ra.LastIndex()
	return fmt.Sprintf(""+
		"cluster_state:ok\n"+
		"cluster_slots_assigned:16384\n"+
		"cluster_slots_ok:16384\n"+
		"cluster_slots_pfail:0\n"+
		"cluster_slots_fail:0\n"+
		"cluster_known_nodes:%d\n"+
		"cluster_size:%d\n"+
		"cluster_current_epoch:%d\n"+
		"cluster_my_epoch:%d\n"+
		"cluster_stats_messages_sent:0\n"+
		"cluster_stats_messages_received:0\n",
		size, size, epoch, epoch,
	), nil
}

// CLUSTER SLOTS
// help: returns the cluster slots, which is always all slots being assigned
// to the leader.
func cmdCLUSTERSLOTS(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	slist, err := ra.getServerList()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	var leader serverEntry
	for _, server := range slist {
		if server.leader {
			leader = server
			break
		}
	}
	if !leader.leader {
		return nil, errors.New("CLUSTERDOWN The cluster is down")
	}
	return []interface{}{
		[]interface{}{
			redcon.SimpleInt(0),
			redcon.SimpleInt(16383),
			[]interface{}{
				leader.host(),
				redcon.SimpleInt(leader.port()),
				leader.clusterID(),
			},
		},
	}, nil
}

// CLUSTER NODES
// help: returns the cluster nodes
func cmdCLUSTERNODES(um Machine, ra *raftWrap, args []string,
) (interface{}, error) {
	slist, err := ra.getServerList()
	if err != nil {
		return nil, errRaftConvert(ra, err)
	}
	var leader serverEntry
	for _, server := range slist {
		if server.leader {
			leader = server
			break
		}
	}
	if !leader.leader {
		return nil, errors.New("CLUSTERDOWN The cluster is down")
	}
	leaderID := leader.clusterID()
	var result string
	for _, server := range slist {
		flags := "slave"
		followerOf := leaderID
		if server.leader {
			flags = "master"
			followerOf = "-"
		}
		result += fmt.Sprintf("%s %s:%d@%d %s %s 0 0 connected 0-16383\n",
			server.clusterID(),
			server.host(), server.port(), server.port(),
			flags, followerOf,
		)
	}
	return result, nil
}

func errUnknownClusterCommand(args []string) error {
	var cmd string
	for _, arg := range args {
		cmd += arg + " "
	}
	return fmt.Errorf("unknown subcommand or wrong number of arguments for "+
		"'%s', try CLUSTER HELP",
		strings.TrimSpace(cmd))
}
