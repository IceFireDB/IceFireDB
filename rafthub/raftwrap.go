/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:10:40
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:13:51
 * @FilePath: /RaftHub/raftwrap.go
 */

package rafthub

import (
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
)

type raftWrap struct {
	*raft.Raft
	conf      Config
	advertise string
	mu        sync.RWMutex
	extra     map[string]serverExtra
}

func (ra *raftWrap) getExtraForAddr(addr string) (extra serverExtra, ok bool) {
	if ra.advertise == "" {
		return extra, false
	}
	ra.mu.RLock()
	defer ra.mu.RUnlock()
	for eaddr, extra := range ra.extra {
		if eaddr == addr || extra.advertise == addr ||
			extra.remoteAddr == addr {
			return extra, true
		}
	}
	return extra, false
}

func (ra *raftWrap) getServerList() ([]serverEntry, error) {
	leader := string(ra.Leader())
	f := ra.GetConfiguration()
	err := f.Error()
	if err != nil {
		return nil, err
	}
	cfg := f.Configuration()
	var servers []serverEntry
	for _, s := range cfg.Servers {
		var entry serverEntry
		entry.id = string(s.ID)
		entry.address = string(s.Address)
		extra, ok := ra.getExtraForAddr(entry.address)
		if ok {
			entry.resolve = extra.remoteAddr
		} else {
			entry.resolve = entry.address
		}
		entry.leader = entry.resolve == leader || entry.address == leader
		servers = append(servers, entry)
	}
	return servers, nil
}

func errRaftConvert(ra *raftWrap, err error) error {
	if ra.conf.TryErrors {
		if err == raft.ErrNotLeader {
			leader := getLeaderAdvertiseAddr(ra)
			if leader != "" {
				return fmt.Errorf("TRY %s", leader)
			}
		}
		return err
	}
	switch err {
	case raft.ErrNotLeader, raft.ErrLeadershipLost,
		raft.ErrLeadershipTransferInProgress:
		leader := getLeaderAdvertiseAddr(ra)
		if leader != "" {
			return fmt.Errorf("MOVED 0 %s", leader)
		}
		fallthrough
	case raft.ErrRaftShutdown, raft.ErrTransportShutdown:
		return fmt.Errorf("CLUSTERDOWN %s", err)
	}
	return err
}

func getLeaderAdvertiseAddr(ra *raftWrap) string {
	leader := string(ra.Leader())
	if ra.advertise == "" {
		return leader
	}
	if leader == "" {
		return ""
	}
	extra, ok := ra.getExtraForAddr(leader)
	if !ok {
		return ""
	}
	return extra.advertise
}
