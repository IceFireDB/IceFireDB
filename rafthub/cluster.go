/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:38:14
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:38:36
 * @FilePath: /RaftHub/cluster.go
 */

package rafthub

import (
	"crypto/tls"
	"errors"
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/hashicorp/raft"
)

func getClusterLastIndex(ra *raftWrap, tlscfg *tls.Config, auth string,
) (uint64, error) {
	if ra.State() == raft.Leader {
		return ra.LastIndex(), nil
	}
	addr := getLeaderAdvertiseAddr(ra)
	if addr == "" {
		return 0, errLeaderUnknown
	}
	conn, err := RedisDial(addr, auth, tlscfg)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	args, err := redis.Strings(conn.Do("raft", "info", "last_log_index"))
	if err != nil {
		return 0, err
	}
	if len(args) != 2 {
		return 0, errors.New("invalid response")
	}
	lastIndex, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return lastIndex, nil
}
