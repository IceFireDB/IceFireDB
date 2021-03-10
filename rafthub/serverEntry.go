/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:11:53
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:12:18
 * @FilePath: /RaftHub/serverEntry.go
 */

package rafthub

import (
	"crypto/sha1"
	"encoding/hex"
	"strconv"
	"strings"
)

type serverEntry struct {
	id      string
	address string
	resolve string
	leader  bool
}

func (e *serverEntry) clusterID() string {
	src := sha1.Sum([]byte(e.id))
	return hex.EncodeToString(src[:])
}

func (e *serverEntry) host() string {
	idx := strings.LastIndexByte(e.address, ':')
	if idx == -1 {
		return ""
	}
	return e.address[:idx]
}

func (e *serverEntry) port() int {
	idx := strings.LastIndexByte(e.address, ':')
	if idx == -1 {
		return 0
	}
	port, _ := strconv.Atoi(e.address[idx+1:])
	return port
}
