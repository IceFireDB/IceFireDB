/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:24:10
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:24:29
 * @FilePath: /RaftHub/serviceEntry.go
 */

package rafthub

import (
	"io"
	"net"
)

type serviceEntry struct {
	sniff func(rd io.Reader) bool
	serve func(s Service, ln net.Listener)
}
