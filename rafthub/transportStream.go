/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:44:58
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:45:18
 * @FilePath: /RaftHub/transportStream.go
 */

package rafthub

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type transportStream struct {
	net.Listener
	auth   string
	tlscfg *tls.Config
}

func (s *transportStream) Dial(addr raft.ServerAddress, timeout time.Duration,
) (conn net.Conn, err error) {
	if timeout <= 0 {
		if s.tlscfg != nil {
			conn, err = tls.Dial("tcp", string(addr), s.tlscfg)
		} else {
			conn, err = net.Dial("tcp", string(addr))
		}
	} else {
		if s.tlscfg != nil {
			conn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout},
				"tcp", string(addr), s.tlscfg)
		} else {
			conn, err = net.DialTimeout("tcp", string(addr), timeout)
		}
	}
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte(transportMarker)); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := conn.Write([]byte(s.auth)); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}
