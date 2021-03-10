/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:40:08
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:42:35
 * @FilePath: /RaftHub/splitServer.go
 */

package rafthub

import (
	"errors"
	"io"
	"net"

	"github.com/tidwall/redlog/v2"
)

// listener is a split network listener
type listener struct {
	addr net.Addr
	next chan net.Conn
}

func (l *listener) Accept() (net.Conn, error) {
	return <-l.next, nil
}
func (l *listener) Addr() net.Addr {
	return l.addr
}
func (l *listener) Close() error {
	return errors.New("disabled")
}

type matcher struct {
	sniff func(r io.Reader) (n int, matched bool)
	ln    *listener
}

// splitServer split a sinle server socket/listener into multiple logical
// listeners. For our use case, there is one transport listener and one client
// listener sharing the same server socket.
type splitServer struct {
	ln       net.Listener
	log      *redlog.Logger
	matchers []*matcher
}

func newSplitServer(ln net.Listener, log *redlog.Logger) *splitServer {
	return &splitServer{ln: ln, log: log}
}

func (m *splitServer) serve() error {
	for {
		c, err := m.ln.Accept()
		if err != nil {
			if m.log != nil {
				m.log.Error(err)
			}
			continue
		}
		conn := &conn{Conn: c, matching: true}
		var matched bool
		for _, ma := range m.matchers {
			conn.bufpos = 0
			if n, ok := ma.sniff(conn); ok {
				conn.buffer = conn.buffer[n:]
				conn.matching = false
				ma.ln.next <- conn
				matched = true
				break
			}
		}
		if !matched {
			c.Close()
		}
	}
}

func (m *splitServer) split(sniff func(r io.Reader) (n int, ok bool),
) net.Listener {
	ln := &listener{addr: m.ln.Addr(), next: make(chan net.Conn)}
	m.matchers = append(m.matchers, &matcher{sniff, ln})
	return ln
}

type conn struct {
	net.Conn
	matching bool
	buffer   []byte
	bufpos   int
}

func (c *conn) Read(p []byte) (n int, err error) {
	if c.matching {
		// matching mode
		if c.bufpos == len(c.buffer) {
			// need more buffer
			packet := make([]byte, 4096)
			nn, err := c.Conn.Read(packet)
			if err != nil {
				return 0, err
			}
			if nn == 0 {
				return 0, nil
			}
			c.buffer = append(c.buffer, packet[:nn]...)
		}
		copy(p, c.buffer[c.bufpos:])
		if len(p) < len(c.buffer)-c.bufpos {
			n = len(p)
		} else {
			n = len(c.buffer) - c.bufpos
		}
		c.bufpos += n
		return n, nil
	}
	if len(c.buffer) > 0 {
		// normal mode but with a buffer
		copy(p, c.buffer)
		if len(p) < len(c.buffer) {
			n = len(p)
			c.buffer = c.buffer[len(p):]
			if len(c.buffer) == 0 {
				c.buffer = nil
			}
		} else {
			n = len(c.buffer)
			c.buffer = nil
		}
		return n, nil
	}
	// normal mode, no buffer
	return c.Conn.Read(p)
}
