/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package rediscluster

import (
	"bufio"
	"container/list"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

type redisConn struct {
	c net.Conn
	t time.Time

	br *bufio.Reader
	bw *bufio.Writer

	// bwm *RedSHandle.WriterHandle

	// writerMemory *bytes.Buffer
	// readerParser *RedisFastParser.ParserHandle

	// readerMemory *bytes.Buffer

	readTimeout  time.Duration
	writeTimeout time.Duration

	// Pending replies to be read in redis pipeling.
	pending int

	// Scratch space for formatting argument length.
	lenScratch [32]byte

	// Scratch space for formatting integer and float.
	numScratch [40]byte
}

const (
	MASTER_NODE = 1
	SLAVE_NODE  = 2
)

type redisNode struct {
	address string

	conns     list.List
	keepAlive int
	aliveTime time.Duration

	connTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	mutex sync.Mutex

	updateTime time.Time

	closed bool

	NodeType int // master or slave
}

func (node *redisNode) getConn() (*redisConn, error) {
	// 需要针对node的tcp连接池进行内存操作，所以先上锁
	node.mutex.Lock()

	if node.closed {
		node.mutex.Unlock()
		return nil, fmt.Errorf("getConn: connection has been closed")
	}

	// 从TCP连接池中清理陈旧的TCP连接
	// 如果连接远程node节点时候设置了TCP连接存活时间 则 进行检验。
	if node.aliveTime > 0 {
		for {
			// 从list中选择一个元素，如果conns列表为空 则跳出检查
			elem := node.conns.Back()
			if elem == nil {
				break
			}

			// 成功获取到一条TCP连接，进行生命期时间校验
			conn := elem.Value.(*redisConn)

			// 如果当前获取的TCP连接是在合法生命周期内部的，立刻退出，但是这个元素还在list中，下次获取仍然能够获取到
			if conn.t.Add(node.aliveTime).After(time.Now()) {
				break
			}

			// 运行到这里，代表TCP连接生命期超时，删除此元素
			node.conns.Remove(elem)
		}
	}

	// 经过前面的操作，前面目的在于清理超时TCP连接
	if node.conns.Len() <= 0 {
		// 没有TCP连接可用，所以需要新建连接，立刻需要释放锁
		node.mutex.Unlock()

		c, err := net.DialTimeout("tcp", node.address, node.connTimeout)
		if err != nil {
			return nil, err
		}

		// var writerMemory bytes.Buffer

		conn := &redisConn{
			c:            c,
			br:           bufio.NewReader(c),
			bw:           bufio.NewWriter(c),
			readTimeout:  node.readTimeout,
			writeTimeout: node.writeTimeout,
			// writerMemory: &writerMemory,
		}

		// 设置内存缓冲区
		// conn.bwm = RedSHandle.NewWriterHandle(conn.writerMemory)
		// conn.readerParser = RedisFastParser.NewParserHandle(conn.c)

		return conn, nil
	}

	// 获取到一条已经存在的存活TCP连接，这条TCP的生命周期也在合法时间内，所以：
	// 1.取出元素
	// 2.删除元素在list中的位置
	// 3.立刻解锁
	elem := node.conns.Back()
	node.conns.Remove(elem)
	node.mutex.Unlock()

	// 重置内存缓冲区
	// elem.Value.(*redisConn).writerMemory.Reset()
	return elem.Value.(*redisConn), nil
}

func (node *redisNode) releaseConn(conn *redisConn) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Connection still has pending replies, just close it.
	if conn.pending > 0 || node.closed {
		conn.shutdown()
		return
	}

	if node.conns.Len() >= node.keepAlive || node.aliveTime <= 0 {
		conn.shutdown()
		return
	}

	conn.t = time.Now()
	node.conns.PushFront(conn)

	// 重置内存缓冲区
	// conn.writerMemory.Reset()
}

func (conn *redisConn) shutdown() {
	conn.c.Close()
}

func (node *redisNode) shutdown() {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	for {
		elem := node.conns.Back()
		if elem == nil {
			break
		}

		conn := elem.Value.(*redisConn)
		conn.c.Close()
		node.conns.Remove(elem)
	}

	node.closed = true
}

func (conn *redisConn) send(cmd string, args ...interface{}) error {
	conn.pending += 1

	if conn.writeTimeout > 0 {
		conn.c.SetWriteDeadline(time.Now().Add(conn.writeTimeout))
	}

	if err := conn.writeCommand(cmd, args); err != nil {
		return err
	}

	return nil
}

func (conn *redisConn) flush() error {
	if conn.writeTimeout > 0 {
		conn.c.SetWriteDeadline(time.Now().Add(conn.writeTimeout))
	}

	if err := conn.bw.Flush(); err != nil {
		return err
	}

	return nil
}

func (conn *redisConn) receive() (interface{}, error) {
	if conn.readTimeout > 0 {
		conn.c.SetReadDeadline(time.Now().Add(conn.readTimeout))
	}

	if conn.pending <= 0 {
		return nil, errors.New("no more pending reply")
	}

	conn.pending -= 1

	return conn.readReply()
}

func (node *redisNode) do(cmd string, args ...interface{}) (interface{}, error) {
	conn, err := node.getConn()
	if err != nil {
		return redisError("ECONNTIMEOUT"), nil
	}

	if err = conn.send(cmd, args...); err != nil {
		conn.shutdown()
		return nil, err
	}

	if err = conn.flush(); err != nil {
		conn.shutdown()
		return nil, err
	}

	reply, err := conn.receive()
	if err != nil {
		conn.shutdown()
		return nil, err
	}

	node.releaseConn(conn)

	return reply, err
}

func (conn *redisConn) writeLen(prefix byte, n int) error {
	conn.lenScratch[len(conn.lenScratch)-1] = '\n'
	conn.lenScratch[len(conn.lenScratch)-2] = '\r'
	i := len(conn.lenScratch) - 3

	for {
		conn.lenScratch[i] = byte('0' + n%10)
		i -= 1
		n = n / 10
		if n == 0 {
			break
		}
	}

	conn.lenScratch[i] = prefix
	_, err := conn.bw.Write(conn.lenScratch[i:])

	return err
}

func (conn *redisConn) writeString(s string) error {
	conn.writeLen('$', len(s))
	conn.bw.WriteString(s)
	_, err := conn.bw.WriteString("\r\n")

	return err
}

func (conn *redisConn) writeBytes(p []byte) error {
	conn.writeLen('$', len(p))
	conn.bw.Write(p)
	_, err := conn.bw.WriteString("\r\n")

	return err
}

func (conn *redisConn) writeInt64(n int64) error {
	return conn.writeBytes(strconv.AppendInt(conn.numScratch[:0], n, 10))
}

func (conn *redisConn) writeFloat64(n float64) error {
	return conn.writeBytes(strconv.AppendFloat(conn.numScratch[:0], n, 'g', -1, 64))
}

// Args must be int64, float64, string, []byte, other types are not supported for safe reason.
func (conn *redisConn) writeCommand(cmd string, args []interface{}) error {
	conn.writeLen('*', len(args)+1)
	err := conn.writeString(cmd)

	for _, arg := range args {
		if err != nil {
			break
		}
		switch arg := arg.(type) {
		case int:
			err = conn.writeInt64(int64(arg))
		case int64:
			err = conn.writeInt64(arg)
		case float64:
			err = conn.writeFloat64(arg)
		case string:
			err = conn.writeString(arg)
		case []byte:
			err = conn.writeBytes(arg)
		default:
			err = fmt.Errorf("unknown type %T", arg)
		}
	}

	return err
}

// readLine read a single line terminated with CRLF.
func (conn *redisConn) readLine() ([]byte, error) {
	var line []byte
	for {
		p, err := conn.br.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		n := len(p) - 2
		if n < 0 {
			return nil, errors.New("invalid response: readLine->the length of readLine is wrong.")
		}

		// bulk string may contain '\n', such as CLUSTER NODES
		if p[n] != '\r' {
			if line != nil {
				line = append(line, p[:]...)
			} else {
				line = p
			}
			continue
		}

		if line != nil {
			return append(line, p[:n]...), nil
		} else {
			return p[:n], nil
		}
	}
}

// parseLen parses bulk string and array length.
func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, errors.New("invalid response : parseLen-> the length of Len data is nil.")
	}

	// null element.
	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, errors.New("invalid response : parseLen-> byte is not a numeric type.")
		}
		n += int(b - '0')
	}

	return n, nil
}

// parseInt parses an integer reply.
func parseInt(p []byte) (int64, error) {
	if len(p) == 0 {
		return 0, errors.New("invalid response : parseInt-> the length of Len data is nil.")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, errors.New("invalid response : parseInt-> the length of Len data2 is nil.")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, errors.New("invalid response : parseInt-> byte is not a numeric type.")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}

	return n, nil
}

var (
	okReply   interface{} = "OK"
	pongReply interface{} = "PONG"
)

type redisError string

func (err redisError) Error() string { return string(err) }

func (conn *redisConn) readReply() (interface{}, error) {
	line, err := conn.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, errors.New("invalid reponse: the length of reponse first line is zero. ")
	}

	switch line[0] {
	case '+':
		switch {
		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
			// Avoid allocation for frequent "+OK" response.
			return okReply, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			// Avoid allocation in PING command benchmarks :)
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return redisError(string(line[1:])), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}

		// 进行二进制安全读取
		p := make([]byte, n)

		_, err = io.ReadFull(conn.br, p)
		if err != nil {
			return nil, err
		}

		// 如果数据正常，此处仅仅是忽略\r\n ，因此len长度为0
		if line, err := conn.readLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, errors.New("invalid reponse $:bad bulk string format.")
		}

		return p, nil

		//line, err = conn.readLine()
		//if err != nil {
		//	return nil, err
		//}
		//if len(line) != n {
		//	return nil, errors.New("invalid reponse: length of $:reponse is error.")
		//}
		//
		//return line, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}

		r := make([]interface{}, n)
		for i := range r {
			r[i], err = conn.readReply()
			if err != nil {
				return nil, err
			}
		}

		return r, nil
	}

	return nil, errors.New("invalid response : unexpected response line.")
}
