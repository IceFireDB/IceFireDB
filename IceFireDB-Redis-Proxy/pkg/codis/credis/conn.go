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

package credis

import (
	"net"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-Redis-Proxy/pkg/codis/bufio2"
	"github.com/IceFireDB/IceFireDB/IceFireDB-Redis-Proxy/pkg/codis/errors"
	"github.com/IceFireDB/IceFireDB/IceFireDB-Redis-Proxy/pkg/codis/unsafe2"
)

type Conn struct {
	Sock net.Conn

	*Decoder
	*Encoder

	ReaderTimeout time.Duration
	WriterTimeout time.Duration

	LastWrite time.Time
}

func DialTimeout(addr string, timeout time.Duration, rbuf, wbuf int) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return NewConn(c, rbuf, wbuf), nil
}

func NewConn(sock net.Conn, rbuf, wbuf int) *Conn {
	conn := &Conn{Sock: sock}
	conn.Decoder = newConnDecoder(conn, rbuf)
	conn.Encoder = newConnEncoder(conn, wbuf)
	return conn
}

func (c *Conn) LocalAddr() string {
	return c.Sock.LocalAddr().String()
}

func (c *Conn) RemoteAddr() string {
	return c.Sock.RemoteAddr().String()
}

func (c *Conn) Close() error {
	return c.Sock.Close()
}

func (c *Conn) CloseReader() error {
	if t, ok := c.Sock.(*net.TCPConn); ok {
		return t.CloseRead()
	}
	return c.Close()
}

func (c *Conn) SetKeepAlivePeriod(d time.Duration) error {
	if t, ok := c.Sock.(*net.TCPConn); ok {
		if err := t.SetKeepAlive(d != 0); err != nil {
			return errors.Trace(err)
		}
		if d != 0 {
			if err := t.SetKeepAlivePeriod(d); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (c *Conn) FlushEncoder() *FlushEncoder {
	return &FlushEncoder{Conn: c}
}

type connReader struct {
	*Conn
	unsafe2.Slice

	hasDeadline bool
}

func newConnDecoder(conn *Conn, bufsize int) *Decoder {
	r := &connReader{Conn: conn}
	r.Slice = unsafe2.MakeSlice(bufsize)
	return NewDecoderBuffer(bufio2.NewReaderBuffer(r, r.Buffer()))
}

func (r *connReader) Read(b []byte) (int, error) {
	if timeout := r.ReaderTimeout; timeout != 0 {
		if err := r.Sock.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return 0, errors.Trace(err)
		}
		r.hasDeadline = true
	} else if r.hasDeadline {
		if err := r.Sock.SetReadDeadline(time.Time{}); err != nil {
			return 0, errors.Trace(err)
		}
		r.hasDeadline = false
	}
	n, err := r.Sock.Read(b)
	if err != nil {
		err = errors.Trace(err)
	}
	return n, err
}

type connWriter struct {
	*Conn
	unsafe2.Slice

	hasDeadline bool
}

func newConnEncoder(conn *Conn, bufsize int) *Encoder {
	w := &connWriter{Conn: conn}
	w.Slice = unsafe2.MakeSlice(bufsize)
	return NewEncoderBuffer(bufio2.NewWriterBuffer(w, w.Buffer()))
}

func (w *connWriter) Write(b []byte) (int, error) {
	if timeout := w.WriterTimeout; timeout != 0 {
		if err := w.Sock.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return 0, errors.Trace(err)
		}
		w.hasDeadline = true
	} else if w.hasDeadline {
		if err := w.Sock.SetWriteDeadline(time.Time{}); err != nil {
			return 0, errors.Trace(err)
		}
		w.hasDeadline = false
	}
	n, err := w.Sock.Write(b)
	if err != nil {
		err = errors.Trace(err)
	}
	w.LastWrite = time.Now()
	return n, err
}

func IsTimeout(err error) bool {
	if err := errors.Cause(err); err != nil {
		e, ok := err.(*net.OpError)
		if ok {
			return e.Timeout()
		}
	}
	return false
}

type FlushEncoder struct {
	Conn *Conn

	MaxInterval time.Duration
	MaxBuffered int

	nbuffered int
}

func (p *FlushEncoder) NeedFlush() bool {
	if p.nbuffered != 0 {
		if p.MaxBuffered < p.nbuffered {
			return true
		}
		if p.MaxInterval < time.Since(p.Conn.LastWrite) {
			return true
		}
	}
	return false
}

func (p *FlushEncoder) Flush(force bool) error {
	if force || p.NeedFlush() {
		if err := p.Conn.Flush(); err != nil {
			return err
		}
		p.nbuffered = 0
	}
	return nil
}

func (p *FlushEncoder) Encode(resp *Resp) error {
	if err := p.Conn.Encode(resp, false); err != nil {
		return err
	} else {
		p.nbuffered++
		return nil
	}
}

func (p *FlushEncoder) EncodeMultiBulk(multi []*Resp) error {
	if err := p.Conn.EncodeMultiBulk(multi, false); err != nil {
		return err
	} else {
		p.nbuffered++
		return nil
	}
}
