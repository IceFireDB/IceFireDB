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

package RedSHandle

import (
	"bytes"
	"errors"
	"io"

	"github.com/IceFireDB/IceFireDB-Proxy/utils"
)

var (
	ExpectNumber   = &ProtocolError{"Expect Number"}
	ExpectNewLine  = &ProtocolError{"Expect Newline"}
	ExpectTypeChar = &ProtocolError{"Expect TypeChar"}

	InvalidNumArg   = errors.New("TooManyArg")
	InvalidBulkSize = errors.New("Invalid bulk size")
	LineTooLong     = errors.New("LineTooLong")

	MaxNumArg = 256
	// A String value can be at max 512 Megabytes in length. redis:https://redis.io/topics/data-types
	MaxBulkSize   = 512000000
	MaxTelnetLine = 1 << 10
	spaceSlice    = []byte{' '}
	emptyBulk     = [0]byte{}
)

type ProtocolError struct {
	message string
}

func (p *ProtocolError) Error() string {
	return p.message
}

type Command struct {
	Argv [][]byte
	Raw  []byte
	last bool
}

func (c *Command) Get(index int) []byte {
	if index >= 0 && index < len(c.Argv) {
		return c.Argv[index]
	}
	return nil
}

func (c *Command) ArgCount() int {
	return len(c.Argv)
}

func (c *Command) IsLast() bool {
	return c.last
}

type ParserHandle struct {
	*utils.Reader
}

func NewParserHandle(reader io.Reader) *ParserHandle {
	return &ParserHandle{utils.NewReader(reader)}
}

func (r *ParserHandle) readNumber() (int, error) {
	var neg bool
	err := r.RequireNBytes(1)
	if err != nil {
		return 0, err
	}
	switch r.Buffer[r.ReadPosition] {
	case '-':
		neg = true
		r.ReadPosition++
		break
	case '+':
		neg = false
		r.ReadPosition++
		break
	}
	var num uint64 = 0
	var startpos int = r.ReadPosition
OUTTER:
	for {
		for i := r.ReadPosition; i < r.WritePosition; i++ {
			c := r.Buffer[r.ReadPosition]
			if c >= '0' && c <= '9' {
				num = num*10 + uint64(c-'0')
				r.ReadPosition++
			} else {
				break OUTTER
			}
		}
		if r.IsEnd() {
			//	r.Reset()
			if e := r.ReadSome(1); e != nil {
				return 0, e
			}
		}
	}
	if r.ReadPosition == startpos {
		return 0, ExpectNumber
	}
	if neg {
		return -int(num), nil
	}
	return int(num), nil
}

func (r *ParserHandle) discardNewLine() error {
	if e := r.RequireNBytes(2); e != nil {
		return e
	}
	if r.Buffer[r.ReadPosition] == '\r' && r.Buffer[r.ReadPosition+1] == '\n' {
		r.ReadPosition += 2
		return nil
	}
	return ExpectNewLine
}

func (r *ParserHandle) parseBinary() (*Command, error) {
	rawBeginPoint := r.ReadPosition
	r.ReadPosition++
	numArg, err := r.readNumber()
	if err != nil {
		return nil, err
	}
	var e error
	if e = r.discardNewLine(); e != nil {
		return nil, e
	}
	switch {
	case numArg == -1:
		return nil, r.discardNewLine() // null array
	case numArg < -1:
		return nil, InvalidNumArg
	case numArg > MaxNumArg:
		return nil, InvalidNumArg
	}
	argv := make([][]byte, 0, numArg)
	for i := 0; i < numArg; i++ {
		if e = r.RequireNBytes(1); e != nil {
			return nil, e
		}
		if r.Buffer[r.ReadPosition] != '$' {
			return nil, ExpectTypeChar
		}
		r.ReadPosition++
		var plen int
		if plen, e = r.readNumber(); e != nil {
			return nil, e
		}
		if e = r.discardNewLine(); e != nil {
			return nil, e
		}

		/*
			RESP Bulk Strings can also be used in order to signal non-existence of a value using a special format that is used to represent a Null value.
			In this special format the length is -1, and there is no data, so a Null is represented as:
			"$-1\r\n"
		*/
		switch {
		case plen == -1:
			argv = append(argv, nil) // null bulk
		case plen == 0:
			argv = append(argv, emptyBulk[:]) // empty bulk
		case plen > 0 && plen <= MaxBulkSize:
			if e = r.RequireNBytes(plen); e != nil {
				return nil, e
			}
			argv = append(argv, r.Buffer[r.ReadPosition:(r.ReadPosition+plen)])
			r.ReadPosition += plen
		default:
			return nil, InvalidBulkSize
		}
		if e = r.discardNewLine(); e != nil {
			return nil, e
		}
	}
	rawEndPoint := r.ReadPosition

	if rawEndPoint <= rawBeginPoint {
		return &Command{Argv: argv}, nil
	}
	return &Command{Argv: argv, Raw: r.Buffer[rawBeginPoint:rawEndPoint]}, nil
}

func (r *ParserHandle) parseTelnet() (*Command, error) {
	nlPos := -1
	for {
		nlPos = bytes.IndexByte(r.Buffer, '\n')
		if nlPos == -1 {
			if e := r.ReadSome(1); e != nil {
				return nil, e
			}
		} else {
			break
		}
		if r.WritePosition > MaxTelnetLine {
			return nil, LineTooLong
		}
	}
	r.ReadPosition = r.WritePosition // we don't support pipeline in telnet mode
	return &Command{Argv: bytes.Split(r.Buffer[:nlPos-1], spaceSlice)}, nil
}

func (r *ParserHandle) ReadCommand() (*Command, error) {
	// if the buffer is empty, try to fetch some
	if r.ReadPosition >= r.WritePosition {
		if err := r.ReadSome(1); err != nil {
			return nil, err
		}
	}

	var cmd *Command
	var err error

	if r.Buffer[r.ReadPosition] == '*' {
		cmd, err = r.parseBinary()
	} else {
		cmd, err = r.parseTelnet()
	}
	if r.IsEnd() {
		if cmd != nil {
			cmd.last = true
		}
	}

	return cmd, err
}

func (r *ParserHandle) Commands() <-chan *Command {
	cmds := make(chan *Command)
	go func() {
		for cmd, err := r.ReadCommand(); err == nil; cmd, err = r.ReadCommand() {
			cmds <- cmd
		}
		close(cmds)
	}()
	return cmds
}
