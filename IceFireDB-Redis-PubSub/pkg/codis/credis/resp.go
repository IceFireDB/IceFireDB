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

import "fmt"

type RespType byte

const (
	TypeString    RespType = '+'
	TypeError     RespType = '-'
	TypeInt       RespType = ':'
	TypeBulkBytes RespType = '$'
	TypeArray     RespType = '*'
)

func (t RespType) String() string {
	switch t {
	case TypeString:
		return "<string>"
	case TypeError:
		return "<error>"
	case TypeInt:
		return "<int>"
	case TypeBulkBytes:
		return "<bulkbytes>"
	case TypeArray:
		return "<array>"
	default:
		return fmt.Sprintf("<unknown-0x%02x>", byte(t))
	}
}

type Resp struct {
	Type RespType

	Value []byte
	Array []*Resp
}

func (r *Resp) IsString() bool {
	return r.Type == TypeString
}

func (r *Resp) IsError() bool {
	return r.Type == TypeError
}

func (r *Resp) IsInt() bool {
	return r.Type == TypeInt
}

func (r *Resp) IsBulkBytes() bool {
	return r.Type == TypeBulkBytes
}

func (r *Resp) IsArray() bool {
	return r.Type == TypeArray
}

func NewString(value []byte) *Resp {
	r := &Resp{}
	r.Type = TypeString
	r.Value = value
	return r
}

func NewError(value []byte) *Resp {
	r := &Resp{}
	r.Type = TypeError
	r.Value = value
	return r
}

func NewErrorf(format string, args ...interface{}) *Resp {
	return NewError([]byte(fmt.Sprintf(format, args...)))
}

func NewInt(value []byte) *Resp {
	r := &Resp{}
	r.Type = TypeInt
	r.Value = value
	return r
}

func NewBulkBytes(value []byte) *Resp {
	r := &Resp{}
	r.Type = TypeBulkBytes
	r.Value = value
	return r
}

func NewArray(array []*Resp) *Resp {
	r := &Resp{}
	r.Type = TypeArray
	r.Array = array
	return r
}
