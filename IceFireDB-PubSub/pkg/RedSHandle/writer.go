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
	"bufio"
	"fmt"
	"io"
)

var (
	star   = []byte{'*'}
	colon  = []byte{':'}
	dollar = []byte{'$'}
	plus   = []byte{'+'}
	subs   = []byte{'-'}
)

type WriterHandle struct {
	w io.Writer
}

func NewWriterHandle(sink io.Writer) *WriterHandle {
	return &WriterHandle{
		w: sink,
	}
}

func (w *WriterHandle) Write(data []byte) (int, error) {
	return w.w.Write(data)
}

func (w *WriterHandle) Flush() error {
	if f, ok := w.w.(*bufio.Writer); ok {
		return f.Flush()
	}
	return nil
}

func (w *WriterHandle) WriteInt(val int64) error {
	w.Write(colon)
	w.Write([]byte(intToString(val)))
	_, err := w.Write(newLine)
	return err
}

func (w *WriterHandle) WriteBulk(val []byte) error {
	if val == nil {
		_, err := w.Write(nilBulk)
		return err
	}
	w.Write(dollar)
	w.Write([]byte(intToString(int64(len(val)))))
	w.Write(newLine)
	w.Write(val)
	_, err := w.Write(newLine)
	return err
}

func (w *WriterHandle) WriteBulkString(s string) error {
	return w.WriteBulk([]byte(s))
}

func (w *WriterHandle) WriteSimpleString(s string) error {
	w.Write(plus)
	w.Write([]byte(s))
	_, err := w.Write(newLine)
	return err
}

func (w *WriterHandle) WriteError(s string) error {
	w.Write(subs)
	w.Write([]byte(s))
	_, err := w.Write(newLine)
	return err
}

func (w *WriterHandle) WriteObjects(objs ...interface{}) error {
	if objs == nil {
		_, err := w.Write(nilArray)
		return err
	}

	w.Write(star)
	w.Write([]byte(intToString(int64(len(objs)))))
	w.Write(newLine)

	numArg := len(objs)
	for i := 0; i < numArg; i++ {
		v := objs[i]
		if v == nil {
			if err := w.WriteBulk(nil); err != nil {
				return err
			}
			continue
		}
		switch v := v.(type) {
		case []byte:
			if err := w.WriteBulk(v); err != nil {
				return err
			}
		case string:
			if err := w.WriteBulkString(v); err != nil {
				return err
			}
		case int:
			if err := w.WriteInt(int64(v)); err != nil {
				return err
			}
		case int32:
			if err := w.WriteInt(int64(v)); err != nil {
				return err
			}
		case int64:
			if err := w.WriteInt(v); err != nil {
				return err
			}
		default:
			return fmt.Errorf("value not suppport %v", v)
		}
	}
	return nil
}

func (w *WriterHandle) RecursivelyWriteObjects(objs ...interface{}) error {
	if objs == nil {
		_, err := w.Write(nilArray)
		return err
	}

	w.Write(star)
	w.Write([]byte(intToString(int64(len(objs)))))
	w.Write(newLine)

	// 空数组
	/*if len(objs) == 1 && objs[0] == nil {
		_, err := w.Write(nilArray)
		return err
	}*/

	numArg := len(objs)
	for i := 0; i < numArg; i++ {
		v := objs[i]
		if v == nil {
			if err := w.WriteBulk(nil); err != nil {
				return err
			}
			continue
		}
		switch v := v.(type) {
		case []byte:
			if err := w.WriteBulk(v); err != nil {
				return err
			}
		case string:
			err := w.WriteBulkString(v)
			if err != nil {
				return err
			}
		case int:
			if err := w.WriteInt(int64(v)); err != nil {
				return err
			}
		case int32:
			if err := w.WriteInt(int64(v)); err != nil {
				return err
			}
		case int64:
			if err := w.WriteInt(v); err != nil {
				return err
			}
		case []interface{}:
			if err := w.RecursivelyWriteObjects(v...); err != nil {
				return err
			}
		default:
			return fmt.Errorf("value not suppport %v", v)
		}
	}
	return nil
}

func (w *WriterHandle) WriteBulks(bulks ...[]byte) error {
	if bulks == nil {
		_, err := w.Write(nilArray)
		return err
	}

	w.Write(star)
	numElement := len(bulks)
	w.Write([]byte(intToString(int64(numElement))))
	w.Write(newLine)

	for i := 0; i < numElement; i++ {
		if err := w.WriteBulk(bulks[i]); err != nil {
			return err
		}
	}
	return nil
}

func (w *WriterHandle) WriteBulkStrings(bulks []string) error {
	if bulks == nil {
		_, err := w.Write(nilArray)
		return err
	}

	w.Write(star)
	numElement := len(bulks)
	w.Write([]byte(intToString(int64(numElement))))
	w.Write(newLine)

	for i := 0; i < numElement; i++ {
		if err := w.WriteBulkString(bulks[i]); err != nil {
			return err
		}
	}
	return nil
}
