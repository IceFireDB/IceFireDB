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

package unsafe2

import (
	"reflect"
	"runtime"
	"unsafe"

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/codis/sync2/atomic2"
)

var allocOffheapBytes atomic2.Int64

func OffheapBytes() int64 {
	return allocOffheapBytes.Int64()
}

type cgoSlice struct {
	ptr unsafe.Pointer
	buf []byte
}

func newCGoSlice(n int, force bool) Slice {
	after := allocOffheapBytes.Add(int64(n))
	if !force && after > MaxOffheapBytes() {
		allocOffheapBytes.Sub(int64(n))
		return nil
	}
	p := cgo_malloc(n)
	if p == nil {
		allocOffheapBytes.Sub(int64(n))
		return nil
	}
	s := &cgoSlice{
		ptr: p,
		buf: *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
			Data: uintptr(p), Len: n, Cap: n,
		})),
	}
	runtime.SetFinalizer(s, (*cgoSlice).reclaim)
	return s
}

func (s *cgoSlice) Type() string {
	return "cgo_slice"
}

func (s *cgoSlice) Buffer() []byte {
	return s.buf
}

func (s *cgoSlice) reclaim() {
	if s.ptr == nil {
		return
	}
	cgo_free(s.ptr)
	allocOffheapBytes.Sub(int64(len(s.buf)))
	s.ptr = nil
	s.buf = nil
	runtime.SetFinalizer(s, nil)
}

func (s *cgoSlice) Slice2(beg, end int) Slice {
	return newGoSliceFrom(s, s.Buffer()[beg:end])
}

func (s *cgoSlice) Slice3(beg, end, cap int) Slice {
	return newGoSliceFrom(s, s.Buffer()[beg:end:cap])
}

func (s *cgoSlice) Parent() Slice {
	return nil
}
