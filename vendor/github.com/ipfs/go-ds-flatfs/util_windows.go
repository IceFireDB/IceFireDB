//go:build windows

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Note: This file is a variant of a subset of the golang standard library
// src/io/ioutil/tempfile.go
// with calls to os.Open replaced with the goissue34681.Open variant.

package flatfs

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	goissue34681 "github.com/alexbrainman/goissue34681"
)

var tmpRand uint32
var randmu sync.Mutex

func reseed() uint32 {
	return uint32(time.Now().UnixNano() + int64(os.Getpid()))
}

func nextRandom() string {
	randmu.Lock()
	r := tmpRand
	if r == 0 {
		r = reseed()
	}
	r = r*1664525 + 1013904223 // constants from Numerical Recipes
	tmpRand = r
	randmu.Unlock()
	return strconv.Itoa(int(1e9 + r%1e9))[1:]
}

func prefixAndSuffix(pattern string) (prefix, suffix string) {
	if pos := strings.LastIndex(pattern, "*"); pos != -1 {
		prefix, suffix = pattern[:pos], pattern[pos+1:]
	} else {
		prefix = pattern
	}
	return
}

func tempFileOnce(dir, pattern string) (f *os.File, err error) {
	if dir == "" {
		dir = os.TempDir()
	}

	prefix, suffix := prefixAndSuffix(pattern)

	nconflict := 0
	for i := 0; i < 10000; i++ {
		name := filepath.Join(dir, prefix+nextRandom()+suffix)
		f, err = goissue34681.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if os.IsExist(err) {
			if nconflict++; nconflict > 10 {
				randmu.Lock()
				tmpRand = reseed()
				randmu.Unlock()
			}
			continue
		}
		break
	}
	return
}

func readFileOnce(filename string) ([]byte, error) {
	f, err := goissue34681.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// It's a good but not certain bet that FileInfo will tell us exactly how much to
	// read, so let's try it but be prepared for the answer to be wrong.
	var sizeHint int = bytes.MinRead

	if fi, err := f.Stat(); err == nil {
		if sz := fi.Size(); sz <= math.MaxInt {
			if sz := int(sz); sz > sizeHint {
				sizeHint = sz
			}
		}
		sizeHint++ // one byte for final read at EOF
	}

	var buf bytes.Buffer
	buf.Grow(sizeHint)
	_, err = buf.ReadFrom(f)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
