// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

// Rand is a random number interface used by Machine
type Rand interface {
	Int() int
	Uint64() uint64
	Uint32() uint32
	Float64() float64
	Read([]byte) (n int, err error)
}
