/*
 * @Author: gitsrc
 * @Date: 2020-12-23 13:48:59
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 13:49:04
 * @FilePath: /RaftHub/rand.go
 */

package rafthub

// Rand is a random number interface used by Machine
type Rand interface {
	Int() int
	Uint64() uint64
	Uint32() uint32
	Float64() float64
	Read([]byte) (n int, err error)
}
