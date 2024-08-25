// SPDX-License-Identifier: Apache-2.0 OR MIT
//
// jsync is a package that implements various synchronisation helpers that are missing from [sync].
// It does not and will not rely on golinkname to be portable.
package jsync

import (
	"sync"
	"sync/atomic"
)

var _ sync.Locker = NoCopy{}

// NoCopy is a type that does nothing, it implements [sync.Locker] to be recognised by the nocopy check of go vet.
// To use it add it as a first blank field in your structs: _ jsync.NoCopy
// Calling the methods panics.
type NoCopy struct{}

func (NoCopy) Lock() {
	panic("called nocopy")
}

func (NoCopy) Unlock() {
	panic("called nocopy")
}

// FWaitGroup is a [sync.WaitGroup] like object that invokes some function when the count reach 0.
// The function will be called synchronously when the Done is called.
// This is intended for asynchronous cleanup work like closing a channel shared my multiple workers.
// The transition to 0 must only ever happen once, else who knows what will happen ? (this should be caught by the race dectector)
type FWaitGroup struct {
	_ NoCopy
	c uint64
	f func()
}

// initial must not be 0, if you plan to use .Add then use 1 in your constructor and call .Done once the constructor is done.
func NewFWaitGroup(f func(), initial uint64) *FWaitGroup {
	fwg := &FWaitGroup{}
	fwg.Init(f, initial)
	return fwg
}

// Init is an alternative to [NewFWaitGroup] that allows to embed in your own fields.
// initial must not be 0, if you plan to use .Add then use 1 in your constructor and call .Done once the constructor is done.
func (fwg *FWaitGroup) Init(f func(), initial uint64) {
	if initial == 0 {
		panic("zero initial")
	}
	fwg.c = initial
	fwg.f = f
}

func (fwg *FWaitGroup) Add() {
	if atomic.AddUint64(&fwg.c, 1) == 1 {
		fwg.f = nil
		panic("called Add on an empty wait group")
	}
}

func (fwg *FWaitGroup) Done() {
	// folded by the compiler
	var minusOne uint64
	minusOne--

	if atomic.AddUint64(&fwg.c, minusOne) == 0 {
		f := fwg.f
		fwg.f = nil
		f()
	}
}
