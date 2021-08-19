// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/redlog/v2"
	"github.com/tidwall/rtime"
)

type remoteTime struct {
	remote bool       // use remote
	mu     sync.Mutex // lock times
	rtime  time.Time  // remote time
	ltime  time.Time  // local time
	ctime  time.Time  // calcd time
}

func (rt *remoteTime) Now() time.Time {
	if !rt.remote {
		return time.Now()
	}
	rt.mu.Lock()
	ctime := rt.rtime.Add(time.Since(rt.ltime))
	if !ctime.After(rt.ctime) {
		// ensure time is monotonic and increasing
		ctime = rt.ctime.Add(1)
		rt.ctime = ctime
	}
	rt.mu.Unlock()
	return ctime
}

// remoteTimeInit initializes the remote time fetching services, and
// continueously runs it in the background to keep synchronized.
func remoteTimeInit(conf Config, log *redlog.Logger) *remoteTime {
	rt := new(remoteTime)
	if conf.LocalTime {
		log.Warning("using local time")
		return rt
	}
	var wg sync.WaitGroup
	var once int32
	wg.Add(1)
	go func() {
		for {
			tm := rtime.Now()
			if tm.IsZero() {
				time.Sleep(time.Second)
				continue
			}
			rt.mu.Lock()
			if tm.After(rt.rtime) {
				rt.ltime = time.Now()
				rt.rtime = tm
				log.Debugf("synchronized time: %s", rt.rtime)
				if atomic.LoadInt32(&once) == 0 {
					atomic.StoreInt32(&once, 1)
					wg.Done()
				}
			}
			rt.mu.Unlock()
			time.Sleep(time.Second * 30)
		}
	}()
	go func() {
		time.Sleep(time.Second * 2)
		if atomic.LoadInt32(&once) != 0 {
			return
		}
		for {
			log.Warning("synchronized time: waiting for internet connection")
			if atomic.LoadInt32(&once) != 0 {
				break
			}
			time.Sleep(time.Second * 5)
		}
	}()
	wg.Wait()
	log.Printf("synchronized time")
	return rt
}
