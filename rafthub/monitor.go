// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import "sync"

type monitor struct {
	s    *service
	obMu sync.Mutex
	obs  map[*observer]struct{}
}

func newMonitor(s *service) *monitor {
	m := &monitor{s: s}
	m.obs = make(map[*observer]struct{})
	return m
}

func (m *monitor) Send(msg Message) {
	if len(msg.Args) > 0 {
		// do not allow monitoring of certain system commands
		switch msg.Args[0] {
		case "raft", "machine", "auth", "cluster":
			return
		}
	}

	m.obMu.Lock()
	defer m.obMu.Unlock()
	for o := range m.obs {
		o.msgC <- msg
	}
}

func (m *monitor) NewObserver() Observer {
	o := new(observer)
	o.mon = m
	o.msgC = make(chan Message, 64)
	m.obMu.Lock()
	m.obs[o] = struct{}{}
	m.obMu.Unlock()
	return o
}
