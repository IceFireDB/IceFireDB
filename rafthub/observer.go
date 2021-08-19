// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

// An Observer holds a channel that delivers the messages for all commands
// processed by a Service.
type Observer interface {
	Stop()
	C() <-chan Message
}

type observer struct {
	mon  *monitor
	msgC chan Message
}

func (o *observer) C() <-chan Message {
	return o.msgC
}

func (o *observer) Stop() {
	o.mon.obMu.Lock()
	defer o.mon.obMu.Unlock()
	if _, ok := o.mon.obs[o]; ok {
		delete(o.mon.obs, o)
		close(o.msgC)
	}
}
