/*
 * @Author: gitsrc
 * @Date: 2020-12-23 13:58:37
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 13:59:15
 * @FilePath: /RaftHub/observer.go
 */

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
