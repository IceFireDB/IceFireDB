/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:21:09
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:21:28
 * @FilePath: /RaftHub/response.go
 */

package rafthub

import "time"

type simpleResponse struct {
	v    interface{}
	elap time.Duration
	err  error
}

func (r *simpleResponse) Recv() (interface{}, time.Duration, error) {
	return r.v, r.elap, r.err
}

// Response ...
func Response(v interface{}, elapsed time.Duration, err error) Receiver {
	return &simpleResponse{v, elapsed, err}
}
