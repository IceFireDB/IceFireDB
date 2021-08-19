// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

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
