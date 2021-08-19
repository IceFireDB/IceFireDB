// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import "time"

type applyResp struct {
	resp interface{}
	elap time.Duration
	err  error
}
