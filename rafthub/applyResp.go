/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:16:44
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-12-23 14:17:12
 * @FilePath: /RaftHub/applyResp.go
 */

package rafthub

import "time"

type applyResp struct {
	resp interface{}
	elap time.Duration
	err  error
}
