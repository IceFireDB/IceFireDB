/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package monitor

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/IceFireDB/IceFireDB-Proxy/utils"
)

type SlowQueryConfS struct {
	Enable                 bool
	SlowQueryTimeThreshold int
	MaxListSize            int
	sync.RWMutex
}

type SlowQueryDataS struct {
	Resp      []interface{}
	StartTime time.Time
	EndTime   time.Time
}

type SlowQueryMonitorDataS struct {
	SlowQueryDataList []*SlowQueryDataS
	Index             int32
	sync.RWMutex
}

func (m *Monitor) IsSlowQuery(args []interface{}, startTime time.Time, endTime time.Time) {
	subMilliseconds := endTime.Sub(startTime).Milliseconds()
	if subMilliseconds < int64(m.SlowQueryConf.SlowQueryTimeThreshold) {
		return
	}

	m.SlowQueryMonitorData.RLock()
	currentIndex := m.SlowQueryMonitorData.Index
	m.SlowQueryMonitorData.RUnlock()

	if currentIndex >= int32(m.SlowQueryConf.MaxListSize-1) {
		return
	}

	m.SlowQueryMonitorData.Lock()

	index := m.SlowQueryMonitorData.Index + 1

	if index >= int32(m.SlowQueryConf.MaxListSize) {
		m.SlowQueryMonitorData.Unlock()
		return
	}

	m.SlowQueryMonitorData.SlowQueryDataList[index].Resp = args
	m.SlowQueryMonitorData.SlowQueryDataList[index].StartTime = startTime
	m.SlowQueryMonitorData.SlowQueryDataList[index].EndTime = endTime
	m.SlowQueryMonitorData.Index = index
	m.SlowQueryMonitorData.Unlock()

	respStr := ""
	for _, v := range args {
		respStr += utils.GetInterfaceString(v) + " "
	}

	/*commandLen := len(args)
	for i := 0; i < commandLen; i++ {
		respStr += string(resp.Array[i].Value) + " "
	}*/
	logrus.Warnf("Found slowquery: %s, cost : %d ms.", respStr, endTime.Sub(startTime).Milliseconds())
}

func (m *Monitor) GetSlowQueryData() (data []*SlowQueryDataS, count int) {
	m.SlowQueryMonitorData.Lock()
	defer m.SlowQueryMonitorData.Unlock()

	count = int(m.SlowQueryMonitorData.Index) + 1

	data = make([]*SlowQueryDataS, count)

	for i := 0; i < count; i++ {
		data[i] = m.SlowQueryMonitorData.SlowQueryDataList[i]

		m.SlowQueryMonitorData.SlowQueryDataList[i] = &SlowQueryDataS{}
	}

	m.SlowQueryMonitorData.Index = INIT_INDEX

	return
}
