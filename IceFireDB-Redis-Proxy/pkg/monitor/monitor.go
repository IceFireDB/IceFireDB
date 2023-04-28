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

	"github.com/prometheus/client_golang/prometheus"

	"github.com/IceFireDB/IceFireDB/IceFireDB-Redis-Proxy/pkg/cache"

	lru "github.com/hashicorp/golang-lru"
)

/*
   monitor_job_interval: 60 # 监控任务时间轮策略：单次休眠时间 （单位：秒）
   monitor_job_lifetime: 20 # 监控任务时间轮策略：单次监控任务维持时间 （单位：秒）
   second_hot_threshold: 500 # hotkey的鉴定阀值，在监控之后，如果发现key的每秒处理量高于这个阀值，则判定为hotkey
   second_increase_threshold: 1000 # 命令处理量增长速率（单位： 次/秒） 如果中间件发现命令处理量增长速率高于这个阀值，则立刻开启hotkey监控
*/

const INIT_INDEX = -1

type HotKeyConfS struct {
	Enable                  bool
	MonitorJobInterval      int
	MonitorJobLifeTime      int
	SecondHotThreshold      int
	SecondIncreaseThreshold int
	LruSize                 int

	EnableCache      bool
	MaxCacheLifeTime int
	sync.RWMutex
}

type TimePair struct {
	Start time.Time
	End   time.Time
}

type HotKeyCountDataS map[string]uint64

type HotKeyMonitorDataS struct {
	HotKeyData HotKeyCountDataS
	TimeRange  *TimePair

	sync.RWMutex
}

func (hkmd *HotKeyMonitorDataS) ReSetData() {
	hkmd.Lock()
	defer hkmd.Unlock()

	hkmd.HotKeyData = make(HotKeyCountDataS)
	hkmd.TimeRange = &TimePair{}
}

type Monitor struct {
	Cache                *cache.Cache
	KeyMonitor           *KeyMonitor
	HotKeyConf           *HotKeyConfS
	HotKeyMonitorData    *HotKeyMonitorDataS
	BigKeyConf           *BigKeyConfS
	BigKeyMonitorData    *BigKeyMonitorDataS
	SlowQueryConf        *SlowQueryConfS
	SlowQueryMonitorData *SlowQueryMonitorDataS
	ConnectionGauge      prometheus.Gauge
	sync.RWMutex
}

func GetNewMonitor(hotKeyConf *HotKeyConfS, bigKeyConf *BigKeyConfS, slowQueryConf *SlowQueryConfS) (m *Monitor, err error) {
	m = &Monitor{
		KeyMonitor: &KeyMonitor{
			hotKeyJobstatus:            WORK_STATUS_STOP,
			hotKeyLastLoopRequestCount: 0,
		},
		HotKeyMonitorData: &HotKeyMonitorDataS{
			HotKeyData: make(HotKeyCountDataS),
			TimeRange:  &TimePair{},
		},
		BigKeyMonitorData: &BigKeyMonitorDataS{},

		SlowQueryMonitorData: &SlowQueryMonitorDataS{
			SlowQueryDataList: make([]*SlowQueryDataS, slowQueryConf.MaxListSize),
			Index:             INIT_INDEX,
		},
	}

	m.HotKeyConf = hotKeyConf

	if hotKeyConf.LruSize < 128 {
		hotKeyConf.LruSize = 128
	}

	if hotKeyConf.LruSize > 1024 {
		hotKeyConf.LruSize = 1024
	}

	m.KeyMonitor.hotKeyLRU, err = lru.New(hotKeyConf.LruSize)

	if err != nil {
		return nil, err
	}

	/*
	 * 初始化bigkey相关监控参数
	 */
	m.BigKeyConf = bigKeyConf

	if bigKeyConf.LruSize < 128 {
		bigKeyConf.LruSize = 128
	}

	if bigKeyConf.LruSize > 1024 {
		bigKeyConf.LruSize = 1024
	}

	m.BigKeyMonitorData.bigKeyLRU, err = lru.New(bigKeyConf.LruSize)

	if err != nil {
		return nil, err
	}

	m.SlowQueryConf = slowQueryConf

	for i := 0; i < m.SlowQueryConf.MaxListSize; i++ {
		m.SlowQueryMonitorData.SlowQueryDataList[i] = &SlowQueryDataS{}
	}
	return
}
