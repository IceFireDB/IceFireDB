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
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/IceFireDB/IceFireDB-Proxy/utils"

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/cache"
	lru "github.com/hashicorp/golang-lru"
)

/* hotkey任务状态值
 * 通过原子操作进行状态的更新。
 */
const (
	WORK_STATUS_ING  = 1
	WORK_STATUS_STOP = 2
)

type HotKeyDataS struct {
	key   string
	value []byte
	count uint64
	sync.RWMutex
}

type KeyMonitor struct {
	hotKeyJobstatus            int32
	hotKeyLRU                  *lru.Cache
	hotKeyLastLoopRequestCount uint64

	keyTotalRequestCount uint64

	sync.RWMutex
}

/*
 *IsShouldPutHotKey()
 *这个函数主要用来判断当前时机是不是应该可以进行hotkey的统计操作
 *通过这个函数的前置判断，避免不必要的函数参数传递而造成的性能损耗
 */
func (m *Monitor) IsShouldPutHotKey() bool {
	atomic.AddUint64(&m.KeyMonitor.keyTotalRequestCount, 1)

	if atomic.LoadInt32(&m.KeyMonitor.hotKeyJobstatus) != WORK_STATUS_ING {
		return false
	}

	return true
}

/*
 *PutHotKey(hotkey的key数据，hotkey的value数据)
 *在调用IsShouldPutHotKey函数发现处于hotkey统计周期内时，进行hotkey的写入工作
 *这个函数主要完成LRU内存的写入，LRU内存中长久存活的对象就是热点对象，不够热的对象都将被LRU算法置换出。
 */
func (m *Monitor) PutHotKey(key string, value []byte) {
	/*if atomic.LoadInt32(&m.KeyMonitor.hotKeyJobstatus) != WORK_STATUS_ING {
		return
	}*/

	if !m.KeyMonitor.hotKeyLRU.Contains(key) {
		m.KeyMonitor.hotKeyLRU.Add(key, &HotKeyDataS{key: key, value: value, count: 1})
	} else if kdata, ok := m.KeyMonitor.hotKeyLRU.Get(key); ok {
		if data, ok := kdata.(*HotKeyDataS); ok {
			atomic.AddUint64(&data.count, 1)

			data.RLock()
			if bytes.Compare(data.value, value) == 0 {
				data.RUnlock()
			} else {

				data.RUnlock()
				data.Lock()
				data.value = value
				data.Unlock()
			}
		}
	}
}

/*
 *AddHotKeyCacheItem(cache缓存对象，cache的key，cache的数据体，超时时间-毫秒)
 *这个函数主要支撑hotkey数据的缓存写入工作，当检测出hotkey时，判断是否开启了cache功能，如果开启了则进行cache的写入工作。
 */
func (m *Monitor) AddHotKeyCacheItem(cache *cache.Cache, key string, value []byte, expireMilliSeconds int) bool {
	if m.HotKeyConf.EnableCache == false || cache == nil {
		return false
	}
	return true
}

/*
 *BeginMonitorHotKey()
 *这个函数主要启动HotKey的监控工作，监控工作主要包括以下几部分：
 * 1.原子切换hotkey任务执行状态，WORK_STATUS_STOP状态代表任务监控停止，WORK_STATUS_ING状态代表正在进行hotkey监控
 * 2.hotkey任务监控等待：等待的目的主要是为了让requesthandle可以在这段时间内统计hotkey lRU数据
 * 3.对于监控获得的数据进行hotkey LRU数据进行汇总工作，并把hotkey存储到hotkeys汇总数据内存中。
 * 4.把hotkeys中的数据并发安全的存储到HotKeyMonitorData数据中，这个数据可以供外部调用并发安全的访问。
 */
func (m *Monitor) BeginMonitorHotKey() {
	utils.GoWithRecover(func() {
		var hotkeyJobStartTime time.Time
		var hotkeyJobEndTime time.Time
		for {

			m.KeyMonitor.hotKeyLRU.Purge()

			if atomic.CompareAndSwapInt32(&m.KeyMonitor.hotKeyJobstatus, WORK_STATUS_STOP, WORK_STATUS_ING) {
				hotkeyJobStartTime = time.Now()
			}
			select {
			case <-time.After(time.Second * time.Duration(m.HotKeyConf.MonitorJobLifeTime)):
				atomic.StoreInt32(&m.KeyMonitor.hotKeyJobstatus, WORK_STATUS_STOP)
				hotkeyJobEndTime = time.Now()
			}

			hotkeys := make([]*HotKeyDataS, 0)
			for _, key := range m.KeyMonitor.hotKeyLRU.Keys() {
				if data, ok := m.KeyMonitor.hotKeyLRU.Get(key); ok {
					if hotkeydata, ok := data.(*HotKeyDataS); ok {
						speed := float32(atomic.LoadUint64(&hotkeydata.count)) / float32(m.HotKeyConf.MonitorJobLifeTime)
						if speed > float32(m.HotKeyConf.SecondHotThreshold) {

							hotkeys = append(hotkeys, hotkeydata)
							logrus.Warnf("Found hotkey: %s, speed : %f count/s", hotkeydata.key, speed)

						}
					}
				}
			}

			if len(hotkeys) > 0 {

				m.HotKeyMonitorData.Lock()
				m.HotKeyMonitorData.HotKeyData = make(HotKeyCountDataS)
				m.HotKeyMonitorData.TimeRange.Start = hotkeyJobStartTime
				m.HotKeyMonitorData.TimeRange.End = hotkeyJobEndTime
				for _, hotkey := range hotkeys {
					m.HotKeyMonitorData.HotKeyData[hotkey.key] = hotkey.count
				}
				m.HotKeyMonitorData.Unlock()
			}

			currentKeyCount := atomic.LoadUint64(&m.KeyMonitor.keyTotalRequestCount)
			atomic.StoreUint64(&m.KeyMonitor.hotKeyLastLoopRequestCount, currentKeyCount)
			for i := 0; i < m.HotKeyConf.MonitorJobInterval; i++ {
				time.Sleep(time.Second)
				currentKeyCount = atomic.LoadUint64(&m.KeyMonitor.keyTotalRequestCount)
				keyLastLoopRequestCount := atomic.LoadUint64(&m.KeyMonitor.hotKeyLastLoopRequestCount)
				atomic.StoreUint64(&m.KeyMonitor.hotKeyLastLoopRequestCount, currentKeyCount)

				if currentKeyCount-keyLastLoopRequestCount >= uint64(m.HotKeyConf.SecondIncreaseThreshold) {
					break
				}
			}
		}
	}, func(r interface{}) {
		m.BeginMonitorHotKey()
	})
}
