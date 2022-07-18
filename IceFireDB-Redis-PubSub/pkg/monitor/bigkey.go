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

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/cache"
	lru "github.com/hashicorp/golang-lru"
)

type BigKeyConfS struct {
	Enable           bool
	KeyMaxBytes      int
	ValueMaxBytes    int
	LruSize          int
	EnableCache      bool
	MaxCacheLifeTime int
	sync.RWMutex
}

type BigKeyDataS struct {
	key       string
	valueSize int
	time      time.Time
}

type BigKeyMonitorDataS struct {
	bigKeyLRU *lru.Cache
	sync.RWMutex
}

func (m *Monitor) PutBigKey(key string, valueSize int) bool {
	if valueSize == 0 {
		return false
	}

	if len(key) >= m.BigKeyConf.KeyMaxBytes || valueSize >= m.BigKeyConf.ValueMaxBytes {
		bigkeyData := &BigKeyDataS{key: key, valueSize: valueSize, time: time.Now()}
		m.BigKeyMonitorData.bigKeyLRU.Add(key, bigkeyData)
		logrus.Warnf("Found bigkey: %s, value size: %d byte", key, valueSize)
		return true
	}
	return false
}

func (m *Monitor) AddBigKeyCacheItem(cache *cache.Cache, key, value []byte, expireMilliSeconds int) bool {
	if m.BigKeyConf.EnableCache == false || cache == nil {
		return false
	}
	cache.Set(string(key), value, time.Millisecond*time.Duration(expireMilliSeconds))
	return true
}

func (m *Monitor) GetBigKeyData() []BigKeyDataS {
	m.BigKeyMonitorData.Lock()
	defer func() {
		m.BigKeyMonitorData.bigKeyLRU.Purge()
		m.BigKeyMonitorData.Unlock()
	}()

	bigKeyData := make([]BigKeyDataS, 0)
	for _, key := range m.BigKeyMonitorData.bigKeyLRU.Keys() {
		if data, ok := m.BigKeyMonitorData.bigKeyLRU.Get(key); ok {
			if data, ok := data.(*BigKeyDataS); ok {
				if key, ok := key.(string); ok {
					bigKeyData = append(bigKeyData, BigKeyDataS{key: key, valueSize: data.valueSize, time: data.time})
				}
			}
		}
	}
	return bigKeyData
}
