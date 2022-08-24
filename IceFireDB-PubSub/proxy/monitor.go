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

package proxy

import (
	"github.com/sirupsen/logrus"

	"github.com/IceFireDB/IceFireDB-PubSub/pkg/config"
	"github.com/IceFireDB/IceFireDB-PubSub/pkg/monitor"
)

func (p *Proxy) StartMonitor() {
	hotKeyMonitorConf := &monitor.HotKeyConfS{
		Enable:                  config.Get().Monitor.HotKeyConf.Enable,
		MonitorJobInterval:      config.Get().Monitor.HotKeyConf.MonitorJobInterval,
		MonitorJobLifeTime:      config.Get().Monitor.HotKeyConf.MonitorJobLifetime,
		SecondHotThreshold:      config.Get().Monitor.HotKeyConf.SecondHotThreshold,
		SecondIncreaseThreshold: config.Get().Monitor.HotKeyConf.SecondIncreaseThreshold,
		LruSize:                 config.Get().Monitor.HotKeyConf.LruSize,
		EnableCache:             config.Get().Monitor.HotKeyConf.EnableCache,
		MaxCacheLifeTime:        config.Get().Monitor.HotKeyConf.MaxCacheLifeTime,
	}

	bigKeyMonitorConf := &monitor.BigKeyConfS{
		Enable:           config.Get().Monitor.BigKeyConf.Enable,
		KeyMaxBytes:      config.Get().Monitor.BigKeyConf.KeyMaxBytes,
		ValueMaxBytes:    config.Get().Monitor.BigKeyConf.ValueMaxBytes,
		LruSize:          config.Get().Monitor.BigKeyConf.LruSize,
		EnableCache:      config.Get().Monitor.BigKeyConf.EnableCache,
		MaxCacheLifeTime: config.Get().Monitor.BigKeyConf.MaxCacheLifeTime,
	}

	if config.Get().Monitor.SlowQueryConf.SlowQueryThreshold <= 0 {
		config.Get().Monitor.SlowQueryConf.SlowQueryThreshold = 100
	}

	if config.Get().Monitor.SlowQueryConf.MaxListSize <= 0 {
		config.Get().Monitor.SlowQueryConf.MaxListSize = 64
	}

	slowQueryMonitorConf := &monitor.SlowQueryConfS{
		Enable:                 config.Get().Monitor.SlowQueryConf.Enable,
		SlowQueryTimeThreshold: config.Get().Monitor.SlowQueryConf.SlowQueryThreshold,
		MaxListSize:            config.Get().Monitor.SlowQueryConf.MaxListSize,
	}

	mon, err := monitor.GetNewMonitor(hotKeyMonitorConf, bigKeyMonitorConf, slowQueryMonitorConf)
	if err != nil {
		logrus.Error("初始化指标遥测错误：", err)
		return
	}
	p.Monitor = mon

	_ = monitor.RunPrometheusExporter(mon, config.Get().PrometheusExporterConf)
}
