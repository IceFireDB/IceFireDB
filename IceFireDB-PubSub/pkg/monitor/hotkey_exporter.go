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
	"strconv"

	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	hotKeyRecordLabels = append(BasicLabels, "_01_hotkey",
		"_02_hotkey_count",
		"_03_hotkey_count_avg_per_second",
		"_04_hotkey_time",
		"_05_hotkey_end_time")

	hotkeyRecordDesc = "hotkey.record"
	hotkeyCountDesc  = "hotkey.count"

	hotKeyGaugeVec = map[string]*prometheus.Desc{
		hotkeyCountDesc:  NewDesc("hotkey_count", "Count of hotkey on request", BasicLabels),
		hotkeyRecordDesc: NewDesc("hotkey_record", "Record hot key", hotKeyRecordLabels),
	}
)

type HotKeyExporter struct {
	mon           *Monitor
	basicConf     *ExporterConf
	hotKeyMetrics map[string]*prometheus.Desc
}

func NewHotKeyExporter(mon *Monitor, c *ExporterConf) *HotKeyExporter {
	metrics := make(map[string]*prometheus.Desc)

	for _, includeMetric := range c.HotKeyExporterConf.Include {
		if metric, ok := hotKeyGaugeVec[includeMetric]; ok {
			metrics[includeMetric] = metric
		}
	}

	if len(metrics) == 0 {
		metrics[hotkeyCountDesc] = hotKeyGaugeVec[hotkeyCountDesc]
	}

	return &HotKeyExporter{
		mon:           mon,
		basicConf:     c,
		hotKeyMetrics: metrics,
	}
}

func (n *HotKeyExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, hotKeyMetric := range n.hotKeyMetrics {
		ch <- hotKeyMetric
	}
}

func (n *HotKeyExporter) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("hotkey promethues collect panic", r)
		}
	}()
	n.mon.HotKeyMonitorData.RLock()

	if len(n.mon.HotKeyMonitorData.HotKeyData) == 0 {
		n.mon.HotKeyMonitorData.RUnlock()

		ch <- prometheus.MustNewConstMetric(
			n.hotKeyMetrics[hotkeyCountDesc],
			prometheus.GaugeValue,
			0,
			n.basicConf.Host,
		)

		return
	}

	statistics := new(HotKeyStatistics)
	statistics.Init(n.mon.HotKeyMonitorData, n.basicConf.HotKeyExporterConf.RecordLimit)

	startTime := n.mon.HotKeyMonitorData.TimeRange.Start
	endTime := n.mon.HotKeyMonitorData.TimeRange.End

	n.mon.HotKeyMonitorData.RUnlock()

	ch <- prometheus.MustNewConstMetric(
		n.hotKeyMetrics[hotkeyCountDesc],
		prometheus.GaugeValue,
		float64(statistics.GetHotKeyTotal()),
		n.basicConf.Host,
	)

	if metric, ok := n.hotKeyMetrics[hotkeyRecordDesc]; ok {
		hotkeyPairs := statistics.GetHotKeyPairArray()
		for _, hotKeyPair := range hotkeyPairs {
			ch <- prometheus.MustNewConstMetric(
				metric,
				prometheus.GaugeValue,

				0,
				n.basicConf.Host,
				hotKeyPair.key,
				strconv.FormatUint(hotKeyPair.count, 10),
				strconv.FormatFloat(hotKeyPair.countAvgPerSecond, 'f', -1, 64),
				startTime.Format("2006-01-02 15:04:05"),
				endTime.Format("2006-01-02 15:04:05"),
			)
		}
	}

	n.mon.HotKeyMonitorData.ReSetData()
}
