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
	bigKeyRecordLabels = append(BasicLabels, "_01_bigkey", "_02_value_size", "_03_start_time")

	bigKeyCountDescKey        = "bigkey.count"
	bigKeyRecordDescKey       = "bigkey.record"
	bigKeyValueSizeSumDescKey = "bigkey.value_size_sum"

	bigKeyGaugeVec = map[string]*prometheus.Desc{
		bigKeyCountDescKey:        NewDesc("bigkey_count", "Count of big key", BasicLabels),
		bigKeyRecordDescKey:       NewDesc("bigkey_record", "Record and format of big key", bigKeyRecordLabels),
		bigKeyValueSizeSumDescKey: NewDesc("bigkey_value_size_sum", "Count of big key sum of all value bytes ", BasicLabels),
	}
)

type BigKeyExporter struct {
	mon           *Monitor
	basicConf     *ExporterConf
	bigKeyMetrics map[string]*prometheus.Desc
}

func NewBigKeyExporter(mon *Monitor, c *ExporterConf) *BigKeyExporter {
	metrics := make(map[string]*prometheus.Desc)

	for _, includeMetric := range c.BigKeyExporterConf.Include {
		if metric, ok := bigKeyGaugeVec[includeMetric]; ok {
			metrics[includeMetric] = metric
		}
	}

	if len(metrics) == 0 {
		metrics[bigKeyCountDescKey] = bigKeyGaugeVec[bigKeyCountDescKey]
	}

	return &BigKeyExporter{
		mon:           mon,
		basicConf:     c,
		bigKeyMetrics: metrics,
	}
}

func (b *BigKeyExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, bigKeyMetric := range b.bigKeyMetrics {
		ch <- bigKeyMetric
	}
}

func (b *BigKeyExporter) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("bigkey promethues collect panic", r)
		}
	}()
	data := b.mon.GetBigKeyData()
	if len(data) == 0 {

		ch <- prometheus.MustNewConstMetric(
			b.bigKeyMetrics[bigKeyCountDescKey],
			prometheus.GaugeValue,
			0,
			b.basicConf.Host,
		)

		if metric, ok := b.bigKeyMetrics[bigKeyValueSizeSumDescKey]; ok {
			ch <- prometheus.MustNewConstMetric(
				metric,
				prometheus.GaugeValue,
				0,
				b.basicConf.Host,
			)
		}
		return
	}

	statistics := new(BigKeyStatistics)
	statistics.Init(data, b.basicConf.BigKeyExporterConf.RecordLimit)

	ch <- prometheus.MustNewConstMetric(
		b.bigKeyMetrics[bigKeyCountDescKey],
		prometheus.GaugeValue,
		float64(statistics.GetBigKeyCount()),
		b.basicConf.Host,
	)

	if metric, ok := b.bigKeyMetrics[bigKeyValueSizeSumDescKey]; ok {
		ch <- prometheus.MustNewConstMetric(
			metric,
			prometheus.GaugeValue,
			float64(statistics.GetBigKeyValueSizeSum()),
			b.basicConf.Host,
		)
	}

	if metric, ok := b.bigKeyMetrics[bigKeyRecordDescKey]; ok {
		bigKeyPairs := statistics.GetBigKeyPairArray()
		for _, bigKeyPair := range bigKeyPairs {
			ch <- prometheus.MustNewConstMetric(
				metric,
				prometheus.GaugeValue,

				0,
				b.basicConf.Host,
				bigKeyPair.key,
				strconv.Itoa(bigKeyPair.valueSize),
				bigKeyPair.startTime.Format("2006-01-02 15:04:05"),
			)
		}
	}
}
