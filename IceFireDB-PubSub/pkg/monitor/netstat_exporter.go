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
	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	tcpStateDescKey = "tcp.state"
	networkGaugeVec = map[string]*prometheus.Desc{
		tcpStateDescKey: NewDesc("tcp_state_count", "Count of tcp state", append(BasicLabels, "tcp_state")),
	}
)

func NewNetExporter(c *ExporterConf) *NetworkExporter {
	cf := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace:   Namespace,
		Name:        "net_input_bytes_total",
		Help:        "net_input_bytes_total metric",
		ConstLabels: BasicLabelsMap,
	}, CurrentNetworkStatInputByte)

	cfw := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace:   Namespace,
		Name:        "net_output_bytes_total",
		Help:        "net_output_bytes_total metric",
		ConstLabels: BasicLabelsMap,
	}, CurrentNetworkStatOutputByte)
	prometheus.MustRegister(cf, cfw)

	metrics := make(map[string]*prometheus.Desc)
	metrics[tcpStateDescKey] = networkGaugeVec[tcpStateDescKey]
	return &NetworkExporter{
		networkMetrics: metrics,
		basicConf:      c,
	}
}

type NetworkExporter struct {
	basicConf      *ExporterConf
	networkMetrics map[string]*prometheus.Desc
}

func (s *NetworkExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, nm := range s.networkMetrics {
		ch <- nm
	}
}

func (s *NetworkExporter) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("network promethues collect panic", r)
		}
	}()
	tcp, err := GetNetstat()
	if err != nil {
		return
	}
	for k, v := range tcp {
		ch <- prometheus.MustNewConstMetric(
			s.networkMetrics[tcpStateDescKey],
			prometheus.GaugeValue,
			float64(v),
			s.basicConf.Host,
			k,
		)
	}
}
