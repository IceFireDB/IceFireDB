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
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/IceFireDB/IceFireDB-PubSub/utils"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const Namespace = "redisproxy"

var BasicLabels = []string{"host"}

var BasicLabelsMap = map[string]string{}

/*
	ip: 127.0.0.1
	host: host-1
	zone: ""
	business: "ppdc"
	address: ":19090"
	hotkey:
		record_number: 200
		include: ["hotkey.record", "hotkey.request_total"]
	bigkey:
		record_number: 200
		include: ["bigkey.total", "bigkey.format_record", "bigkey.bytes_value_total"]
	slowquery:
		record_number: 200
*/

type HotKeyExporterConf struct {
	RecordLimit int      `mapstructure:"record_limit"`
	Include     []string `mapstructure:"include_metrics"`
}

type BigKeyExporterConf struct {
	RecordLimit int      `mapstructure:"record_limit"`
	Include     []string `mapstructure:"include_metrics"`
}

type SlowQueryExporterConf struct {
	RecordLimit int `mapstructure:"record_limit"`
}

type RunTimeExporterConf struct {
	Enable    bool `mapstructure:"enable"`
	EnableCPU bool `mapstructure:"enable_cpu"`
	EnableMem bool `mapstructure:"enable_mem"`
	EnableGC  bool `mapstructure:"enable_gc"`
}

type ExporterConf struct {
	Enable              bool                  `mapstructure:"enable"`
	Host                string                `mapstructure:"host"`
	Address             string                `mapstructure:"address"`
	HotKeyExporterConf  HotKeyExporterConf    `mapstructure:"hotkey_exporter"`
	BigKeyExporterConf  BigKeyExporterConf    `mapstructure:"bigkey_exporter"`
	SlowQueryConf       SlowQueryExporterConf `mapstructure:"slowquery_exporter"`
	RunTimeExporterConf RunTimeExporterConf   `mapstructure:"runtime_exporter"`
}

func (e *ExporterConf) setHost(host string) {
	e.Host = host
}

func (e *ExporterConf) SetDefaultHostname() {
	e.Host = utils.GetHostname()
}

func NewDesc(metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName(Namespace, "", metricName),
		docString,
		labels,
		nil)
}

func RunPrometheusExporter(mon *Monitor, c *ExporterConf) error {
	c.SetDefaultHostname()
	BasicLabelsMap["host"] = c.Host

	expoterCount := 0

	if mon.HotKeyConf.Enable {
		expoterCount++
		prometheus.MustRegister(NewHotKeyExporter(mon, c))
		mon.BeginMonitorHotKey()
	}

	if mon.BigKeyConf.Enable {
		expoterCount++
		prometheus.MustRegister(NewBigKeyExporter(mon, c))
	}

	if mon.SlowQueryConf.Enable {
		expoterCount++
		prometheus.MustRegister(NewSlowQueryExporter(mon, c))
	}

	if c.RunTimeExporterConf.Enable {
		expoterCount++
		prometheus.MustRegister(NewRuntimeExport(c))
	}

	prometheus.MustRegister(NewNetExporter(c))

	mon.ConnectionGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "connected_clients",
		Help:      "Count of connection",
		ConstLabels: map[string]string{
			"host": c.Host,
		},
	})
	prometheus.MustRegister(mon.ConnectionGauge)
	utils.GoWithRecover(func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(c.Address, nil)
		if err != nil {
			logrus.Error("指标遥测错误：", err)
		}
	}, nil)
	return nil
}
