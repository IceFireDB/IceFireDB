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
	"runtime"

	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"
)

var runtimeGaugeVec = map[string]*prometheus.Desc{
	"cpu.count":      NewDesc("cpu_count", "Count of logical CPUs usable by the current process.", BasicLabels),
	"cpu.goroutines": NewDesc("cpu_goroutines", "Count of goroutines that currently exist.", BasicLabels),

	"mem.alloc":   NewDesc("mem_alloc", "mem_alloc is the same as HeapAlloc.", BasicLabels),
	"mem.total":   NewDesc("mem_total", "mem_total is cumulative bytes allocated for heap objects.", BasicLabels),
	"mem.sys":     NewDesc("mem_sys", "mem_sys is the total bytes of memory obtained from the OS.", BasicLabels),
	"mem.lookups": NewDesc("mem_lookups", "mem_lookups is the number of pointer lookups performed by the runtime.", BasicLabels),
	"mem.malloc":  NewDesc("mem_malloc", "mem_malloc is the cumulative count of heap objects allocated.", BasicLabels),
	"mem.frees":   NewDesc("mem_frees", "mem_frees is the cumulative count of heap objects freed.", BasicLabels),

	"mem.heap.alloc":    NewDesc("mem_heap_alloc", "mem_heap_alloc is bytes of allocated heap objects.", BasicLabels),
	"mem.heap.sys":      NewDesc("mem_heap_sys", "mem_heap_sys is bytes of heap memory obtained from the OS.", BasicLabels),
	"mem.heap.idle":     NewDesc("mem_heap_idle", "mem_heap_idle is bytes in idle (unused) spans.", BasicLabels),
	"mem.heap.inuse":    NewDesc("mem_heap_inuse", "mem_heap_inuse is bytes in in-use spans.", BasicLabels),
	"mem.heap.released": NewDesc("mem_heap_released", "mem_heap_released is bytes of physical memory returned to the OS.", BasicLabels),
	"mem.heap.objects":  NewDesc("mem_heap_objects", "mem_heap_objects is the number of allocated heap objects.", BasicLabels),

	"mem.stack.inuse":        NewDesc("mem_stack_inuse", "mem_stack_inuse is bytes in stack spans.", BasicLabels),
	"mem.stack.sys":          NewDesc("mem_stack_sys", "mem_stack_sys is bytes of stack memory obtained from the OS.", BasicLabels),
	"mem.stack.mspan_inuse":  NewDesc("mem_stack_mspan_inuse", "mem_stack_mspan_inuse Off-heap memory statistics.", BasicLabels),
	"mem.stack.mspan_sys":    NewDesc("mem_stack_mspan_sys", "mem_stack_mspan_sys is bytes of memory obtained from the OS for mspan.", BasicLabels),
	"mem.stack.mcache_inuse": NewDesc("mem_stack_mcache_inuse", "mem_stack_mcache_inuse is bytes of allocated mcache structures.", BasicLabels),
	"mem.stack.mcache_sys":   NewDesc("mem_stack_mcache_sys", "mem_stack_mcache_sys is bytes of memory obtained from the OS for mcache structures.", BasicLabels),
	"mem.othersys":           NewDesc("mem_othersys", "mem_othersys is bytes of memory in miscellaneous off-heap runtime allocations.", BasicLabels),

	"mem.gc.sys":          NewDesc("mem_gc_sys", "mem_gc_sys is bytes of memory in garbage collection metadata.", BasicLabels),
	"mem.gc.next":         NewDesc("mem_gc_next", "mem_gc_next is the target heap size of the next GC cycle.", BasicLabels),
	"mem.gc.last":         NewDesc("mem_gc_last", "mem_gc_last is the time the last garbage collection finished, as nanoseconds since 1970 (the UNIX epoch).", BasicLabels),
	"mem.gc.pause_total":  NewDesc("mem_gc_pause_total", "mem_gc_pause_total is the cumulative nanoseconds in GC stop-the-world pauses since the program started.", BasicLabels),
	"mem.gc.pause":        NewDesc("mem_gc_pause", "mem_gc_pause is a circular buffer of recent GC stop-the-world pause times in nanoseconds.", BasicLabels),
	"mem.gc.count":        NewDesc("mem_gc_count", "mem_gc_count is the number of completed GC cycles.", BasicLabels),
	"mem.gc.cpu_fraction": NewDesc("mem_gc_next_cpu_fraction", "mem_gc_next_cpu_fraction is the fraction of this program's available CPU time used by the GC since the program started.", BasicLabels),
}

type RuntimeExporter struct {
	collect        *collector
	runtimeMetrics map[string]*prometheus.Desc
	basicConf      *ExporterConf
}

func NewRuntimeExport(c *ExporterConf) *RuntimeExporter {
	metrics := make(map[string]*prometheus.Desc)

	for k, v := range runtimeGaugeVec {
		metrics[k] = v
	}

	return &RuntimeExporter{
		basicConf:      c,
		runtimeMetrics: metrics,
		collect: &collector{
			EnableCPU: c.RunTimeExporterConf.EnableCPU,
			EnableMem: c.RunTimeExporterConf.EnableMem,
			EnableGC:  c.RunTimeExporterConf.EnableGC,
		},
	}
}

func (r *RuntimeExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range r.runtimeMetrics {
		ch <- metric
	}
}

func (r *RuntimeExporter) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("runtime promethues collect panic", r)
		}
	}()
	fields := r.collect.oneOff()
	values := fields.Values()

	for metricKey, runtimeMetric := range r.runtimeMetrics {
		if value, ok := values[metricKey]; ok {
			switch value.(type) {
			case int64:
				ch <- prometheus.MustNewConstMetric(
					runtimeMetric,
					prometheus.GaugeValue,
					float64(value.(int64)),
					r.basicConf.Host,
				)
			case float64:
				ch <- prometheus.MustNewConstMetric(
					runtimeMetric,
					prometheus.GaugeValue,
					value.(float64),
					r.basicConf.Host,
				)
			default:
				ch <- prometheus.MustNewConstMetric(
					runtimeMetric,
					prometheus.GaugeValue,
					0,
					r.basicConf.Host,
				)
			}
		}
	}
}

type FieldsFunc func(Fields)

type collector struct {
	EnableCPU bool

	EnableMem bool

	EnableGC bool
}

func (c *collector) oneOff() Fields {
	return c.collectStats()
}

func (c *collector) collectStats() Fields {
	fields := Fields{}

	if c.EnableCPU {
		cStats := cpuStats{
			NumGoroutine: int64(runtime.NumGoroutine()),

			NumCPU: int64(runtime.NumCPU()),
		}
		c.collectCPUStats(&fields, &cStats)
	}

	if c.EnableMem {
		m := &runtime.MemStats{}
		runtime.ReadMemStats(m)
		c.collectMemStats(&fields, m)
		if c.EnableGC {
			c.collectGCStats(&fields, m)
		}
	}

	return fields
}

func (*collector) collectCPUStats(fields *Fields, s *cpuStats) {
	fields.NumCPU = s.NumCPU
	fields.NumGoroutine = s.NumGoroutine
}

func (*collector) collectMemStats(fields *Fields, m *runtime.MemStats) {
	fields.Alloc = int64(m.Alloc)
	fields.TotalAlloc = int64(m.TotalAlloc)
	fields.Sys = int64(m.Sys)
	fields.Lookups = int64(m.Lookups)
	fields.Mallocs = int64(m.Mallocs)
	fields.Frees = int64(m.Frees)

	fields.HeapAlloc = int64(m.HeapAlloc)
	fields.HeapSys = int64(m.HeapSys)
	fields.HeapIdle = int64(m.HeapIdle)
	fields.HeapInuse = int64(m.HeapInuse)
	fields.HeapReleased = int64(m.HeapReleased)
	fields.HeapObjects = int64(m.HeapObjects)

	fields.StackInuse = int64(m.StackInuse)
	fields.StackSys = int64(m.StackSys)
	fields.MSpanInuse = int64(m.MSpanInuse)
	fields.MSpanSys = int64(m.MSpanSys)
	fields.MCacheInuse = int64(m.MCacheInuse)
	fields.MCacheSys = int64(m.MCacheSys)

	fields.OtherSys = int64(m.OtherSys)
}

func (*collector) collectGCStats(fields *Fields, m *runtime.MemStats) {
	fields.GCSys = int64(m.GCSys)
	fields.NextGC = int64(m.NextGC)
	fields.LastGC = int64(m.LastGC)
	fields.PauseTotalNs = int64(m.PauseTotalNs)
	fields.PauseNs = int64(m.PauseNs[(m.NumGC+255)%256])
	fields.NumGC = int64(m.NumGC)
	fields.GCCPUFraction = float64(m.GCCPUFraction)
}

type cpuStats struct {
	NumCPU       int64
	NumGoroutine int64
}

type Fields struct {
	NumCPU       int64 `json:"cpu.count"`
	NumGoroutine int64 `json:"cpu.goroutines"`

	Alloc      int64 `json:"mem.alloc"`
	TotalAlloc int64 `json:"mem.total"`
	Sys        int64 `json:"mem.sys"`
	Lookups    int64 `json:"mem.lookups"`
	Mallocs    int64 `json:"mem.malloc"`
	Frees      int64 `json:"mem.frees"`

	HeapAlloc    int64 `json:"mem.heap.alloc"`
	HeapSys      int64 `json:"mem.heap.sys"`
	HeapIdle     int64 `json:"mem.heap.idle"`
	HeapInuse    int64 `json:"mem.heap.inuse"`
	HeapReleased int64 `json:"mem.heap.released"`
	HeapObjects  int64 `json:"mem.heap.objects"`

	StackInuse  int64 `json:"mem.stack.inuse"`
	StackSys    int64 `json:"mem.stack.sys"`
	MSpanInuse  int64 `json:"mem.stack.mspan_inuse"`
	MSpanSys    int64 `json:"mem.stack.mspan_sys"`
	MCacheInuse int64 `json:"mem.stack.mcache_inuse"`
	MCacheSys   int64 `json:"mem.stack.mcache_sys"`

	OtherSys int64 `json:"mem.othersys"`

	GCSys         int64   `json:"mem.gc.sys"`
	NextGC        int64   `json:"mem.gc.next"`
	LastGC        int64   `json:"mem.gc.last"`
	PauseTotalNs  int64   `json:"mem.gc.pause_total"`
	PauseNs       int64   `json:"mem.gc.pause"`
	NumGC         int64   `json:"mem.gc.count"`
	GCCPUFraction float64 `json:"mem.gc.cpu_fraction"`
}

func (f *Fields) Values() map[string]interface{} {
	return map[string]interface{}{
		"cpu.count":      f.NumCPU,
		"cpu.goroutines": f.NumGoroutine,

		"mem.alloc":   f.Alloc,
		"mem.total":   f.TotalAlloc,
		"mem.sys":     f.Sys,
		"mem.lookups": f.Lookups,
		"mem.malloc":  f.Mallocs,
		"mem.frees":   f.Frees,

		"mem.heap.alloc":    f.HeapAlloc,
		"mem.heap.sys":      f.HeapSys,
		"mem.heap.idle":     f.HeapIdle,
		"mem.heap.inuse":    f.HeapInuse,
		"mem.heap.released": f.HeapReleased,
		"mem.heap.objects":  f.HeapObjects,

		"mem.stack.inuse":        f.StackInuse,
		"mem.stack.sys":          f.StackSys,
		"mem.stack.mspan_inuse":  f.MSpanInuse,
		"mem.stack.mspan_sys":    f.MSpanSys,
		"mem.stack.mcache_inuse": f.MCacheInuse,
		"mem.stack.mcache_sys":   f.MCacheSys,
		"mem.othersys":           f.OtherSys,

		"mem.gc.sys":          f.GCSys,
		"mem.gc.next":         f.NextGC,
		"mem.gc.last":         f.LastGC,
		"mem.gc.pause_total":  f.PauseTotalNs,
		"mem.gc.pause":        f.PauseNs,
		"mem.gc.count":        f.NumGC,
		"mem.gc.cpu_fraction": float64(f.GCCPUFraction),
	}
}
