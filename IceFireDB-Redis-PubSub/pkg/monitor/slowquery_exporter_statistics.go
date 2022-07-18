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
	"sort"
	"time"
)

type SlowQueryPair struct {
	startTime         time.Time
	execTime          int64
	formatRespCommand string
}

type SlowQueryStatistics struct {
	total       int
	slowQueries []SlowQueryPair
}

func respToString(resp []interface{}) string {
	/*n := len(resp.Array)
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := 0; i < n; i++ {
		buf.Write(resp.Array[i].Value)
		if i != n-1 {
			buf.WriteString(", ")
		}
	}
	buf.WriteString("]")*/
	n := len(resp)
	var buf bytes.Buffer
	buf.WriteString("[")
	for k, v := range resp {
		buf.Write(v.([]byte))
		if k != n-1 {
			buf.WriteString(", ")
		}
	}
	return buf.String()
}

func (s *SlowQueryStatistics) Filter() {
	newSlowQueries := make([]SlowQueryPair, 0, 0)
	for i, n := 0, len(s.slowQueries); i < n; i++ {
		duplicate := false
		for j := 0; j < len(newSlowQueries); j++ {
			if s.slowQueries[i].execTime == newSlowQueries[j].execTime {
				duplicate = true
				break
			}
		}

		if duplicate == false {
			newSlowQueries = append(newSlowQueries, s.slowQueries[i])
		}
	}

	s.slowQueries = nil
	s.slowQueries = newSlowQueries
}

func (s *SlowQueryStatistics) Len() int {
	return len(s.slowQueries)
}

func (s *SlowQueryStatistics) Swap(i, j int) {
	s.slowQueries[i], s.slowQueries[j] = s.slowQueries[j], s.slowQueries[i]
}

func (s *SlowQueryStatistics) Less(i, j int) bool {
	return s.slowQueries[i].execTime > s.slowQueries[j].execTime
}

func (s *SlowQueryStatistics) Init(data []*SlowQueryDataS, threshold int) {
	s.slowQueries = make([]SlowQueryPair, len(data))

	s.total = len(data)

	for i, n := 0, len(data); i < n; i++ {
		s.slowQueries[i].formatRespCommand = respToString(data[i].Resp)
		s.slowQueries[i].execTime = data[i].EndTime.Sub(data[i].StartTime).Milliseconds()
		s.slowQueries[i].startTime = data[i].StartTime
	}

	s.Filter()

	if threshold > 0 && len(s.slowQueries) > threshold {
		sort.Sort(s)
	}
}

func (s *SlowQueryStatistics) GetQueryPairArray() []SlowQueryPair {
	return s.slowQueries
}
