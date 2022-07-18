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
	"fmt"
	"math/rand"
	"testing"
)

func mockHotkeyPair(n int) []HotKeyPair {
	pairs := make([]HotKeyPair, n)
	for i := 0; i < n; i++ {
		pairs[i].key = fmt.Sprintf("mock-key-%d", rand.Intn(n))
		pairs[i].count = uint64(rand.Intn(n) + rand.Intn(n)*2)
	}
	return pairs
}

func checkFilter(paris []HotKeyPair) bool {
	aux := make(map[string]bool)
	for _, hotkey := range paris {

		if _, ok := aux[hotkey.key]; ok {
			return false
		}

		aux[hotkey.key] = true
	}

	return true
}

func TestHotkeyStatistics_Filter(t *testing.T) {
	testRound := 1 << 4
	dataCount := 1 << 16
	for i := 0; i < testRound; i++ {
		t.Run(fmt.Sprintf("parallel round %d", i+1), func(t *testing.T) {
			statistics := new(HotKeyStatistics)
			statistics.hotkeys = mockHotkeyPair(dataCount)
			statistics.Filter()
			if pass := checkFilter(statistics.hotkeys); !pass {
				t.Error("Filter test failed")
			}
		})
	}
}
