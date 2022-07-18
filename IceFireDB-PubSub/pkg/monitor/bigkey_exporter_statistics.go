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
	"sort"
	"time"
)

type BigKeyPair struct {
	key       string
	valueSize int
	startTime time.Time
}

type BigKeyStatistics struct {
	bigKeys      []BigKeyPair
	keyCount     int
	valueSizeSum int
}

func (b *BigKeyStatistics) Filter() {
	auxiliary := make(map[string]BigKeyPair)
	for _, bigKeyPair := range b.bigKeys {
		if v, ok := auxiliary[bigKeyPair.key]; !ok {
			auxiliary[bigKeyPair.key] = bigKeyPair
		} else {
			if bigKeyPair.valueSize > v.valueSize {
				auxiliary[bigKeyPair.key] = bigKeyPair
			}
		}
	}

	newBigKeys := make([]BigKeyPair, 0, len(auxiliary))
	for _, bigkeyPair := range auxiliary {
		newBigKeys = append(newBigKeys, bigkeyPair)
	}

	b.bigKeys = nil
	b.bigKeys = newBigKeys
}

func (b *BigKeyStatistics) Init(data []BigKeyDataS, threshold int) {
	b.keyCount = len(data)
	b.bigKeys = make([]BigKeyPair, b.keyCount)

	for i, bigKeyData := range data {
		b.valueSizeSum += bigKeyData.valueSize
		b.bigKeys[i].key = bigKeyData.key
		b.bigKeys[i].valueSize = bigKeyData.valueSize
		b.bigKeys[i].startTime = bigKeyData.time
	}

	b.Filter()

	if threshold > 0 && len(b.bigKeys) > threshold {
		sort.Sort(b)
		b.bigKeys = b.bigKeys[:threshold]
	}
}

func (b *BigKeyStatistics) Len() int {
	return len(b.bigKeys)
}

func (b *BigKeyStatistics) Swap(i, j int) {
	b.bigKeys[i], b.bigKeys[j] = b.bigKeys[j], b.bigKeys[i]
}

func (b *BigKeyStatistics) Less(i, j int) bool {
	return b.bigKeys[i].valueSize > b.bigKeys[j].valueSize
}

func (b *BigKeyStatistics) GetBigKeyCount() int {
	return b.keyCount
}

func (b *BigKeyStatistics) GetBigKeyValueSizeSum() int {
	return b.valueSizeSum
}

func (b *BigKeyStatistics) GetBigKeyPairArray() []BigKeyPair {
	return b.bigKeys
}
