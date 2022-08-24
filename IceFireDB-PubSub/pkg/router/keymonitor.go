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

package router

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/IceFireDB/IceFireDB-PubSub/pkg/monitor"
	"github.com/IceFireDB/IceFireDB-PubSub/utils"
)

type valueIndexFunc func(mon *monitor.Monitor, arg []interface{}, reply interface{}) bool

const IgnoreWCONFIG = "WCONFIG"

var (
	HotKeyCmdList = []string{"GET", "HGET", "MGET"}
	sqIgnoreCMD   = []string{IgnoreWCONFIG}
	bigKeyHandle  = map[string]valueIndexFunc{
		"GET":   BHGET,
		"SET":   BHSET,
		"HGET":  BHHGET,
		"HSET":  BHHSET,
		"MGET":  BHMGET,
		"MSET":  BHMSET,
		"SETNX": BHSET,
		"SETEX": BHHSET,
		"LPUSH": BHPUSH,
		"RPUSH": BHPUSH,
	}
)

var _monitor *monitor.Monitor

func KeyMonitorMiddleware(mon *monitor.Monitor, slowQueryIgnoreCMD []string) HandlerFunc {
	_monitor = mon
	sqIgnoreCMD = slowQueryIgnoreCMD

	exec := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   monitor.Namespace,
		Name:        "commands_processed_total",
		Help:        "Count of exec total",
		ConstLabels: monitor.BasicLabelsMap,
	})

	cmdMetric := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   monitor.Namespace,
		Name:        "commands_total",
		Help:        "Count of command exec total",
		ConstLabels: monitor.BasicLabelsMap,
	}, []string{"cmd"})
	prometheus.MustRegister(exec)
	prometheus.MustRegister(cmdMetric)
	labels := sync.Pool{New: func() interface{} {
		return make(map[string]string)
	}}
	return func(ctx *Context) error {
		exec.Inc()
		l := labels.Get().(map[string]string)
		l["cmd"] = ctx.Cmd
		cmdMetric.With(l).Inc()
		labels.Put(l)

		if len(ctx.Args) == 1 {
			return ctx.Next()
		}

		if mon.SlowQueryConf.Enable && !utils.InArray(ctx.Cmd, sqIgnoreCMD) {
			commandStartTime := time.Now()
			defer func() {
				mon.IsSlowQuery(ctx.Args, commandStartTime, time.Now())
			}()
		}

		if mon.HotKeyConf.Enable && utils.InArray(ctx.Cmd, HotKeyCmdList) && mon.IsShouldPutHotKey() {
			var keyIndex []uint8
			fn, ok := cmdKeyMap[ctx.Cmd]
			if !ok {
				fn = firstKey
			}
			keyIndex = fn(ctx.Args)
			for _, v := range keyIndex {
				mon.PutHotKey(utils.GetInterfaceString(ctx.Args[v]), nil)
			}
		}

		if mon.BigKeyConf.Enable {
			if fn, ok := bigKeyHandle[ctx.Cmd]; ok {
				defer fn(mon, ctx.Args, ctx.Reply)
			}
		}
		return ctx.Next()
	}
}

func BHGET(mon *monitor.Monitor, args []interface{}, reply interface{}) bool {
	if reply == nil {
		return false
	}
	l := 0
	if r, ok := reply.([]byte); ok {
		l = len(r)
	}
	return mon.PutBigKey(utils.GetInterfaceString(args[1]), l)
}

func BHSET(mon *monitor.Monitor, args []interface{}, _ interface{}) bool {
	return mon.PutBigKey(utils.GetInterfaceString(args[1]), len(utils.GetInterfaceString(args[2])))
}

func BHHGET(mon *monitor.Monitor, args []interface{}, reply interface{}) bool {
	if reply == nil {
		return false
	}
	l := 0
	if r, ok := reply.([]byte); ok {
		l = len(r)
	}
	return mon.PutBigKey(utils.GetInterfaceString(args[1]), l)
}

func BHHSET(mon *monitor.Monitor, args []interface{}, _ interface{}) bool {
	return mon.PutBigKey(utils.GetInterfaceString(args[1]), len(utils.GetInterfaceString(args[3])))
}

func BHMGET(mon *monitor.Monitor, args []interface{}, reply interface{}) bool {
	r, ok := reply.([]interface{})
	if !ok {
		return false
	}
	for k, v := range r {
		if vl, ok := v.([]byte); ok {
			mon.PutBigKey(utils.GetInterfaceString(args[k+1]), len(vl))
		}
	}
	return false
}

func BHMSET(mon *monitor.Monitor, args []interface{}, _ interface{}) bool {
	l := len(args)
	for i := 1; i < l; i++ {
		if i%2 == 0 {
			continue
		}
		mon.PutBigKey(utils.GetInterfaceString(args[i]), len(utils.GetInterfaceString(args[i+1])))
	}
	return false
}

func BHPUSH(mon *monitor.Monitor, args []interface{}, _ interface{}) bool {
	l := len(args)
	for i := 2; i < l; i++ {
		mon.PutBigKey(utils.GetInterfaceString(args[1]), len(utils.GetInterfaceString(args[i])))
	}
	return false
}
