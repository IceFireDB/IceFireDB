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

package server

import (
	"bytes"
	"context"
	"fmt"

	"github.com/IceFireDB/IceFireDB-Proxy/proxy"

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/config"
	"github.com/alicebob/miniredis/v2"
	"github.com/spf13/viper"
)

var _yamlConfig = []byte(`
proxy:
  local_port: 16379

redisdb:
  type: node 
  start_nodes: "192.168.2.250:6379"
  conn_timeout: 5 # Unit: second
  conn_read_timeout: 1
  conn_write_timeout: 1
  conn_alive_timeout: 60
  conn_pool_size: 80 
  slave_operate_rate: 0 
  cluster_update_heartbeat: 30

prometheus_exporter:
  address: ":19090"
  hotkey_exporter:
    record_limit: 32
    include_metrics: [ "hotkey.record", "hotkey.count" ]
  bigkey_exporter:
    record_limit: 32
    include_metrics: [ "bigkey.count", "bigkey.record", "bigkey.value_size_sum" ]
  slowquery_exporter:
    record_limit: 16
  runtime_exporter:
    enable: true
    enable_cpu: true
    enable_mem: true
    enable_gc: true
`)

type ProxyTest struct {
	cancel context.CancelFunc
	minis  *miniredis.Miniredis
	proxy  *proxy.Proxy
}

func newProxy2Server() *ProxyTest {
	ms, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	viper.SetConfigType("yaml")
	err = viper.ReadConfig(bytes.NewBuffer(_yamlConfig))
	if err != nil {
		panic(err)
	}

	err = config.InitConfig()
	if err != nil {
		panic(err)
	}
	// config.Get().RedisDB.Type = config.TypeNode
	// config.Get().RedisDB.StartNodes = ms.Addr()

	p, err := proxy.New()
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.TODO())
	errSignal := make(chan error)
	go func() {
		p.Run(ctx, errSignal)
	}()
	<-errSignal

	return &ProxyTest{minis: ms, proxy: p, cancel: cancel}
}

func (pt *ProxyTest) Close() {
	pt.cancel()
	pt.minis.Close()
}

func (pt *ProxyTest) Direct() *miniredis.Miniredis {
	return pt.minis
}

func (pt *ProxyTest) Addr() string {
	return fmt.Sprintf(":%d", config.Get().Proxy.LocalPort)
	// return pt.minis.Addr()
}

var defaultProxy = newProxy2Server()

func Addr() string {
	return defaultProxy.Addr()
}

func Clear() {
	defaultProxy.minis.FlushAll()
}

func Direct() *miniredis.Miniredis {
	return defaultProxy.minis
}
