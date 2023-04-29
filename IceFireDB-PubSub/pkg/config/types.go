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

package config

type Config struct {
	Proxy       ProxyS       `mapstructure:"proxy"`
	RedisDB     RedisDBS     `mapstructure:"redisdb"`
	PprofDebug  PprofDebugS  `mapstructure:"pprof_debug"`
	Log         LogS         `mapstructure:"log"`
	IPWhiteList IPWhiteListS `mapstructure:"ip_white_list"`
	Cache       CacheS       `mapstructure:"cache"`
	IgnoreCMD   IgnoreCMDS   `mapstructure:"ignore_cmd"`

	P2P P2PS `mapstructure:"p2p"`
}

type P2PS struct {
	Enable              bool   `mapstructure:"enable" json:"enable"`
	ServiceDiscoveryID  string `mapstructure:"service_discovery_id" json:"service_discovery_id"`
	ServiceCommandTopic string `mapstructure:"service_command_topic" json:"service_command_topic"`
	ServiceDiscoverMode string `mapstructure:"service_discover_mode" json:"service_discover_mode"`
}

type ProxyS struct {
	LocalPort  int  `mapstructure:"local_port" json:"local_port"`   // Port to listen on locally when proxying
	EnableMTLS bool `mapstructure:"enable_mtls" json:"enable_mtls"` // Cluster nodes, multiple, split
}

type PprofDebugS struct {
	Enable bool   `mapstructure:"enable"`
	Port   uint16 `mapstructure:"port"`
}

type LogS struct {
	Level     string `mapstructure:"level"`
	cmd       bool   `mapstructure:"cmd"`
	OutPut    string `mapstructure:"output"`
	Key       string `mapstructure:"key"`
	RedisHost string `mapstructure:"redis_host"`
	RedisPort int    `mapstructure:"redis_port"`
}

type CacheS struct {
	Enable bool `mapstructure:"enable"`
	// cache The maximum number of items in
	MaxItemsSize int `mapstructure:"max_items_size"`
	// cache Kv default expiration time (unit: milliseconds)
	DefaultExpiration int `mapstructure:"default_expiration"`
	// cache Expired KV cleaning cycle (unit: second)
	CleanupInterval int `mapstructure:"cleanup_interval"`
}

type IgnoreCMDS struct {
	Enable bool `mapstructure:"enable" json:"enable"`
	// Slowly check omitted keys
	CMDList []string `mapstructure:"cmd_list" json:"cmd_list"`
}

// RedisClusterConf is redis cluster configure options
type RedisDBS struct {
	// node、cluster
	Type       string `mapstructure:"type"`
	StartNodes string `mapstructure:"start_nodes"`
	// Connection timeout parameter of cluster nodes Unit: ms
	ConnTimeOut int `mapstructure:"conn_timeout"`
	// Cluster node read timeout parameter Unit: ms
	ConnReadTimeOut int `mapstructure:"conn_read_timeout"`
	// Cluster node write timeout parameter Unit: ms
	ConnWriteTimeOut int `mapstructure:"conn_write_timeout"`
	// Cluster node TCP idle survival time Unit: seconds
	ConnAliveTimeOut int `mapstructure:"conn_alive_timeout"`
	// The size of the TCP connection pool for each node in the cluster
	ConnPoolSize int `mapstructure:"conn_pool_size"`
}

type IPWhiteListS struct {
	Enable bool     `json:"enable"`
	List   []string `json:"list"`
}
