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

import "github.com/IceFireDB/IceFireDB-PubSub/pkg/monitor"

type Config struct {
	Proxy                  ProxyS                `mapstructure:"proxy"`
	RedisDB                RedisDBS              `mapstructure:"redisdb"`
	PprofDebug             PprofDebugS           `mapstructure:"pprof_debug"`
	Log                    LogS                  `mapstructure:"log"`
	IPWhiteList            IPWhiteListS          `mapstructure:"ip_white_list"`
	Cache                  CacheS                `mapstructure:"cache"`
	Monitor                MonitorS              `mapstructure:"monitor"`
	PrometheusExporterConf *monitor.ExporterConf `mapstructure:"prometheus_exporter"`
	IgnoreCMD              IgnoreCMDS            `mapstructure:"ignore_cmd"`

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

type HotKeyConfS struct {
	// Whether to enable hotkey monitoring
	Enable bool `mapstructure:"enable" json:"enable"`
	// Monitoring task time round policy: Single sleep time (unit: second)
	MonitorJobInterval int `mapstructure:"monitor_job_interval" json:"monitor_job_interval"`
	// Monitoring task Time round policy: Duration of a monitoring task (unit: second)
	MonitorJobLifetime int `mapstructure:"monitor_job_lifetime" json:"monitor_job_lifetime"`
	// Hotkey identification threshold. After monitoring, if the key processing capacity per second is higher than the threshold, the key is considered as a hotkey
	SecondHotThreshold int `mapstructure:"second_hot_threshold" json:"second_hot_threshold"`
	// Command processing increase rate (unit: second) If the command processing increase rate is higher than the threshold, the middleware immediately enables hotkey monitoring
	SecondIncreaseThreshold int `mapstructure:"second_increase_threshold" json:"second_increase_threshold"`
	// The maximum hotkey storage space is based on the LRU policy
	LruSize int `mapstructure:"lru_size" json:"-"`
	// Whether to enable middleware caching
	EnableCache bool `mapstructure:"enable_cache" json:"enable_cache"`
	// Maximum cache entry lifetime
	MaxCacheLifeTime int `mapstructure:"max_cache_life_time" json:"max_cache_life_time"`
}

type BigKeyConfS struct {
	// Whether to enable bigkey monitoring
	Enable bool `mapstructure:"enable" json:"enable"`
	// Maximum redis key length (unit: byte)
	KeyMaxBytes int `mapstructure:"key_max_bytes" json:"key_max_bytes"`
	// Maximum redis Value length (unit: byte)
	ValueMaxBytes int `mapstructure:"value_max_bytes" json:"value_max_bytes"`
	// Bigkey Specifies the maximum storage space. Storage is based on the LRU policy
	LruSize int `mapstructure:"lru_size" json:"-"`
	// Whether to enable middleware caching
	EnableCache bool `mapstructure:"enable_cache" json:"enable_cache"`
	// Maximum cache entry lifetime
	MaxCacheLifeTime int `mapstructure:"max_cache_life_time" json:"max_cache_life_time"`
}

type SlowQueryConfS struct {
	Enable bool `mapstructure:"enable" json:"enable"`
	// Slow Query time threshold: Higher than this threshold, slow Query LRU memory is entered
	SlowQueryThreshold int `mapstructure:"slow_query_threshold" json:"slow_query_threshold"`
	// The maximum hotkey storage space is based on the queue policy
	MaxListSize int `mapstructure:"max_list_size" json:"-"`
	// Slowly check omitted keys
	SlowQueryIgnoreCMD []string `mapstructure:"slow_query_ignore_cmd" json:"slow_query_ignore_cmd"`
}

type IgnoreCMDS struct {
	Enable bool `mapstructure:"enable" json:"enable"`
	// Slowly check omitted keys
	CMDList []string `mapstructure:"cmd_list" json:"cmd_list"`
}

type MonitorS struct {
	HotKeyConf    HotKeyConfS    `mapstructure:"hotkey"`
	BigKeyConf    BigKeyConfS    `mapstructure:"bigkey"`
	SlowQueryConf SlowQueryConfS `mapstructure:"slowquery"`
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
	ConnPoolSize           int `mapstructure:"conn_pool_size"`
	SlaveOperateRate       int `mapstructure:"slave_operate_rate"`
	ClusterUpdateHeartbeat int `mapstructure:"cluster_update_heartbeat"`
}

type IPWhiteListS struct {
	Enable bool     `json:"enable"`
	List   []string `json:"list"`
}
