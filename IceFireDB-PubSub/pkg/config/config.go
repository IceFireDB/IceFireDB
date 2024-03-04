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

import (
	"errors"
	"net"
	"strings"

	rediscluster "github.com/chasex/redis-go-cluster"
	"github.com/spf13/viper"
)

var (
	ErrConfigNotInit       = errors.New("config not init")
	ErrDuplicateInitConfig = errors.New("duplicate init config！")
)

const (
	TypeNode    = "node"
	TypeCluster = "cluster"
)

// Do not use config directly in the agent's data link to prevent race
// Global configuration
var (
	_config          *Config
	_ruleRedisClient *rediscluster.Cluster
)

func InitConfig() error {
	if _config != nil {
		return ErrDuplicateInitConfig
	}

	err := viper.Unmarshal(&_config)
	if err != nil {
		return err
	}
	if _config.IgnoreCMD.Enable && len(_config.IgnoreCMD.CMDList) > 0 {
		CmdToUpper(_config.IgnoreCMD.CMDList)
	}

	if net.ParseIP(_config.P2P.NodeHostIP) == nil {
		_config.P2P.NodeHostIP = "0.0.0.0"
	}

	if _config.P2P.NodeHostPort < 0 || _config.P2P.NodeHostPort > 65535 {
		_config.P2P.NodeHostPort = 0
	}
	return nil
}

func Get() *Config {
	return _config
}

func CmdToUpper(list []string) {
	for k := range list {
		list[k] = strings.ToUpper(list[k])
	}
}
