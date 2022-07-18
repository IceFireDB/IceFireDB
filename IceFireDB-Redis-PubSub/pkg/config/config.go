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
	"strings"

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/rediscluster"
	"github.com/spf13/viper"
)

var (
	ErrConfigNotInit       = errors.New("Config not init")
	ErrDuplicateInitConfig = errors.New("Duplicate init config！")
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

	if _config.Monitor.SlowQueryConf.Enable && len(_config.Monitor.SlowQueryConf.SlowQueryIgnoreCMD) > 0 {
		CmdToUpper(_config.Monitor.SlowQueryConf.SlowQueryIgnoreCMD)
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
