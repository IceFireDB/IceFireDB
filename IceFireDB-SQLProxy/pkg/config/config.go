package config

import (
	"net"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerC    `json:"server"`
	Debug    DebugC     `json:"debug"`
	Mysql    MysqlS     `json:"mysql"`
	UserList []UserInfo `json:"userlist"`
	P2P      P2PS       `json:"p2p"`
}

type ServerC struct {
	Addr string `json:"addr"`
}

type MysqlS struct {
	Addr     string `json:"addr"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
	MinAlive int    `json:"minAlive"`
	MaxAlive int    `json:"maxAlive"`
	MaxIdle  int    `json:"maxIdle"`
}

type UserInfo struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

type DebugC struct {
	Enable bool `json:"enable"`
	Port   int  `json:"port"`
}

type P2PS struct {
	Enable              bool   `json:"enable"`
	ServiceDiscoveryID  string `json:"service_discovery_id"`
	ServiceCommandTopic string `json:"service_command_topic"`
	ServiceDiscoverMode string `json:"service_discover_mode"`
	NodeHostIP          string `json:"node_host_ip"`
	NodeHostPort        int    `json:"node_host_port"`
}

func init() {
	defaultConfig = &Config{}
}

var defaultConfig *Config

func InitConfig(path string) {
	viper.SetConfigFile(path)
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	err := viper.Unmarshal(defaultConfig)
	if err != nil {
		panic(err)
	}

	if net.ParseIP(defaultConfig.P2P.NodeHostIP) == nil {
		defaultConfig.P2P.NodeHostIP = "0.0.0.0"
	}

	if defaultConfig.P2P.NodeHostPort < 0 || defaultConfig.P2P.NodeHostPort > 65535 {
		defaultConfig.P2P.NodeHostPort = 0
	}
}

func Get() *Config {
	return defaultConfig
}
