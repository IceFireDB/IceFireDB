package config

import (
	"net"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerC    `mapstructure:"server" json:"server"`
	Debug    DebugC     `mapstructure:"debug" json:"debug"`
	SQLite   SQLiteC    `mapstructure:"sqlite" json:"sqlite"`
	P2P      P2PS       `mapstructure:"p2p" json:"p2p"`
	UserList []UserInfo `mapstructure:"userlist" json:"userlist"`
}

type ServerC struct {
	Addr string `mapstructure:"addr" json:"addr"`
}

type SQLiteC struct {
	Filename string `mapstructure:"filename" json:"filename"`
}

type UserInfo struct {
	User     string `mapstructure:"user" json:"user"`
	Password string `mapstructure:"password" json:"password"`
}

type DebugC struct {
	Enable bool `mapstructure:"enable" json:"enable"`
	Port   int  `mapstructure:"port" json:"port"`
}

type P2PS struct {
	Enable              bool   `mapstructure:"enable" json:"enable"`
	ServiceDiscoveryID  string `mapstructure:"service_discovery_id" json:"service_discovery_id"`
	ServiceCommandTopic string `mapstructure:"service_command_topic" json:"service_command_topic"`
	ServiceDiscoverMode string `mapstructure:"service_discover_mode" json:"service_discover_mode"`
	NodeHostIP          string `mapstructure:"node_host_ip" json:"node_host_ip"`
	NodeHostPort        int    `mapstructure:"node_host_port" json:"node_host_port"`
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
