package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerC    `json:"server"`
	Debug    DebugC     `json:"debug"`
	SQLite   SQLiteC    `json:"sqlite"`
	P2P      P2PS       `json:"p2p"`
	UserList []UserInfo `json:"userlist"`
}

type ServerC struct {
	Addr string `json:"addr"`
}

type SQLiteC struct {
	Filename string `json:"filename"`
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
}

func Get() *Config {
	return defaultConfig
}
