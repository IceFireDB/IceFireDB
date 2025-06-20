package config

import (
	"net"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerC    `yaml:"server"`
	Debug    DebugC     `yaml:"debug"`
	Mysql    MysqlS     `yaml:"mysql"`
	UserList []UserInfo `yaml:"userlist"`
	P2P      P2PS       `yaml:"p2p"`
}

type ServerC struct {
	Addr string `yaml:"addr"`
}

type MysqlC struct {
	Addr     string `yaml:"addr"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	MinAlive int    `yaml:"minAlive"`
	MaxAlive int    `yaml:"maxAlive"`
	MaxIdle  int    `yaml:"maxIdle"`
}

type MysqlS struct {
	Admin    MysqlC `yaml:"admin"`
	Readonly MysqlC `yaml:"readonly"`
}

type UserInfo struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type DebugC struct {
	Enable bool `yaml:"enable"`
	Port   int  `yaml:"port"`
}

type P2PS struct {
	Enable              bool   `yaml:"enable"`
	ServiceDiscoveryID  string `yaml:"serviceDiscoveryId"`
	ServiceCommandTopic string `yaml:"serviceCommandTopic"`
	ServiceDiscoverMode string `yaml:"serviceDiscoverMode"`
	NodeHostIP          string `yaml:"nodeHostIp"`
	NodeHostPort        int    `yaml:"nodeHostPort"`
	AdminTopic          string `yaml:"adminTopic"`
	ReadonlyTopic       string `yaml:"readonlyTopic"`
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
