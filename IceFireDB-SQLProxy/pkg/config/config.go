package config

import (
	"fmt"
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
	// IdleTimeout closes a client connection that has not sent a packet for that many
	// seconds. 0 (the default) keeps the pre-1.0.0 behaviour of waiting forever.
	IdleTimeout int `yaml:"idleTimeout"`
	// WriteTimeout bounds how long a single response may take to be written. 0 means
	// unlimited.
	WriteTimeout int `yaml:"writeTimeout"`
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
	// Role selects the upstream connection pool this tenant is served from.
	// "admin" (default when empty) may write, "readonly" may only read.
	Role string `yaml:"role"`
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
	// AuthToken is the shared HMAC-SHA256 secret used to authenticate replicated
	// writes. When empty, peers do not sign and unsigned messages are accepted
	// (the pre-1.0.0 behaviour) — a warning is logged at startup.
	AuthToken string `yaml:"authToken"`
	// AllowedPeers restricts which libp2p peer IDs may publish to this node. Empty
	// means "any peer that can join the topic".
	AllowedPeers []string `yaml:"allowedPeers"`
	// BootstrapPeers lists multiaddrs to dial at startup. When set, public DHT
	// discovery is disabled and the listed peers are the only way in.
	BootstrapPeers []string `yaml:"bootstrapPeers"`
}

func init() {
	defaultConfig = &Config{}
}

var defaultConfig *Config

// InitConfig loads the configuration file. It reports a missing or malformed file as an
// error so the process can explain itself and exit, instead of panicking with a stack
// trace.
func InitConfig(path string) error {
	viper.SetConfigFile(path)
	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}

	if err := viper.Unmarshal(defaultConfig); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}

	if net.ParseIP(defaultConfig.P2P.NodeHostIP) == nil {
		defaultConfig.P2P.NodeHostIP = "0.0.0.0"
	}

	if defaultConfig.P2P.NodeHostPort < 0 || defaultConfig.P2P.NodeHostPort > 65535 {
		defaultConfig.P2P.NodeHostPort = 0
	}

	if defaultConfig.Server.IdleTimeout < 0 {
		defaultConfig.Server.IdleTimeout = 0
	}
	if defaultConfig.Server.WriteTimeout < 0 {
		defaultConfig.Server.WriteTimeout = 0
	}
	return nil
}

func Get() *Config {
	return defaultConfig
}
