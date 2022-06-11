package mysql

import (
	"context"
	"errors"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/server"
	"github.com/IceFireDB/IceFireDB-SQLProxy/utils"
	"github.com/sirupsen/logrus"
	"io"
	"net"
)

func NewMysqlProxy() *mysqlProxy {
	return newMysqlProxy()
}

func (m *mysqlProxy) Run(ctx context.Context) {
	m.ctx = ctx
	m.closed.Store(true)
	ln, err := net.Listen("tcp4", config.Get().Server.Addr)
	if err != nil {
		logrus.Errorf("mysql代理监听端口错误：%v", err)
		return
	}
	utils.GoWithRecover(func() {
		if <-ctx.Done(); true {
			_ = ln.Close()
			m.closed.Store(true)
		}
	}, nil)
	// 标志开启
	m.closed.Store(false)

	logrus.Infof("启动中间件，监听地址：%s\n", config.Get().Server.Addr)
	// 初始化客户端连接池
	if err := m.initClientPool(); err != nil {
		logrus.Errorf("initClientPool error: %v", err)
		return
	}
	// p2p
	if config.Get().P2P.Enable {
		initP2P(m)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			panic(err)
		}
		go m.onConn(conn)
	}
}

// 创建代理，初始化账户密码存储组件
func newMysqlProxy() *mysqlProxy {
	p := &mysqlProxy{}
	p.server = server.NewDefaultServer()
	p.credential = server.NewInMemoryProvider()
	for _, info := range config.Get().UserList {
		p.credential.(*server.InMemoryProvider).AddUser(info.User, info.Password)
	}
	return p
}

func (m *mysqlProxy) initClientPool() error {
	mc := config.Get().Mysql
	m.pool = client.NewPool(logrus.Infof, mc.MinAlive, mc.MaxAlive, mc.MaxIdle, mc.Addr, mc.User, mc.Password, mc.DBName)
	return nil
}
