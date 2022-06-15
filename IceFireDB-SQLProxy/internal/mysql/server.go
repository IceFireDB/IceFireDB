package mysql

import (
	"context"
	"errors"
	"fmt"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/server"
	"github.com/IceFireDB/IceFireDB-SQLProxy/utils"
	"github.com/sirupsen/logrus"
	"io"
	"net"
)

func Run(ctx context.Context) (err error) {
	ms := newMysqlProxy()
	ms.ctx = ctx
	ms.closed.Store(true)
	// 初始化客户端连接池
	if err = ms.initClientPool(); err != nil {
		return fmt.Errorf("initClientPool error: %v", err)
	}
	ln, err := net.Listen("tcp4", config.Get().Server.Addr)
	if err != nil {
		logrus.Errorf("mysql代理监听端口错误：%v", err)
		return
	}
	utils.GoWithRecover(func() {
		if <-ctx.Done(); true {
			_ = ln.Close()
			ms.closed.Store(true)
		}
	}, nil)
	// 标志开启
	ms.closed.Store(false)
	logrus.Infof("启动中间件，监听地址：%s\n", config.Get().Server.Addr)
	// p2p
	if config.Get().P2P.Enable {
		initP2P(ms)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				continue
			}
			panic(err)
		}
		go ms.onConn(conn)
	}
	return
}

// 创建代理，初始化账户密码存储组件
func newMysqlProxy() *mysqlProxy {
	p := &mysqlProxy{}
	p.server = server.NewDefaultServer()
	p.credential = server.NewInMemoryProvider()
	for _, info := range config.Get().UserList {
		p.credential.AddUser(info.User, info.Password)
	}
	return p
}

func (m *mysqlProxy) initClientPool() error {
	mc := config.Get().Mysql
	pool, err := client.NewPool(logrus.Infof, mc.MinAlive, mc.MaxAlive, mc.MaxIdle, mc.Addr, mc.User, mc.Password, mc.DBName)
	if err != nil {
		return err
	}
	m.pool = pool
	return nil
}
