package mysql

import (
	"context"
	"errors"
	"github.com/IceFireDB/IceFireDB-SQLite/internal/sqlite"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/config"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/mysql/server"
	"github.com/IceFireDB/IceFireDB-SQLite/utils"
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
		logrus.Errorf("github.com/IceFireDB/IceFireDB-SQLite监听端口错误：%v", err)
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

	logrus.Infof("启动IceFireDB-SQLite，监听地址：%s\n", config.Get().Server.Addr)

	// init sqlitedb
	db := sqlite.InitSQLite(ctx, config.Get().SQLite.Filename)
	m.db = db
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
		p.credential.AddUser(info.User, info.Password)
	}
	return p
}
