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
	
	if err = ms.initClientPool(); err != nil {
		return fmt.Errorf("initClientPool error: %v", err)
	}
	ln, err := net.Listen("tcp4", config.Get().Server.Addr)
	if err != nil {
		logrus.Errorf("mysql%v", err)
		return
	}
	utils.GoWithRecover(func() {
		if <-ctx.Done(); true {
			_ = ln.Close()
			ms.closed.Store(true)
		}
	}, nil)
	
	ms.closed.Store(false)
	logrus.Infof("%s\n", config.Get().Server.Addr)
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

，
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
