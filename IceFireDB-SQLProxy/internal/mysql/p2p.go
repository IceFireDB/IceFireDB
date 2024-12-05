package mysql

import (
	"strings"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/p2p"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/utils"
	"github.com/sirupsen/logrus"
)

var (
	p2pHost   *p2p.P2P
	p2pPubSub *p2p.PubSub
)

func initP2P(m *mysqlProxy) {
	// create p2p element
	p2pHost = p2p.NewP2P(config.Get().P2P.ServiceDiscoveryID, config.Get().P2P.NodeHostIP, config.Get().P2P.NodeHostPort) // create p2p
	logrus.Info("Completed P2P Setup")
	// Connect to peers with the chosen discovery method
	switch strings.ToLower(config.Get().P2P.ServiceDiscoverMode) {
	case "announce":
		p2pHost.AnnounceConnect() // KadDHT p2p net create
	case "advertise":
		p2pHost.AdvertiseConnect()
	default:
		p2pHost.AdvertiseConnect()
	}

	logrus.Info("Connected to P2P Service Peers")
	var err error
	p2pPubSub, err = p2p.JoinPubSub(p2pHost, "mysql-client", config.Get().P2P.ServiceCommandTopic)
	if err != nil {
		panic(err)
	}
	logrus.Infof("Successfully joined [%s] P2P channel. \n", config.Get().P2P.ServiceCommandTopic)
	asyncSQL(m)
}

func asyncSQL(m *mysqlProxy) {
	utils.GoWithRecover(func() {
		txConn := make(map[string]*client.Conn)

		for {
			select {
			case <-m.ctx.Done():
				p2pPubSub.Exit()
				_ = p2pHost.Host.Close()
				_ = p2pHost.KadDHT.Close()
				return
			case s := <-p2pPubSub.Inbound:
				var err error
				conn, ok := txConn[s.SenderID]
				if !ok {
					conn, err = m.popMysqlConn()
					if err != nil {
						logrus.Errorf("asyncSQL get conn err: %v", err)
						continue
					}
				}
				_, err = conn.Execute(s.Message)
				if err != nil {
					logrus.Infof("Inbound sql: %s err: %v", s, err)
					continue
				}
				logrus.Infof("Inbound id: %s, sql:  %s", s.SenderID, s.Message)
				if !ok {
					if conn.IsInTransaction() {
						txConn[s.SenderID] = conn
					} else {
						m.pushMysqlConn(conn, err)
					}
				} else if ok && !conn.IsInTransaction() {
					delete(txConn, s.SenderID)
				}
			}
		}
	}, func(r interface{}) {
		time.Sleep(time.Second)
		asyncSQL(m)
	})
}

var DMLSQL = []string{
	"BEGIN",
	"BEGIN TRANSACTION",
	"COMMIT",
	"END TRANSACTION",
	"ROLLBACK",
	"INSERT",
	"UPDATE",
	"DELETE",
	"CREATE",
	"ALERT",
	"DROP",
}

func isDML(sql string) bool {
	prefix := sql
	if len(sql) > 20 {
		prefix = sql[:20]
	}
	prefix = strings.ToUpper(prefix)
	for _, v := range DMLSQL {
		if strings.HasPrefix(prefix, v) {
			return true
		}
	}
	return false
}

func broadcast(sql string) {
	if !isDML(sql) {
		return
	}
	p2pPubSub.Outbound <- sql
	logrus.Infof("Outbound sql: %s", sql)
}
