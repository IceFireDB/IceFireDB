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

type p2pChannels struct {
	adminHost    *p2p.P2P
	adminPubSub  *p2p.PubSub
	readonlyHost *p2p.P2P
	readonlyPubSub *p2p.PubSub
}

var (
	p2pChans *p2pChannels
)

func initP2P(m *mysqlProxy) {
	p2pChans = &p2pChannels{}
	
	// Initialize admin P2P
	p2pChans.adminHost = p2p.NewP2P(config.Get().P2P.Admin.ServiceDiscoveryID, 
		config.Get().P2P.Admin.NodeHostIP, config.Get().P2P.Admin.NodeHostPort)
	
	// Initialize readonly P2P
	p2pChans.readonlyHost = p2p.NewP2P(config.Get().P2P.Readonly.ServiceDiscoveryID,
		config.Get().P2P.Readonly.NodeHostIP, config.Get().P2P.Readonly.NodeHostPort)

	// Connect both to their networks
	connectP2PNetwork(p2pChans.adminHost, config.Get().P2P.Admin.ServiceDiscoverMode)
	connectP2PNetwork(p2pChans.readonlyHost, config.Get().P2P.Readonly.ServiceDiscoverMode)

	// Join pubsub channels
	var err error
	p2pChans.adminPubSub, err = p2p.JoinPubSub(p2pChans.adminHost, "mysql-admin", 
		config.Get().P2P.Admin.ServiceCommandTopic)
	if err != nil {
		panic(err)
	}

	p2pChans.readonlyPubSub, err = p2p.JoinPubSub(p2pChans.readonlyHost, "mysql-readonly",
		config.Get().P2P.Readonly.ServiceCommandTopic)
	if err != nil {
		panic(err)
	}

	logrus.Info("Successfully initialized both admin and readonly P2P channels")
	asyncSQL(m)
}

func connectP2PNetwork(host *p2p.P2P, mode string) {
	switch strings.ToLower(mode) {
	case "announce":
		host.AnnounceConnect()
	case "advertise":
		host.AdvertiseConnect()
	default:
		host.AdvertiseConnect()
	}
	logrus.Info("Connected to P2P network")
}

func asyncSQL(m *mysqlProxy) {
	utils.GoWithRecover(func() {
		adminTxConn := make(map[string]*client.Conn)
		readonlyTxConn := make(map[string]*client.Conn)

		for {
			select {
			case <-m.ctx.Done():
				p2pChans.adminPubSub.Exit()
				p2pChans.readonlyPubSub.Exit()
				_ = p2pChans.adminHost.Host.Close()
				_ = p2pChans.adminHost.KadDHT.Close()
				_ = p2pChans.readonlyHost.Host.Close()
				_ = p2pChans.readonlyHost.KadDHT.Close()
				return
				
			// Handle admin channel messages
			case s := <-p2pChans.adminPubSub.Inbound:
				handleInboundSQL(m, s, adminTxConn, "admin")
				
			// Handle readonly channel messages
			case s := <-p2pChans.readonlyPubSub.Inbound:
				handleInboundSQL(m, s, readonlyTxConn, "readonly")
			}
		}
	}, func(r interface{}) {
		time.Sleep(time.Second)
		asyncSQL(m)
	})
}

func handleInboundSQL(m *mysqlProxy, s *p2p.Message, txConn map[string]*client.Conn, accessType string) {
	var err error
	conn, ok := txConn[s.SenderID]
	
	if !ok {
		// Get appropriate connection based on access type
		if accessType == "admin" {
			conn, err = m.popAdminConn()
		} else {
			conn, err = m.popReadonlyConn()
		}
		if err != nil {
			logrus.Errorf("asyncSQL get %s conn err: %v", accessType, err)
			return
		}
	}

	// Execute query
	_, err = conn.Execute(s.Message)
	if err != nil {
		logrus.Infof("Inbound %s sql: %s err: %v", accessType, s.Message, err)
		return
	}
	logrus.Infof("Inbound %s id: %s, sql: %s", accessType, s.SenderID, s.Message)

	if !ok {
		if conn.IsInTransaction() {
			txConn[s.SenderID] = conn
		} else {
			if accessType == "admin" {
				m.pushAdminConn(conn, err)
			} else {
				m.pushReadonlyConn(conn, err)
			}
		}
	} else if ok && !conn.IsInTransaction() {
		delete(txConn, s.SenderID)
	}
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

func broadcast(sql string, accessType string) {
	if !isDML(sql) {
		return
	}
	
	if accessType == "admin" {
		p2pChans.adminPubSub.Outbound <- sql
		logrus.Infof("Outbound admin sql: %s", sql)
	} else {
		// Readonly nodes don't broadcast writes
		logrus.Infof("Readonly node attempted to broadcast write: %s", sql)
	}
}
