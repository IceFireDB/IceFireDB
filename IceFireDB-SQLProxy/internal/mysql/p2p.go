package mysql

import (
	"fmt"
	"strings"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/p2p"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/utils"
	"github.com/sirupsen/logrus"
)

type p2pChannels struct {
	adminHost      *p2p.P2P
	adminPubSub    *p2p.PubSub
	readonlyHost   *p2p.P2P
	readonlyPubSub *p2p.PubSub
	// authToken signs published messages; empty means no signing.
	authToken string
}

var (
	p2pChans              *p2pChannels
	adminInboundPolicy    *inboundPolicy
	readonlyInboundPolicy *inboundPolicy
)

func initP2P(m *mysqlProxy) {
	p2pChans = &p2pChannels{}
	cfg := config.Get().P2P
	p2pChans.authToken = cfg.AuthToken

	// One de-duplication cache shared by both topics: a message ID is globally
	// unique, and a duplicate is a duplicate whichever topic it arrives on.
	dedup := newDedupCache(4096)
	adminInboundPolicy = newInboundPolicy(roleAdmin, cfg.AuthToken, cfg.AllowedPeers, dedup)
	readonlyInboundPolicy = newInboundPolicy(roleReadonly, cfg.AuthToken, cfg.AllowedPeers, dedup)

	if cfg.AuthToken == "" {
		logrus.Warn("p2p.authToken is not configured: any peer that joins the topic can execute writes on this node")
	}
	if len(cfg.AllowedPeers) == 0 {
		logrus.Warn("p2p.allowedPeers is not configured: every peer that joins the topic is accepted")
	}

	// Configured peers mean a private network: discovery through the public DHT is
	// skipped, which keeps private clusters off the public network and makes startup
	// deterministic.
	opts := p2p.Options{
		Bootstrap: len(cfg.BootstrapPeers) == 0,
		Peers:     cfg.BootstrapPeers,
	}

	// Initialize admin P2P
	p2pChans.adminHost = p2p.NewP2P(cfg.ServiceDiscoveryID,
		cfg.NodeHostIP, cfg.NodeHostPort, opts)

	// Initialize readonly P2P
	p2pChans.readonlyHost = p2p.NewP2P(cfg.ServiceDiscoveryID,
		cfg.NodeHostIP, cfg.NodeHostPort, opts)

	// Connect both to their networks
	if opts.Bootstrap {
		connectP2PNetwork(p2pChans.adminHost, cfg.ServiceDiscoverMode)
		connectP2PNetwork(p2pChans.readonlyHost, cfg.ServiceDiscoverMode)
	} else {
		logrus.Infof("p2p discovery: %d configured peer(s), public DHT disabled", len(cfg.BootstrapPeers))
	}
	logrus.Infof("p2p peer id %s, addrs %v", p2pChans.adminHost.PeerID(), p2pChans.adminHost.Addrs())

	// Join pubsub channels
	var err error
	p2pChans.adminPubSub, err = p2p.JoinPubSub(p2pChans.adminHost, "mysql-admin",
		config.Get().P2P.AdminTopic)
	if err != nil {
		panic(err)
	}

	p2pChans.readonlyPubSub, err = p2p.JoinPubSub(p2pChans.readonlyHost, "mysql-readonly",
		config.Get().P2P.ReadonlyTopic)
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
		adminTxConn := newTxConns()
		readonlyTxConn := newTxConns()
		reaper := time.NewTicker(time.Minute)
		defer reaper.Stop()

		for {
			select {
			case <-reaper.C:
				adminTxConn.reap(txnIdleTimeout)
				readonlyTxConn.reap(txnIdleTimeout)
				continue
			case <-m.ctx.Done():
				adminTxConn.closeAll()
				readonlyTxConn.closeAll()
				p2pChans.adminPubSub.Exit()
				p2pChans.readonlyPubSub.Exit()
				_ = p2pChans.adminHost.Host.Close()
				_ = p2pChans.adminHost.KadDHT.Close()
				_ = p2pChans.readonlyHost.Host.Close()
				_ = p2pChans.readonlyHost.KadDHT.Close()
				return

			// Handle admin channel messages
			case msg, ok := <-p2pChans.adminPubSub.Inbound:
				if !ok {
					logrus.Error("admin replication channel closed")
					return
				}
				handleInbound(m, msg, adminTxConn, adminInboundPolicy)

			// Handle readonly channel messages
			case msg, ok := <-p2pChans.readonlyPubSub.Inbound:
				if !ok {
					logrus.Error("readonly replication channel closed")
					return
				}
				handleInbound(m, msg, readonlyTxConn, readonlyInboundPolicy)
			}
		}
	}, func(r any) {
		time.Sleep(time.Second)
		asyncSQL(m)
	})
}

// txnIdleTimeout is how long a peer may leave a transaction open on its bound
// connection before the proxy reclaims it. Without this, a peer that never sends COMMIT
// keeps a MySQL connection (and any locks it holds) for the lifetime of the process.
const txnIdleTimeout = 5 * time.Minute

// senderConn remembers the connection bound to a peer that opened a transaction.
type senderConn struct {
	conn *client.Conn
	last time.Time
}

// txConns maps a peer ID to the connection holding its open transaction.
type txConns map[string]*senderConn

func newTxConns() txConns { return make(txConns) }

// bind records that the peer's transaction continues on conn.
func (t txConns) bind(sender string, conn *client.Conn) {
	t[sender] = &senderConn{conn: conn, last: time.Now()}
}

// release forgets the peer and hands its connection back to the pool.
func (t txConns) release(m *mysqlProxy, sender string, role string, err error) {
	entry, ok := t[sender]
	if !ok {
		return
	}
	delete(t, sender)
	pushConnForRole(m, entry.conn, err, role)
}

// reap closes transactions that have been idle for longer than timeout.
func (t txConns) reap(timeout time.Duration) {
	now := time.Now()
	for sender, entry := range t {
		if now.Sub(entry.last) < timeout {
			continue
		}
		logrus.Warnf("reclaiming idle transaction from peer %s (idle %s)", sender, now.Sub(entry.last))
		_ = entry.conn.Rollback()
		_ = entry.conn.Close()
		delete(t, sender)
	}
}

// closeAll drops every bound connection on shutdown.
func (t txConns) closeAll() {
	for sender, entry := range t {
		_ = entry.conn.Rollback()
		_ = entry.conn.Close()
		delete(t, sender)
	}
}

// publishMessage hands an envelope to the admin topic.
func publishMessage(msg *replicationMessage) {
	chans := p2pChans
	if chans == nil || chans.adminPubSub == nil {
		return
	}
	if msg.Role != roleAdmin {
		// Read-only sessions cannot write; nothing to replicate.
		recordRejected(rejectReplicationRole)
		return
	}
	// Authenticate before the message leaves this node; a peer verifies it before
	// executing anything.
	msg.sign(chans.authToken)

	payload, err := msg.encode()
	if err != nil {
		recordPublishFailed()
		logrus.Errorf("encode replication message %s: %v", msg.ID, err)
		return
	}
	if !chans.adminPubSub.Publish(payload) {
		// The queue is full: dropping is better than stalling the client that is
		// waiting for its write, and the counter makes the loss visible.
		recordPublishDropped()
		logrus.Errorf("replication queue full, dropped message %s with %d statement(s)",
			msg.ID, len(msg.Statements))
		return
	}
	recordPublished(msg.Kind)
}

// handleInbound validates a message against the topic policy and applies it.
func handleInbound(m *mysqlProxy, raw *p2p.Message, txConn txConns, policy *inboundPolicy) {
	sender := raw.GetFrom()
	msg, reason, err := policy.accept(sender, raw.GetData())
	if err != nil {
		recordRejected(reason)
		logrus.Warnf("inbound replication message from %s rejected (%s): %v", sender, reason, err)
		return
	}

	if err := applyMessage(m, sender, msg, txConn, policy.role); err != nil {
		recordApplyFailed()
		logrus.Errorf("applying %s message %s from %s: %v", msg.Kind, msg.ID, sender, err)
		return
	}
	recordApplied(msg.Kind)
	logrus.Debugf("applied %s message %s from %s (%d statement(s))", msg.Kind, msg.ID, sender, len(msg.Statements))
}

// applyMessage replays a replicated message on the local MySQL.
func applyMessage(m *mysqlProxy, sender string, msg *replicationMessage, txConn txConns, role string) error {
	switch msg.Kind {
	case messageKindSQL:
		for _, statement := range msg.Statements {
			if err := applyStatement(m, sender, statement, txConn, role); err != nil {
				return err
			}
		}
		return nil
	case messageKindTxn:
		return applyTransaction(m, msg.Statements, role)
	default:
		return fmt.Errorf("unknown replication message kind %q", msg.Kind)
	}
}

// applyStatement runs one autocommit statement, keeping the connection bound to the
// sender while a transaction it opened stays open.
func applyStatement(m *mysqlProxy, sender, statement string, txConn txConns, role string) error {
	var conn *client.Conn
	entry, bound := txConn[sender]
	if bound {
		conn = entry.conn
	} else {
		var err error
		conn, err = m.popConnForRole(role)
		if err != nil {
			return err
		}
	}

	// handBack returns the connection either way: a bound one is forgotten first, so
	// a failed statement cannot leave a stale entry behind.
	handBack := func(err error) {
		if bound {
			txConn.release(m, sender, role, err)
			return
		}
		pushConnForRole(m, conn, err, role)
	}

	if _, err := conn.Execute(statement); err != nil {
		handBack(err)
		return err
	}

	if conn.IsInTransaction() {
		txConn.bind(sender, conn)
		return nil
	}

	handBack(nil)
	return nil
}

// applyTransaction replays a committed transaction atomically: either every statement
// lands on the peer or none does.
func applyTransaction(m *mysqlProxy, statements []string, role string) error {
	conn, err := m.popConnForRole(role)
	if err != nil {
		return err
	}

	if err := conn.Begin(); err != nil {
		pushConnForRole(m, conn, err, role)
		return err
	}

	for _, statement := range statements {
		if _, err := conn.Execute(statement); err != nil {
			_ = conn.Rollback()
			pushConnForRole(m, conn, err, role)
			return err
		}
	}

	if err := conn.Commit(); err != nil {
		_ = conn.Rollback()
		pushConnForRole(m, conn, err, role)
		return err
	}

	pushConnForRole(m, conn, nil, role)
	return nil
}

func pushConnForRole(m *mysqlProxy, conn *client.Conn, err error, role string) {
	if role == roleReadonly {
		m.pushReadonlyConn(conn, err)
		return
	}
	m.pushAdminConn(conn, err)
}
