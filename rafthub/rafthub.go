// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"bufio"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/snappy"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/tidwall/redlog/v2"
)

type serverExtra struct {
	reachable  bool   // server is reachable
	remoteAddr string // remote tcp address
	advertise  string // advertise address
	lastError  error  // last error, if any
}

type command struct {
	kind byte // 's' system, 'r' read, 'w' write
	fn   func(m Machine, ra *raftWrap, args []string) (interface{}, error)
}

// SendOptions ...
type SendOptions struct {
	Context        interface{}
	From           interface{}
	AllowOpenReads bool
	DenyOpenReads  bool
}

var defSendOpts = &SendOptions{}

// Receiver ...
type Receiver interface {
	Recv() (interface{}, time.Duration, error)
}

// A Message represents a command and is in a format that is consumed by
// an Observer.
type Message struct {
	// Args are the original command arguments.
	Args []string
	// Resp is the command reponse, if not an error.
	Resp interface{}
	// Err is the command error, if not successful.
	Err error
	// Elapsed is the amount of time that the command took to process.
	Elapsed time.Duration
	// Addr is the remote TCP address of the connection that generated
	// this message.
	Addr string
}

// Monitor represents an interface for sending and consuming command
// messages that are processed by a Service.
type Monitor interface {
	// Send a message to observers
	Send(msg Message)
	// NewObjser returns a new Observer containing a channel that will send the
	// messages for every command processed by the service.
	// Stop the observer to release associated resources.
	NewObserver() Observer
}

// Service is a client facing service.
type Service interface {
	// Send a command with args from a client
	Send(args []string, opts *SendOptions) Receiver
	// Auth authorizes a client
	Auth(auth string) error
	// Log is shared logger
	Log() Logger
	// Monitor returns a service monitor for observing client commands.
	Monitor() Monitor
	// Opened
	Opened(addr string) (context interface{}, accept bool)
	// Closed
	Closed(context interface{}, addr string)
}

// The Backend database format used for storing Raft logs and meta data.
type Backend int

const (
	// LevelDB is an on-disk LSM (LSM log-structured merge-tree) database. This
	// format is optimized for fast sequential reads and writes, which is ideal
	// for most Raft implementations. This is the default format used by rafthub.
	LevelDB Backend = iota
	// Bolt is an on-disk single-file b+tree database. This format has been a
	// popular choice for Go-based Raft implementations for years.
	Bolt
)

const transportMarker = "8e35747e37d192d9a819021ba2a02909"

// writeRequestFuture is the basic unity of communication from services to the
// raft log. It's a Future type that is sent through a channel, picked up by a
// background routine that then applies the `args` to the raft log. Upon
// successfully being applied, the `resp` is fill with the response, and
// `wg.Done` is called.
type writeRequestFuture struct {
	args []string
	resp interface{}
	err  error
	elap time.Duration
	wg   sync.WaitGroup
	s    *service
	from interface{}
}

// Recv received the response and time elapsed to process the write. Or, it
// returns an error.
func (r *writeRequestFuture) Recv() (interface{}, time.Duration, error) {
	r.wg.Wait()
	r.s.writeMu.Lock()
	if r.s.write[r.from] == r {
		delete(r.s.write, r.from)
	}
	r.s.writeMu.Unlock()
	return r.resp, r.elap, r.err
}

// Main entrypoint for the cluster node. This must be called once and only
// once, and as the last call in the Go main() function. There are no return
// values as all application operations, logging, and I/O will be forever
// transferred.
func Main(conf Config) {
	confInit(&conf)
	conf.AddService(redisService())

	hclogger, log := logInit(conf)
	tm := remoteTimeInit(conf, log)
	dir, data := dataDirInit(conf, log)
	m := machineInit(conf, dir, data, log)
	tlscfg := tlsInit(conf, log)
	svr, addr := serverInit(conf, tlscfg, log)
	trans := transportInit(conf, tlscfg, svr, hclogger, log)
	lstore, sstore := storeInit(conf, dir, log)
	snaps := snapshotInit(conf, dir, m, hclogger, log)
	ra := raftInit(conf, hclogger, m, lstore, sstore, snaps, trans, log)

	joinClusterIfNeeded(conf, ra, addr, tlscfg, log)
	startUserServices(conf, svr, m, ra, log)

	go runMaintainServers(ra)
	go runWriteApplier(conf, m, ra)
	go runLogLoadedPoller(conf, m, ra, tlscfg, log)
	go runTicker(conf, tm, m, ra, log)

	log.Fatal(svr.serve())
}

// runTicker is a background routine that keeps the raft machine time and
// random seed updated.
func runTicker(conf Config, rt *remoteTime, m *machine, ra *raftWrap,
	log *redlog.Logger,
) {
	rbuf := make([]byte, 4096)
	var rnb []byte
	for {
		start := time.Now()
		ts := rt.Now().UnixNano()
		if len(rnb) == 0 {
			n, err := rand.Read(rbuf[:])
			if err != nil || n != len(rbuf) {
				log.Panic(err)
			}
			rnb = rbuf[:]
		}
		seed := int64(binary.LittleEndian.Uint64(rnb))
		rnb = rnb[8:]
		req := new(writeRequestFuture)
		req.args = []string{
			"tick",
			strconv.FormatInt(ts, 10),
			strconv.FormatInt(seed, 10),
		}
		req.wg.Add(1)
		m.wrC <- req
		req.wg.Wait()
		m.mu.Lock()
		if req.err == nil {
			l := req.resp.(raft.Log)
			m.tickedIndex = l.Index
			m.tickedTerm = l.Term
		} else {
			m.tickedIndex = 0
			m.tickedTerm = 0
		}
		m.tickedSig.Broadcast()
		m.mu.Unlock()
		dur := time.Since(start)
		delay := m.tickDelay - dur
		if delay < 1 {
			delay = 1
		}
		time.Sleep(delay)
	}
}

// runLogLoadedPoller is a background routine that reports on raft log progress
// and also maintains the m.logLoaded atomic boolean for open read systems.
func runLogLoadedPoller(conf Config, m *machine, ra *raftWrap,
	tlscfg *tls.Config, log *redlog.Logger,
) {
	var loaded bool
	var lastPerc string
	lastPrint := time.Now()
	for {
		// load the last index from the cluster leader
		lastIndex, err := getClusterLastIndex(ra, tlscfg, conf.Auth)
		if err != nil {
			if err != errLeaderUnknown {
				log.Warningf("cluster_last_index: %v", err)
			} else {
				// This service is probably a candidate, flip the loaded
				// off to begin printing log progress.
				loaded = false
				atomic.StoreInt32(&m.logLoaded, 0)
			}
			time.Sleep(time.Second)
			continue
		}

		// update machine with the known leader last index and determine
		// the load progress and how many logs are remaining.
		m.mu.Lock()
		m.logRemain = lastIndex - m.appliedIndex
		if lastIndex == 0 {
			m.logPercent = 0
		} else {
			m.logPercent = float64(m.appliedIndex-m.firstIndex) /
				float64(lastIndex-m.firstIndex)
		}
		lpercent := m.logPercent
		remain := m.logRemain
		m.mu.Unlock()

		if !loaded {
			// Print progress status to console log
			perc := fmt.Sprintf("%.1f%%", lpercent*100)
			if remain < 5 {
				log.Printf("logs loaded: ready for commands")
				loaded = true
				atomic.StoreInt32(&m.logLoaded, 1)
			} else if perc != "0.0%" && perc != lastPerc {
				msg := fmt.Sprintf("logs progress: %.1f%%, remaining=%d",
					lpercent*100, remain)
				now := time.Now()
				if now.Sub(lastPrint) > time.Second*5 {
					log.Print(msg)
					lastPrint = now
				} else {
					log.Verb(msg)
				}
			}
			lastPerc = perc
		}
		time.Sleep(time.Second / 5)
	}
}

// runWriteApplier is a background routine that handles all write requests.
// It's job is to apply the request to the Raft log and returns the result to
// writeRequest.
func runWriteApplier(conf Config, m *machine, ra *raftWrap) {
	var maxReqs = 256 // TODO: make configurable
	for {
		// Gather up as many requests (up to 256) into a single list.
		var reqs []*writeRequestFuture
		r := <-m.wrC
		reqs = append(reqs, r)
		var done bool
		for !done {
			select {
			case r := <-m.wrC:
				reqs = append(reqs, r)
				done = len(reqs) == maxReqs
			default:
				done = true
			}
		}
		// Combined multiple requests the data to a single, snappy-encoded,
		// message using the following binary format:
		// (count, cmd...)
		//   - count: uvarint
		//   - cmd: (count, args...)
		//     - count: uvarint
		//     - arg: (count, byte...)
		//       - count: uvarint
		var data []byte
		data = appendUvarint(data, uint64(len(reqs)))
		for _, r := range reqs {
			data = appendUvarint(data, uint64(len(r.args)))
			for _, arg := range r.args {
				data = appendUvarint(data, uint64(len(arg)))
				data = append(data, arg...)
			}
		}
		data = snappy.Encode(nil, data)

		// Apply the data and read back the messages
		resps, err := func() ([]applyResp, error) {
			// THE ONLY APPLY CALL IN THE CODEBASE SO ENJOY IT
			f := ra.Apply(data, 0)
			err := f.Error()
			if err != nil {
				return nil, err
			}
			return f.Response().([]applyResp), nil
		}()
		if err != nil {
			for _, r := range reqs {
				r.err = errRaftConvert(ra, err)
				r.wg.Done()
			}
		} else {
			for i := range reqs {
				reqs[i].resp = resps[i].resp
				reqs[i].elap = resps[i].elap
				reqs[i].err = resps[i].err
				reqs[i].wg.Done()
			}
		}
	}
}

func runMaintainServers(ra *raftWrap) {
	if ra.advertise == "" {
		return
	}
	for {
		f := ra.GetConfiguration()
		if err := f.Error(); err != nil {
			time.Sleep(time.Second)
			continue
		}
		cfg := f.Configuration()
		var wg sync.WaitGroup
		wg.Add(len(cfg.Servers))
		for _, svr := range cfg.Servers {
			go func(addr string) {
				defer wg.Done()
				c, err := net.DialTimeout("tcp", addr, time.Second*5)
				if err == nil {
					defer c.Close()
				}
				ra.mu.Lock()
				defer ra.mu.Unlock()
				if ra.extra == nil {
					ra.extra = make(map[string]serverExtra)
				}
				extra := ra.extra[addr]
				if err != nil {
					extra.reachable = false
					extra.lastError = err
				} else {
					extra.reachable = true
					extra.lastError = nil
					extra.remoteAddr = c.RemoteAddr().String()
					extra.advertise = addr
				}
				ra.extra[addr] = extra
			}(string(svr.Address))
		}
		wg.Wait()
		time.Sleep(time.Second)
	}
}

func startUserServices(conf Config, svr *splitServer, m *machine, ra *raftWrap,
	log *redlog.Logger,
) {
	// rearrange so that services with nil sniffers are last
	var nilServices []serviceEntry
	var services []serviceEntry
	for i := 0; i < len(conf.services); i++ {
		if conf.services[i].sniff == nil {
			nilServices = append(nilServices, conf.services[i])
		} else {
			services = append(services, conf.services[i])
		}
	}
	conf.services = append(services, nilServices...)
	for _, s := range conf.services {
		ln := svr.split(func(rd io.Reader) (n int, ok bool) {
			if s.sniff == nil {
				return 0, true
			}
			return 0, s.sniff(rd)
		})
		go s.serve(newService(m, ra, conf.Auth), ln)
	}
}

// joinClusterIfNeeded attempts to make this server join a Raft cluster. If
// the server already belongs to a cluster or if the server is bootstrapping
// then this operation is ignored.
func joinClusterIfNeeded(conf Config, ra *raftWrap, addr net.Addr,
	tlscfg *tls.Config, log *redlog.Logger,
) {
	// Get the current Raft cluster configuration for determining whether this
	// server needs to bootstrap a new cluster, or join/re-join an existing
	// cluster.
	f := ra.GetConfiguration()
	if err := f.Error(); err != nil {
		log.Fatalf("could not get Raft configuration: %v", err)
	}
	var addrStr string
	if ra.advertise != "" {
		addrStr = conf.Advertise
	} else {
		addrStr = addr.String()
	}
	cfg := f.Configuration()
	servers := cfg.Servers
	if len(servers) == 0 {
		// Empty configuration. Either bootstrap or join an existing cluster.
		if conf.JoinAddr == "" {
			// No '-join' flag provided.
			// Bootstrap new cluster.
			log.Noticef("bootstrapping new cluster")

			var configuration raft.Configuration
			configuration.Servers = []raft.Server{
				raft.Server{
					ID:      raft.ServerID(conf.NodeID),
					Address: raft.ServerAddress(addrStr),
				},
			}
			err := ra.BootstrapCluster(configuration).Error()
			if err != nil && err != raft.ErrCantBootstrap {
				log.Fatalf("bootstrap: %s", err)
			}
		} else {
			// Joining an existing cluster
			joinAddr := conf.JoinAddr
			log.Noticef("joining existing cluster at %v", joinAddr)
			err := func() error {
				for {
					conn, err := RedisDial(joinAddr, conf.Auth, tlscfg)
					if err != nil {
						return err
					}
					defer conn.Close()
					res, err := redis.String(conn.Do("raft", "server", "add",
						conf.NodeID, addrStr))
					if err != nil {
						if strings.HasPrefix(err.Error(), "MOVED ") {
							parts := strings.Split(err.Error(), " ")
							if len(parts) == 3 {
								joinAddr = parts[2]
								time.Sleep(time.Millisecond * 100)
								continue
							}
						}
						return err
					}
					if res != "1" {
						return fmt.Errorf("'1', got '%s'", res)
					}
					return nil
				}
			}()
			if err != nil {
				log.Fatalf("raft server add: %v", err)
			}
		}
	} else {
		if conf.JoinAddr != "" {
			log.Warningf("ignoring join request because server already " +
				"belongs to a cluster")
		}
		if ra.advertise != "" {
			// Check that the address is the same as before
			found := false
			same := true
			before := ra.advertise
			for _, s := range servers {
				if string(s.ID) == conf.NodeID {
					found = true
					if string(s.Address) != ra.advertise {
						same = false
						before = string(s.Address)
						break
					}
				}
			}
			if !found {
				log.Fatalf("advertise address changed but node not found\n")
			} else if !same {
				log.Fatalf("advertise address change from \"%s\" to \"%s\" ",
					before, ra.advertise)
			}
		}
	}
}

func raftInit(conf Config, hclogger hclog.Logger, fsm raft.FSM,
	logStore raft.LogStore, stableStore raft.StableStore,
	snaps raft.SnapshotStore, trans raft.Transport, log *redlog.Logger,
) *raftWrap {
	rconf := raft.DefaultConfig()
	rconf.Logger = hclogger
	rconf.LocalID = raft.ServerID(conf.NodeID)
	ra, err := raft.NewRaft(rconf, fsm, logStore, stableStore, snaps, trans)
	if err != nil {
		log.Fatal(err)
	}
	return &raftWrap{
		Raft:      ra,
		conf:      conf,
		advertise: conf.Advertise,
	}
}

func transportInit(conf Config, tlscfg *tls.Config, svr *splitServer,
	hclogger hclog.Logger, log *redlog.Logger,
) raft.Transport {
	ln := svr.split(func(r io.Reader) (n int, ok bool) {
		rd := bufio.NewReader(r)
		for i := 0; i < len(transportMarker); i++ {
			b, err := rd.ReadByte()
			if err != nil || b != transportMarker[i] {
				return 0, false
			}
		}
		for i := 0; i < len(conf.Auth); i++ {
			b, err := rd.ReadByte()
			if err != nil || b != conf.Auth[i] {
				return 0, false
			}
		}
		return len(transportMarker) + len(conf.Auth), true
	})
	stream := new(transportStream)
	stream.Listener = ln
	stream.auth = conf.Auth
	stream.tlscfg = tlscfg
	return raft.NewNetworkTransport(stream, conf.MaxPool, 0, log)
}

func serverInit(conf Config, tlscfg *tls.Config, log *redlog.Logger,
) (*splitServer, net.Addr) {
	var ln net.Listener
	var err error
	if tlscfg != nil {
		ln, err = tls.Listen("tcp", conf.Addr, tlscfg)
	} else {
		ln, err = net.Listen("tcp", conf.Addr)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("server listening at %s", ln.Addr())
	if conf.Advertise != "" {
		log.Printf("server advertising as %s", conf.Advertise)
	}
	if conf.ServerReady != nil {
		conf.ServerReady(ln.Addr().String(), conf.Auth, tlscfg)
	}
	return newSplitServer(ln, log), ln.Addr()
}
