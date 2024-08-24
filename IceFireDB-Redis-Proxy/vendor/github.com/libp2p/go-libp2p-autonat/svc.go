package autonat

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	pb "github.com/libp2p/go-libp2p-autonat/pb"

	"github.com/libp2p/go-msgio/protoio"
	ma "github.com/multiformats/go-multiaddr"
)

var streamReadTimeout = 60 * time.Second

// AutoNATService provides NAT autodetection services to other peers
type autoNATService struct {
	ctx          context.Context
	instance     context.CancelFunc
	instanceLock sync.Mutex

	config *config

	// rate limiter
	running    uint32
	mx         sync.Mutex
	reqs       map[peer.ID]int
	globalReqs int
}

// NewAutoNATService creates a new AutoNATService instance attached to a host
func newAutoNATService(ctx context.Context, c *config) (*autoNATService, error) {
	if c.dialer == nil {
		return nil, errors.New("Cannot create NAT service without a network")
	}

	as := &autoNATService{
		ctx:    ctx,
		config: c,
		reqs:   make(map[peer.ID]int),
	}

	return as, nil
}

func (as *autoNATService) handleStream(s network.Stream) {
	s.SetReadDeadline(time.Now().Add(streamReadTimeout))

	defer s.Close()

	pid := s.Conn().RemotePeer()
	log.Debugf("New stream from %s", pid.Pretty())

	r := protoio.NewDelimitedReader(s, network.MessageSizeMax)
	w := protoio.NewDelimitedWriter(s)

	var req pb.Message
	var res pb.Message

	err := r.ReadMsg(&req)
	if err != nil {
		log.Debugf("Error reading message from %s: %s", pid.Pretty(), err.Error())
		s.Reset()
		return
	}

	t := req.GetType()
	if t != pb.Message_DIAL {
		log.Debugf("Unexpected message from %s: %s (%d)", pid.Pretty(), t.String(), t)
		s.Reset()
		return
	}

	dr := as.handleDial(pid, s.Conn().RemoteMultiaddr(), req.GetDial().GetPeer())
	res.Type = pb.Message_DIAL_RESPONSE.Enum()
	res.DialResponse = dr

	err = w.WriteMsg(&res)
	if err != nil {
		log.Debugf("Error writing response to %s: %s", pid.Pretty(), err.Error())
		s.Reset()
		return
	}
}

func (as *autoNATService) handleDial(p peer.ID, obsaddr ma.Multiaddr, mpi *pb.Message_PeerInfo) *pb.Message_DialResponse {
	if mpi == nil {
		return newDialResponseError(pb.Message_E_BAD_REQUEST, "missing peer info")
	}

	mpid := mpi.GetId()
	if mpid != nil {
		mp, err := peer.IDFromBytes(mpid)
		if err != nil {
			return newDialResponseError(pb.Message_E_BAD_REQUEST, "bad peer id")
		}

		if mp != p {
			return newDialResponseError(pb.Message_E_BAD_REQUEST, "peer id mismatch")
		}
	}

	addrs := make([]ma.Multiaddr, 0, as.config.maxPeerAddresses)
	seen := make(map[string]struct{})

	// Don't even try to dial peers with blocked remote addresses. In order to dial a peer, we
	// need to know their public IP address, and it needs to be different from our public IP
	// address.
	if as.config.dialPolicy.skipDial(obsaddr) {
		return newDialResponseError(pb.Message_E_DIAL_ERROR, "refusing to dial peer with blocked observed address")
	}

	// Determine the peer's IP address.
	hostIP, _ := ma.SplitFirst(obsaddr)
	switch hostIP.Protocol().Code {
	case ma.P_IP4, ma.P_IP6:
	default:
		// This shouldn't be possible as we should skip all addresses that don't include
		// public IP addresses.
		return newDialResponseError(pb.Message_E_INTERNAL_ERROR, "expected an IP address")
	}

	// add observed addr to the list of addresses to dial
	addrs = append(addrs, obsaddr)
	seen[obsaddr.String()] = struct{}{}

	for _, maddr := range mpi.GetAddrs() {
		addr, err := ma.NewMultiaddrBytes(maddr)
		if err != nil {
			log.Debugf("Error parsing multiaddr: %s", err.Error())
			continue
		}

		// For security reasons, we _only_ dial the observed IP address.
		// Replace other IP addresses with the observed one so we can still try the
		// requested ports/transports.
		if ip, rest := ma.SplitFirst(addr); !ip.Equal(hostIP) {
			// Make sure it's an IP address
			switch ip.Protocol().Code {
			case ma.P_IP4, ma.P_IP6:
			default:
				continue
			}
			addr = hostIP
			if rest != nil {
				addr = addr.Encapsulate(rest)
			}
		}

		// Make sure we're willing to dial the rest of the address (e.g., not a circuit
		// address).
		if as.config.dialPolicy.skipDial(addr) {
			continue
		}

		str := addr.String()
		_, ok := seen[str]
		if ok {
			continue
		}

		addrs = append(addrs, addr)
		seen[str] = struct{}{}

		if len(addrs) >= as.config.maxPeerAddresses {
			break
		}
	}

	if len(addrs) == 0 {
		return newDialResponseError(pb.Message_E_DIAL_ERROR, "no dialable addresses")
	}

	return as.doDial(peer.AddrInfo{ID: p, Addrs: addrs})
}

func (as *autoNATService) doDial(pi peer.AddrInfo) *pb.Message_DialResponse {
	// rate limit check
	as.mx.Lock()
	count := as.reqs[pi.ID]
	if count >= as.config.throttlePeerMax || (as.config.throttleGlobalMax > 0 &&
		as.globalReqs >= as.config.throttleGlobalMax) {
		as.mx.Unlock()
		return newDialResponseError(pb.Message_E_DIAL_REFUSED, "too many dials")
	}
	as.reqs[pi.ID] = count + 1
	as.globalReqs++
	as.mx.Unlock()

	ctx, cancel := context.WithTimeout(as.ctx, as.config.dialTimeout)
	defer cancel()

	as.config.dialer.Peerstore().ClearAddrs(pi.ID)

	as.config.dialer.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.TempAddrTTL)
	conn, err := as.config.dialer.DialPeer(ctx, pi.ID)
	if err != nil {
		log.Debugf("error dialing %s: %s", pi.ID.Pretty(), err.Error())
		// wait for the context to timeout to avoid leaking timing information
		// this renders the service ineffective as a port scanner
		<-ctx.Done()
		return newDialResponseError(pb.Message_E_DIAL_ERROR, "dial failed")
	}

	ra := conn.RemoteMultiaddr()
	as.config.dialer.ClosePeer(pi.ID)
	return newDialResponseOK(ra)
}

// Enable the autoNAT service if it is not running.
func (as *autoNATService) Enable() {
	as.instanceLock.Lock()
	defer as.instanceLock.Unlock()
	if as.instance != nil {
		return
	}
	inst, cncl := context.WithCancel(as.ctx)
	as.instance = cncl

	go as.background(inst)
}

// Disable the autoNAT service if it is running.
func (as *autoNATService) Disable() {
	as.instanceLock.Lock()
	defer as.instanceLock.Unlock()
	if as.instance != nil {
		as.instance()
		as.instance = nil
	}
}

func (as *autoNATService) background(ctx context.Context) {
	as.config.host.SetStreamHandler(AutoNATProto, as.handleStream)

	timer := time.NewTimer(as.config.throttleResetPeriod)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			as.mx.Lock()
			as.reqs = make(map[peer.ID]int)
			as.globalReqs = 0
			as.mx.Unlock()
			jitter := rand.Float32() * float32(as.config.throttleResetJitter)
			timer.Reset(as.config.throttleResetPeriod + time.Duration(int64(jitter)))
		case <-ctx.Done():
			as.config.host.RemoveStreamHandler(AutoNATProto)
			return
		}
	}
}
