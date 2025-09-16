package pubsub

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-varint"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// get the initial RPC containing all of our subscriptions to send to new peers
func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC

	subscriptions := make(map[string]bool)

	for t := range p.mySubs {
		subscriptions[t] = true
	}

	for t := range p.myRelays {
		subscriptions[t] = true
	}

	for t := range subscriptions {
		as := &pb.RPC_SubOpts{
			Topicid:   proto.String(t),
			Subscribe: proto.Bool(true),
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as)
	}
	return &rpc
}

func (p *PubSub) handleNewStream(s network.Stream) {
	peer := s.Conn().RemotePeer()

	p.inboundStreamsMx.Lock()
	other, dup := p.inboundStreams[peer]
	if dup {
		p.logger.Debug("duplicate inbound stream from; resetting other stream", "peer", peer)
		other.Reset()
	}
	p.inboundStreams[peer] = s
	p.inboundStreamsMx.Unlock()

	defer func() {
		p.inboundStreamsMx.Lock()
		if p.inboundStreams[peer] == s {
			delete(p.inboundStreams, peer)
		}
		p.inboundStreamsMx.Unlock()
	}()

	r := msgio.NewVarintReaderSize(s, p.maxMessageSize)
	for {
		// Peek at the message length to know when we should mark the start time
		// for measuring how long it took to receive a message.
		_, _ = r.NextMsgLen()
		start := time.Now()
		msgbytes, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(msgbytes)
			if err != io.EOF {
				s.Reset()
				p.rpcLogger.Debug("error reading rpc", "from", s.Conn().RemotePeer(), "err", err)
			} else {
				// Just be nice. They probably won't read this
				// but it doesn't hurt to send it.
				s.Close()
			}

			return
		}
		if len(msgbytes) == 0 {
			continue
		}

		rpc := new(RPC)
		err = rpc.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			s.Reset()

			p.rpcLogger.Warn("bogus rpc from", "peer", s.Conn().RemotePeer(), "err", err)
			return
		}

		timeToReceive := time.Since(start)
		p.rpcLogger.Debug("received", "peer", s.Conn().RemotePeer(), "duration_s", timeToReceive.Seconds(), "rpc", rpc)

		rpc.from = peer
		select {
		case p.incoming <- rpc:
		case <-p.ctx.Done():
			// Close is useless because the other side isn't reading.
			s.Reset()
			return
		}
	}
}

func (p *PubSub) notifyPeerDead(pid peer.ID) {
	p.peerDeadPrioLk.RLock()
	p.peerDeadMx.Lock()
	p.peerDeadPend[pid] = struct{}{}
	p.peerDeadMx.Unlock()
	p.peerDeadPrioLk.RUnlock()

	select {
	case p.peerDead <- struct{}{}:
	default:
	}
}

func (p *PubSub) handleNewPeer(ctx context.Context, pid peer.ID, outgoing *rpcQueue) {
	s, err := p.host.NewStream(p.ctx, pid, p.rt.Protocols()...)
	if err != nil {
		p.logger.Debug("error opening new stream to peer", "err", err, "peer", pid)

		select {
		case p.newPeerError <- pid:
		case <-ctx.Done():
		}

		return
	}

	go p.handleSendingMessages(ctx, s, outgoing)
	go p.handlePeerDead(s)
	select {
	case p.newPeerStream <- s:
	case <-ctx.Done():
	}
}

func (p *PubSub) handleNewPeerWithBackoff(ctx context.Context, pid peer.ID, backoff time.Duration, outgoing *rpcQueue) {
	select {
	case <-time.After(backoff):
		p.handleNewPeer(ctx, pid, outgoing)
	case <-ctx.Done():
		return
	}
}

func (p *PubSub) handlePeerDead(s network.Stream) {
	pid := s.Conn().RemotePeer()

	_, err := s.Read([]byte{0})
	if err == nil {
		p.logger.Debug("unexpected message from peer", "peer", pid)
	}

	s.Reset()
	p.notifyPeerDead(pid)
}

func (p *PubSub) handleSendingMessages(ctx context.Context, s network.Stream, outgoing *rpcQueue) {
	writeRpc := func(rpc *RPC) error {
		size := uint64(rpc.Size())

		buf := pool.Get(varint.UvarintSize(size) + int(size))
		defer pool.Put(buf)

		n := binary.PutUvarint(buf, size)
		_, err := rpc.MarshalTo(buf[n:])
		if err != nil {
			return err
		}

		_, err = s.Write(buf)
		if err != nil {
			p.rpcLogger.Debug("failed to send message", "peer", s.Conn().RemotePeer(), "rpc", rpc, "err", err)
			return err
		}
		p.rpcLogger.Debug("sent", "peer", s.Conn().RemotePeer(), "rpc", rpc)
		return nil
	}

	defer s.Close()
	for ctx.Err() == nil {
		rpc, err := outgoing.Pop(ctx)
		if err != nil {
			p.logger.Debug("error popping message from the queue to send to peer", "peer", s.Conn().RemotePeer(), "err", err)
			return
		}

		err = writeRpc(rpc)
		if err != nil {
			s.Reset()
			p.logger.Debug("error writing message to peer", "peer", s.Conn().RemotePeer(), "err", err)
			return
		}
	}
}

func rpcWithSubs(subs ...*pb.RPC_SubOpts) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Subscriptions: subs,
		},
	}
}

func rpcWithMessages(msgs ...*pb.Message) *RPC {
	return &RPC{RPC: pb.RPC{Publish: msgs}}
}

func rpcWithControl(msgs []*pb.Message,
	ihave []*pb.ControlIHave,
	iwant []*pb.ControlIWant,
	graft []*pb.ControlGraft,
	prune []*pb.ControlPrune,
	idontwant []*pb.ControlIDontWant) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Publish: msgs,
			Control: &pb.ControlMessage{
				Ihave:     ihave,
				Iwant:     iwant,
				Graft:     graft,
				Prune:     prune,
				Idontwant: idontwant,
			},
		},
	}
}

func copyRPC(rpc *RPC) *RPC {
	res := new(RPC)
	*res = *rpc
	if rpc.Control != nil {
		res.Control = new(pb.ControlMessage)
		*res.Control = *rpc.Control
	}
	return res
}
