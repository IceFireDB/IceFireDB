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
		// don't announce fanout-only topics
		if topic := p.myTopics[t]; topic != nil && topic.fanoutOnly {
			continue
		}
		subscriptions[t] = true
	}

	for t := range p.myRelays {
		subscriptions[t] = true
	}

	for t := range subscriptions {
		var requestPartial, supportsPartialMessages bool
		if ts, ok := p.myTopics[t]; ok {
			requestPartial = ts.requestPartialMessages
			supportsPartialMessages = ts.supportsPartialMessages
		}
		as := &pb.RPC_SubOpts{
			Topicid:                proto.String(t),
			Subscribe:              proto.Bool(true),
			RequestsPartial:        &requestPartial,
			SupportsSendingPartial: &supportsPartialMessages,
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as)
	}
	return &rpc
}

func (p *PubSub) handleNewStream(s network.Stream) {
	peer := s.Conn().RemotePeer()
	done := make(chan struct{})
	sentNewStream := false

	defer func() {
		p.inboundStreamsMx.Lock()
		if p.inboundStreams[peer].s == s {
			delete(p.inboundStreams, peer)
		}
		p.inboundStreamsMx.Unlock()

		if sentNewStream {
			select {
			case p.incoming <- incomingUnion{kind: incomingKindClosedStream, s: s}:
			case <-p.ctx.Done():
			}
		}

		close(done)
	}()

	p.inboundStreamsMx.Lock()
	prev, hasPrev := p.inboundStreams[peer]
	p.inboundStreams[peer] = inboundHandler{s: s, done: done}
	p.inboundStreamsMx.Unlock()

	if hasPrev {
		p.logger.Debug("duplicate inbound stream; replacing handler", "peer", peer)
		prev.s.Reset()
		select {
		case <-prev.done:
		case <-p.ctx.Done():
			return
		}
	}

	select {
	case p.incoming <- incomingUnion{kind: incomingKindNewStream, s: s}:
		sentNewStream = true
	case <-p.ctx.Done():
		// Close is useless because the other side isn't reading.
		s.Reset()
		return
	}

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
		case p.incoming <- incomingUnion{
			kind: incomingKindRPC,
			rpc:  rpc,
		}:
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
	s, err := p.host.NewStream(ctx, pid, p.rt.Protocols()...)
	if err != nil {
		p.logger.Debug("error opening new stream to peer", "err", err, "peer", pid)

		select {
		case p.newPeerError <- pid:
		case <-ctx.Done():
		}

		return
	}

	firstMessage := make(chan *RPC, 1)
	sCtx, cancel := context.WithCancel(ctx)
	go p.handleSendingMessages(sCtx, s, outgoing, firstMessage)
	go p.handlePeerDead(s)
	select {
	case p.newPeerStream <- peerOutgoingStream{Stream: s, FirstMessage: firstMessage, Cancel: cancel}:
	case <-ctx.Done():
		cancel()
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

func (p *PubSub) handleSendingMessages(ctx context.Context, s network.Stream, outgoing *rpcQueue, firstMessage chan *RPC) {
	writeRpc := func(rpc *RPC) error {
		size := uint64(rpc.Size())

		buf := pool.Get(varint.UvarintSize(size) + int(size))
		defer pool.Put(buf)

		n := binary.PutUvarint(buf, size)
		_, err := rpc.MarshalTo(buf[n:])
		if err != nil {
			return err
		}

		if err := s.SetWriteDeadline(time.Now().Add(time.Second * 30)); err != nil {
			p.rpcLogger.Debug("failed to set write deadline", "peer", s.Conn().RemotePeer(), "err", err)
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

	select {
	case rpc := <-firstMessage:
		if rpc.Size() > 0 {
			err := writeRpc(rpc)
			if err != nil {
				s.Reset()
				p.logger.Debug("error writing message to peer", "peer", s.Conn().RemotePeer(), "err", err)
				return
			}
		}
	case <-ctx.Done():
		s.Reset()
		return
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
