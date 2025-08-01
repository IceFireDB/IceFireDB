package dht

import (
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/libp2p/go-libp2p-kad-dht/internal/metrics"
	"github.com/libp2p/go-libp2p-kad-dht/internal/net"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-msgio"
	"go.uber.org/zap"

	"go.opentelemetry.io/otel/attribute"
)

var dhtStreamIdleTimeout = 1 * time.Minute

// ErrReadTimeout is an error that occurs when no message is read within the timeout period.
var ErrReadTimeout = net.ErrReadTimeout

// handleNewStream implements the network.StreamHandler
func (dht *IpfsDHT) handleNewStream(s network.Stream) {
	if dht.handleNewMessage(s) {
		// If we exited without error, close gracefully.
		_ = s.Close()
	} else {
		// otherwise, send an error.
		_ = s.Reset()
	}
}

// Returns true on orderly completion of writes (so we can Close the stream).
func (dht *IpfsDHT) handleNewMessage(s network.Stream) bool {
	ctx := dht.ctx
	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	mPeer := s.Conn().RemotePeer()

	timer := time.AfterFunc(dhtStreamIdleTimeout, func() { _ = s.Reset() })
	defer timer.Stop()

	for {
		if dht.getMode() != modeServer {
			logger.Debugf("ignoring incoming dht message while not in server mode")
			return false
		}

		var req pb.Message
		msgbytes, err := r.ReadMsg()
		msgLen := len(msgbytes)
		if err != nil {
			r.ReleaseMsg(msgbytes)
			if err == io.EOF {
				return true
			}
			// This string test is necessary because there isn't a single stream reset error
			// instance	in use.
			if c := baseLogger.Check(zap.DebugLevel, "error reading message"); c != nil && err.Error() != "stream reset" {
				c.Write(zap.String("from", mPeer.String()),
					zap.Error(err))
			}
			if msgLen > 0 {
				metrics.RecordMessageRecvErr(ctx, "", int64(msgLen))
			}
			return false
		}
		err = proto.Unmarshal(msgbytes, &req)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			if c := baseLogger.Check(zap.DebugLevel, "error unmarshaling message"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Error(err))
			}
			metrics.RecordMessageRecvErr(ctx, "", int64(msgLen))
			return false
		}

		timer.Reset(dhtStreamIdleTimeout)

		startTime := time.Now()

		attrMsgType := attribute.Key(metrics.KeyMessageType).String(req.GetType().String())
		// store a new context to not pollute the parent dht.ctx
		ctx := metrics.ContextWithAttributes(ctx, attrMsgType)

		metrics.RecordMessageRecvOK(ctx, int64(msgLen))

		if dht.onRequestHook != nil {
			dht.onRequestHook(ctx, s, &req)
		}

		handler := dht.handlerForMsgType(req.GetType())
		if handler == nil {
			metrics.RecordMessageHandleErr(ctx)
			if c := baseLogger.Check(zap.DebugLevel, "can't handle received message"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())))
			}
			return false
		}

		if c := baseLogger.Check(zap.DebugLevel, "handling message"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()))
		}
		resp, err := handler(ctx, mPeer, &req)
		if err != nil {
			metrics.RecordMessageHandleErr(ctx)
			if c := baseLogger.Check(zap.DebugLevel, "error handling message"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())),
					zap.Binary("key", req.GetKey()),
					zap.Error(err))
			}
			return false
		}

		if c := baseLogger.Check(zap.DebugLevel, "handled message"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()),
				zap.Duration("time", time.Since(startTime)))
		}

		if resp == nil {
			continue
		}

		// send out response msg
		err = net.WriteMsg(s, resp)
		if err != nil {
			metrics.RecordMessageHandleErr(ctx)
			if c := baseLogger.Check(zap.DebugLevel, "error writing response"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())),
					zap.Binary("key", req.GetKey()),
					zap.Error(err))
			}
			return false
		}

		elapsedTime := time.Since(startTime)

		if c := baseLogger.Check(zap.DebugLevel, "responded to message"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()),
				zap.Duration("time", elapsedTime))
		}

		latencyMillis := float64(elapsedTime) / float64(time.Millisecond)
		metrics.RecordRequestLatency(ctx, latencyMillis)
	}
}
