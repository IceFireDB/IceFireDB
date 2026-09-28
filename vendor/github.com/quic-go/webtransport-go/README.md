# webtransport-go

[![Documentation](https://img.shields.io/badge/docs-quic--go.net-red?style=flat)](https://quic-go.net/docs/)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/quic-go/webtransport-go)](https://pkg.go.dev/github.com/quic-go/webtransport-go)
[![Code Coverage](https://img.shields.io/codecov/c/github/quic-go/webtransport-go/master.svg?style=flat-square)](https://codecov.io/gh/quic-go/webtransport-go/)

webtransport-go is an implementation of the WebTransport protocol, based on [quic-go](https://github.com/quic-go/quic-go). It currently implements [draft-16](https://www.ietf.org/archive/id/draft-ietf-webtrans-http3-16.html) of the specification.

Detailed documentation can be found on [quic-go.net](https://quic-go.net/docs/).

## Example

A runnable client and server example is available in [`example`](./example).

## Projects using webtransport-go

| Project | Description | Stars |
| --- | --- | --- |
| [any-sync](https://github.com/anyproto/any-sync) | Local-first, peer-to-peer synchronization protocol for encrypted collaborative apps | ![GitHub Repo stars](https://img.shields.io/github/stars/anyproto/any-sync?style=flat-square) |
| [Centrifugo](https://github.com/centrifugal/centrifugo) | Scalable real-time messaging server in a language-agnostic way. Self-hosted alternative to Pubnub, Pusher, Ably, socket.io, Phoenix.PubSub, SignalR. | ![GitHub Repo stars](https://img.shields.io/github/stars/centrifugal/centrifugo?style=flat-square) |
| [go-libp2p](https://github.com/libp2p/go-libp2p) | libp2p implementation in Go, powering [Kubo](https://github.com/ipfs/kubo) (IPFS) and [Lotus](https://github.com/filecoin-project/lotus) (Filecoin), among others | ![GitHub Repo stars](https://img.shields.io/github/stars/libp2p/go-libp2p?style=flat-square) |
| [MediaMTX](https://github.com/bluenviron/mediamtx) | Ready-to-use live media server and media proxy supporting Media-over-QUIC, SRT, WebRTC, RTSP, RTMP, HLS, and more | ![GitHub Repo stars](https://img.shields.io/github/stars/bluenviron/mediamtx?style=flat-square) |
| [signalr](https://github.com/philippseith/signalr) | SignalR server and client in Go | ![GitHub Repo stars](https://img.shields.io/github/stars/philippseith/signalr?style=flat-square) |
| [socket.io](https://github.com/zishang520/socket.io) | Socket.IO server and client implementation in Go | ![GitHub Repo stars](https://img.shields.io/github/stars/zishang520/socket.io?style=flat-square) |

If you'd like to see your project added to this list, please send us a PR.


## Release Policy

webtransport-go always aims to support the latest two Go releases.
