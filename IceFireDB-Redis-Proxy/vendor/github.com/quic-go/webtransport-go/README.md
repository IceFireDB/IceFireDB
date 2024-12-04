# webtransport-go

[![Documentation](https://img.shields.io/badge/docs-quic--go.net-red?style=flat)](https://quic-go.net/docs/)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/quic-go/webtransport-go)](https://pkg.go.dev/github.com/quic-go/webtransport-go)
[![Code Coverage](https://img.shields.io/codecov/c/github/quic-go/webtransport-go/master.svg?style=flat-square)](https://codecov.io/gh/quic-go/webtransport-go/)

webtransport-go is an implementation of the WebTransport protocol, based on [quic-go](https://github.com/quic-go/quic-go). It currently implements [draft-02](https://www.ietf.org/archive/id/draft-ietf-webtrans-http3-02.html) of the specification.

Detailed documentation can be found on [quic-go.net](https://quic-go.net/docs/).

## webtransport-go is currently unfunded.

### What does this mean?

webtransport-go has been unfunded since the beginning of 2024. For the first half of the year, I have been maintaining the project in my spare time.  Maintaining high-quality open-source software requires significant time and effort. This situation is becoming unsustainable, and as of June 2024, I will be ceasing maintenance work on the project.

Specifically, this means:
* I will no longer respond to issues or review PRs.
* I will not keep the API in sync with quic-go.
* Since WebTransport is still an IETF draft, browser compatibility will break as soon as the interoperability target changes.

### If your project relies on WebTransport support, what can you do?

I’m glad you asked. First, I would like to hear about your use case. Second, please consider sponsoring the maintenance and future development of the project. It’s best to reach out to me via email.
