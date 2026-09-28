package webtransport

import "io"

const (
	// settingsEnableWebtransportDraft06 is the value for ENABLE_WEBTRANSPORT
	// that was used up until draft-ietf-webtrans-http3-06.
	settingsEnableWebtransportDraft06 = 0x2b603742

	// settingsWebTransportEnabled is the value for SETTINGS_WT_ENABLED
	settingsWebTransportEnabled = 0x2c7cf000

	// settingsWebTransportMaxSessions is the value for SETTINGS_WT_MAX_SESSIONS
	settingsWebTransportMaxSessions = 0x14e9cd29

	// settingsWebTransportInitialMaxStreamsUni is the value for SETTINGS_WT_INITIAL_MAX_STREAMS_UNI
	settingsWebTransportInitialMaxStreamsUni = 0x2b64

	// settingsWebTransportInitialMaxStreamsBidi is the value for SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI
	settingsWebTransportInitialMaxStreamsBidi = 0x2b65

	// settingsWebTransportInitialMaxData is the value for SETTINGS_WT_INITIAL_MAX_DATA
	settingsWebTransportInitialMaxData = 0x2b61
)

const (
	// protocolHeader is the Extended-CONNECT :protocol value per
	// draft-ietf-webtrans-http3-15 §3.2, §9.1.
	protocolHeader = "webtransport-h3"
	// protocolHeaderLegacy is the pre-draft-15 value. Accepted by the server
	// (but not sent by the client) for compatibility with peers that have not yet
	// migrated.
	protocolHeaderLegacy = "webtransport"
)

func isWebTransportProtocol(s string) bool {
	return s == protocolHeader || s == protocolHeaderLegacy
}

// WebTransport session IDs are the stream IDs of the CONNECT requests that
// establish the sessions. Those requests are sent on client-initiated
// bidirectional streams, whose QUIC stream IDs are divisible by 4.
func isValidSessionID(id uint64) bool {
	return id%4 == 0
}

type byteCountingReader struct {
	io.ByteReader

	BytesRead uint64
}

func (r *byteCountingReader) ReadByte() (byte, error) {
	b, err := r.ByteReader.ReadByte()
	if err == nil {
		r.BytesRead++
	}
	return b, err
}
