package webtransport

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

type unestablishedSession struct {
	Streams    []*quic.Stream
	UniStreams []*quic.ReceiveStream

	Timer *time.Timer
}

type sessionEntry struct {
	// at any point in time, only one of these will be non-nil
	Unestablished *unestablishedSession
	Session       *Session
}

const maxRecentlyClosedSessions = 16

type sessionManager struct {
	timeout time.Duration

	mx                     sync.Mutex
	sessions               map[sessionID]sessionEntry
	recentlyClosedSessions []sessionID
}

func newSessionManager(timeout time.Duration) *sessionManager {
	return &sessionManager{
		timeout:  timeout,
		sessions: make(map[sessionID]sessionEntry),
	}
}

// AddStream adds a new bidirectional stream to a WebTransport session.
// If the WebTransport session has not yet been established,
// the stream is buffered until the session is established.
// If that takes longer than timeout, the stream is reset.
func (m *sessionManager) AddStream(str *quic.Stream, id sessionID) {
	m.mx.Lock()
	defer m.mx.Unlock()

	entry, ok := m.sessions[id]
	if !ok {
		// Receiving a stream for an unknown session is expected to be rare,
		// so the performance impact of searching through the slice is negligible.
		if slices.Contains(m.recentlyClosedSessions, id) {
			str.CancelRead(WTBufferedStreamRejectedErrorCode)
			str.CancelWrite(WTBufferedStreamRejectedErrorCode)
			return
		}
		entry = sessionEntry{Unestablished: &unestablishedSession{}}
		m.sessions[id] = entry
	}
	if entry.Session != nil {
		entry.Session.addIncomingStream(str)
		return
	}

	entry.Unestablished.Streams = append(entry.Unestablished.Streams, str)
	m.resetTimer(id)
}

// AddUniStream adds a new unidirectional stream to a WebTransport session.
// If the WebTransport session has not yet been established,
// the stream is buffered until the session is established.
// If that takes longer than timeout, the stream is reset.
func (m *sessionManager) AddUniStream(str *quic.ReceiveStream, id sessionID) {
	m.mx.Lock()
	defer m.mx.Unlock()

	entry, ok := m.sessions[id]
	if !ok {
		// Receiving a stream for an unknown session is expected to be rare,
		// so the performance impact of searching through the slice is negligible.
		if slices.Contains(m.recentlyClosedSessions, id) {
			str.CancelRead(WTBufferedStreamRejectedErrorCode)
			return
		}
		entry = sessionEntry{Unestablished: &unestablishedSession{}}
		m.sessions[id] = entry
	}
	if entry.Session != nil {
		entry.Session.addIncomingUniStream(str)
		return
	}

	entry.Unestablished.UniStreams = append(entry.Unestablished.UniStreams, str)
	m.resetTimer(id)
}

func (m *sessionManager) resetTimer(id sessionID) {
	entry := m.sessions[id]
	if entry.Unestablished.Timer != nil {
		entry.Unestablished.Timer.Reset(m.timeout)
		return
	}
	entry.Unestablished.Timer = time.AfterFunc(m.timeout, func() { m.onTimer(id) })
}

func (m *sessionManager) onTimer(id sessionID) {
	m.mx.Lock()
	defer m.mx.Unlock()

	sessionEntry, ok := m.sessions[id]
	if !ok { // session already closed
		return
	}
	if sessionEntry.Session != nil { // session already established
		return
	}
	for _, str := range sessionEntry.Unestablished.Streams {
		str.CancelRead(WTBufferedStreamRejectedErrorCode)
		str.CancelWrite(WTBufferedStreamRejectedErrorCode)
	}
	for _, uniStr := range sessionEntry.Unestablished.UniStreams {
		uniStr.CancelRead(WTBufferedStreamRejectedErrorCode)
	}
	delete(m.sessions, id)
}

// AddSession adds a new WebTransport session.
func (m *sessionManager) AddSession(id sessionID, s *Session) {
	m.mx.Lock()
	defer m.mx.Unlock()

	entry, ok := m.sessions[id]

	if ok && entry.Unestablished != nil {
		// We might already have an entry of this session.
		// This can happen when we receive streams for this WebTransport session before we complete
		// the Extended CONNECT request.
		for _, str := range entry.Unestablished.Streams {
			s.addIncomingStream(str)
		}
		for _, uniStr := range entry.Unestablished.UniStreams {
			s.addIncomingUniStream(uniStr)
		}
		if entry.Unestablished.Timer != nil {
			entry.Unestablished.Timer.Stop()
		}
		entry.Unestablished = nil
	}
	m.sessions[id] = sessionEntry{Session: s}

	context.AfterFunc(s.Context(), func() {
		m.deleteSession(id)
	})
}

func (m *sessionManager) deleteSession(id sessionID) {
	m.mx.Lock()
	defer m.mx.Unlock()

	delete(m.sessions, id)
	m.recentlyClosedSessions = append(m.recentlyClosedSessions, id)
	if len(m.recentlyClosedSessions) > maxRecentlyClosedSessions {
		m.recentlyClosedSessions = m.recentlyClosedSessions[1:]
	}
}

func (m *sessionManager) Close() {
	m.mx.Lock()
	defer m.mx.Unlock()

	for _, entry := range m.sessions {
		if entry.Unestablished != nil && entry.Unestablished.Timer != nil {
			entry.Unestablished.Timer.Stop()
		}
	}
	clear(m.sessions)
}
