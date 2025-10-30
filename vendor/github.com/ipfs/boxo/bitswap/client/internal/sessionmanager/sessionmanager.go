package sessionmanager

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/client/internal"
	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	notifications "github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bssession "github.com/ipfs/boxo/bitswap/client/internal/session"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	exchange "github.com/ipfs/boxo/exchange"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Session represents a bitswap session managed by the SessionManager.
type Session interface {
	exchange.Fetcher
	ID() uint64
	ReceiveFrom(peer.ID, []cid.Cid, []cid.Cid, []cid.Cid)
	Close()
}

// SessionFactory generates a new session for the SessionManager to track.
type SessionFactory func(
	ctx context.Context,
	sm bssession.SessionManager,
	id uint64,
	sprm bssession.SessionPeerManager,
	sim *bssim.SessionInterestManager,
	pm bssession.PeerManager,
	bpm *bsbpm.BlockPresenceManager,
	notif notifications.PubSub,
	provSearchDelay time.Duration,
	rebroadcastDelay time.Duration,
	self peer.ID) Session

// PeerManagerFactory generates a new peer manager for a session.
type PeerManagerFactory func(id uint64) bssession.SessionPeerManager

// SessionManager is responsible for creating, managing, and dispatching to
// sessions.
type SessionManager struct {
	sessionFactory         SessionFactory
	sessionInterestManager *bssim.SessionInterestManager
	peerManagerFactory     PeerManagerFactory
	blockPresenceManager   *bsbpm.BlockPresenceManager
	peerManager            bssession.PeerManager
	notif                  notifications.PubSub

	// Sessions
	sessLk   sync.Mutex
	sessions map[uint64]Session

	// Session Index
	sessIDLk sync.Mutex
	sessID   uint64

	self peer.ID
}

// New creates a new SessionManager.
func New(sessionFactory SessionFactory, sessionInterestManager *bssim.SessionInterestManager, peerManagerFactory PeerManagerFactory,
	blockPresenceManager *bsbpm.BlockPresenceManager, peerManager bssession.PeerManager, notif notifications.PubSub, self peer.ID,
) *SessionManager {
	return &SessionManager{
		sessionFactory:         sessionFactory,
		sessionInterestManager: sessionInterestManager,
		peerManagerFactory:     peerManagerFactory,
		blockPresenceManager:   blockPresenceManager,
		peerManager:            peerManager,
		notif:                  notif,
		sessions:               make(map[uint64]Session),
		self:                   self,
	}
}

// NewSession initializes a session and adds to the session manager.
//
// The returned Session must be closed via its Close() method, or by canceling
// the context, when no longer needed.
func (sm *SessionManager) NewSession(ctx context.Context, provSearchDelay, rebroadcastDelay time.Duration) Session {
	id := sm.GetNextSessionID()

	ctx, span := internal.StartSpan(ctx, "SessionManager.NewSession", trace.WithAttributes(attribute.String("ID", strconv.FormatUint(id, 10))))
	defer span.End()

	pm := sm.peerManagerFactory(id)
	session := sm.sessionFactory(ctx, sm, id, pm, sm.sessionInterestManager, sm.peerManager, sm.blockPresenceManager, sm.notif, provSearchDelay, rebroadcastDelay, sm.self)

	sm.sessLk.Lock()
	if sm.sessions != nil { // check if SessionManager was shutdown
		sm.sessions[id] = session
	}
	sm.sessLk.Unlock()

	return session
}

func (sm *SessionManager) Shutdown() {
	sm.sessLk.Lock()

	sessions := sm.sessions
	// Ensure that if Shutdown() is called twice we only shut down
	// the sessions once
	sm.sessions = nil

	sm.sessLk.Unlock()

	for _, ses := range sessions {
		ses.Close()
	}
}

func (sm *SessionManager) RemoveSession(sesid uint64) {
	// Remove session from SessionInterestManager - returns the keys that no
	// session is interested in anymore.
	cancelKs := sm.sessionInterestManager.RemoveSession(sesid)

	// Cancel keys that no session is interested in anymore
	sm.cancelWants(cancelKs)

	sm.sessLk.Lock()
	defer sm.sessLk.Unlock()

	// Clean up session
	if sm.sessions != nil { // check if SessionManager was shutdown
		delete(sm.sessions, sesid)
	}
}

// GetNextSessionID returns the next sequential identifier for a session.
func (sm *SessionManager) GetNextSessionID() uint64 {
	sm.sessIDLk.Lock()
	defer sm.sessIDLk.Unlock()

	sm.sessID++
	return sm.sessID
}

// ReceiveFrom is called when a new message is received.
//
// IMPORTANT: ReceiveFrom filters the given Cid slices in place, modifying
// their contents. If the caller needs to preserve a copy of the lists it
// should make a copy before calling ReceiveFrom.
func (sm *SessionManager) ReceiveFrom(ctx context.Context, p peer.ID, blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	// Keep only the keys that at least one session wants
	keys := sm.sessionInterestManager.FilterInterests(blks, haves, dontHaves)
	blks = keys[0]
	haves = keys[1]
	dontHaves = keys[2]
	// Record block presence for HAVE / DONT_HAVE
	sm.blockPresenceManager.ReceiveFrom(p, haves, dontHaves)

	// Notify each session that is interested in the blocks / HAVEs / DONT_HAVEs
	for _, id := range sm.sessionInterestManager.InterestedSessions(blks, haves, dontHaves) {
		sm.sessLk.Lock()
		if sm.sessions == nil { // check if SessionManager was shutdown
			sm.sessLk.Unlock()
			return
		}
		sess, ok := sm.sessions[id]
		sm.sessLk.Unlock()

		if ok {
			sess.ReceiveFrom(p, blks, haves, dontHaves)
		}
	}
}

// CancelSessionWants is called when a session cancels wants because a call to
// GetBlocks() is cancelled
func (sm *SessionManager) CancelSessionWants(sesid uint64, wants []cid.Cid) {
	// Remove session's interest in the given blocks - returns the keys that no
	// session is interested in anymore.
	cancelKs := sm.sessionInterestManager.RemoveSessionWants(sesid, wants)
	sm.cancelWants(cancelKs)
}

func (sm *SessionManager) cancelWants(wants []cid.Cid) {
	// Free up block presence tracking for keys that no session is interested
	// in anymore
	sm.blockPresenceManager.RemoveKeys(wants)

	// Send CANCEL to all peers for blocks that no session is interested in
	// anymore.
	// Note: use bitswap context because session context may already be Done.
	sm.peerManager.SendCancels(wants)
}
