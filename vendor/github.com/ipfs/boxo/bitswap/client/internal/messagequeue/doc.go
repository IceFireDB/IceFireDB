// Package messagequeue implements a queue of want messages to send to peers.
//
// There is a MessageQueue for each peer. The MessageQueue does not enqueue
// individual outgoing messages, but accumulates information to put into the
// next message. Each MessageQueue keeps want lists and CIDs to cancel:
//
//   - sent/pending peer wants + sent times
//   - sent/pending broadcast wants + sent times
//   - cancel CIDs
//
// As messages are added, existing wantlist items may be changed or removed.
// For example, adding a cancel to the queue for some CIDs also removes any
// pending wants for those same CIDs. Adding a want will remove a cancel for
// that CID. If a want already exists then only the type and priority may be
// adjusted for that same want, so that duplicate messages are not sent.
//
// When enough message updates have accumulated or it has been long enough
// since the previous message was sent, then send the current message. The
// message contains wants and cancels up to a limited size, and is sent to the
// peer. The time that the message was sent is recorded.
//
// If a want has been sent with no response received, for longer than the
// rebroadcast interval, then the want is moved back to the pending list to be
// resent to the peer.
//
// When a response is received, the earliest request time is used to calculate
// the longest latency for updating the message timeout time. The sent times
// are cleared for CIDs in the response.
package messagequeue
