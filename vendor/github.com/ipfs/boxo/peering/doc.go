// Package peering maintains persistent connections to specified peers.
//
// [PeeringService] automatically reconnects to registered peers when
// disconnected, using exponential backoff up to 10 minutes between attempts.
// Peers can be added and removed at any time, including before the service is
// started.
package peering
