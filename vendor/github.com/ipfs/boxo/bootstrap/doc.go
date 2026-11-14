// Package bootstrap manages network bootstrapping for IPFS nodes.
//
// [Bootstrap] periodically checks the number of open connections and, if below
// [BootstrapConfig.MinPeerThreshold], connects to well-known bootstrap peers.
// It also supports saving and restoring backup peers via [WithBackupPeers] for
// resilience when primary bootstrap nodes are unreachable.
package bootstrap
