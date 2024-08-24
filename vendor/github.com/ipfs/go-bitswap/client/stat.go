package client

import (
	cid "github.com/ipfs/go-cid"
)

// Stat is a struct that provides various statistics on bitswap operations
type Stat struct {
	Wantlist         []cid.Cid
	BlocksReceived   uint64
	DataReceived     uint64
	DupBlksReceived  uint64
	DupDataReceived  uint64
	MessagesReceived uint64
}

// Stat returns aggregated statistics about bitswap operations
func (bs *Client) Stat() (st Stat, err error) {
	bs.counterLk.Lock()
	c := bs.counters
	st.BlocksReceived = c.blocksRecvd
	st.DupBlksReceived = c.dupBlocksRecvd
	st.DupDataReceived = c.dupDataRecvd
	st.DataReceived = c.dataRecvd
	st.MessagesReceived = c.messagesRecvd
	bs.counterLk.Unlock()
	st.Wantlist = bs.GetWantlist()

	return st, nil
}
