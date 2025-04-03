package dht

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"

	logging "github.com/ipfs/go-log"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var dfLog = logging.Logger("dht/RtDiversityFilter")

var _ peerdiversity.PeerIPGroupFilter = (*rtPeerIPGroupFilter)(nil)

type rtPeerIPGroupFilter struct {
	mu sync.RWMutex
	h  host.Host

	maxPerCpl   int
	maxForTable int

	cplIpGroupCount   map[int]map[peerdiversity.PeerIPGroupKey]int
	tableIpGroupCount map[peerdiversity.PeerIPGroupKey]int
}

// NewRTPeerDiversityFilter constructs the `PeerIPGroupFilter` that will be used to configure
// the diversity filter for the Routing Table.
// Please see the docs for `peerdiversity.PeerIPGroupFilter` AND `peerdiversity.Filter` for more details.
func NewRTPeerDiversityFilter(h host.Host, maxPerCpl, maxForTable int) *rtPeerIPGroupFilter {
	return &rtPeerIPGroupFilter{
		h: h,

		maxPerCpl:   maxPerCpl,
		maxForTable: maxForTable,

		cplIpGroupCount:   make(map[int]map[peerdiversity.PeerIPGroupKey]int),
		tableIpGroupCount: make(map[peerdiversity.PeerIPGroupKey]int),
	}
}

func (r *rtPeerIPGroupFilter) Allow(g peerdiversity.PeerGroupInfo) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := g.IPGroupKey
	cpl := g.Cpl

	if r.tableIpGroupCount[key] >= r.maxForTable {
		dfLog.Debugw("rejecting (max for table) diversity", "peer", g.Id, "cpl", g.Cpl, "ip group", g.IPGroupKey)
		return false
	}

	c, ok := r.cplIpGroupCount[cpl]
	allow := !ok || c[key] < r.maxPerCpl
	if !allow {
		dfLog.Debugw("rejecting (max for cpl) diversity", "peer", g.Id, "cpl", g.Cpl, "ip group", g.IPGroupKey)
	}
	return allow
}

func (r *rtPeerIPGroupFilter) Increment(g peerdiversity.PeerGroupInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := g.IPGroupKey
	cpl := g.Cpl

	r.tableIpGroupCount[key] = r.tableIpGroupCount[key] + 1
	if _, ok := r.cplIpGroupCount[cpl]; !ok {
		r.cplIpGroupCount[cpl] = make(map[peerdiversity.PeerIPGroupKey]int)
	}

	r.cplIpGroupCount[cpl][key] = r.cplIpGroupCount[cpl][key] + 1
}

func (r *rtPeerIPGroupFilter) Decrement(g peerdiversity.PeerGroupInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := g.IPGroupKey
	cpl := g.Cpl

	r.tableIpGroupCount[key] = r.tableIpGroupCount[key] - 1
	if r.tableIpGroupCount[key] == 0 {
		delete(r.tableIpGroupCount, key)
	}

	r.cplIpGroupCount[cpl][key] = r.cplIpGroupCount[cpl][key] - 1
	if r.cplIpGroupCount[cpl][key] == 0 {
		delete(r.cplIpGroupCount[cpl], key)
	}
	if len(r.cplIpGroupCount[cpl]) == 0 {
		delete(r.cplIpGroupCount, cpl)
	}
}

func (r *rtPeerIPGroupFilter) PeerAddresses(p peer.ID) []ma.Multiaddr {
	cs := r.h.Network().ConnsToPeer(p)
	addr := make([]ma.Multiaddr, 0, len(cs))
	for _, c := range cs {
		addr = append(addr, c.RemoteMultiaddr())
	}
	return addr
}

// filterPeersByIPDiversity filters out peers from the response that are overrepresented by IP group.
// If an IP group has more than `limit` peers, all peers with at least 1 address in that IP group
// are filtered out.
func filterPeersByIPDiversity(newPeers []*peer.AddrInfo, limit int) []*peer.AddrInfo {
	// If no diversity limit is set, return all peers
	if limit == 0 {
		return newPeers
	}

	// Count peers per IP group
	ipGroupPeers := make(map[peerdiversity.PeerIPGroupKey]map[peer.ID]struct{})
	for _, p := range newPeers {
		// Find all IP groups this peer belongs to
		for _, addr := range p.Addrs {
			ip, err := manet.ToIP(addr)
			if err != nil {
				continue
			}
			group := peerdiversity.IPGroupKey(ip)
			if len(group) == 0 {
				continue
			}
			if _, ok := ipGroupPeers[group]; !ok {
				ipGroupPeers[group] = make(map[peer.ID]struct{})
			}
			ipGroupPeers[group][p.ID] = struct{}{}
		}
	}

	// Identify overrepresented groups and tag peers for removal
	peersToRemove := make(map[peer.ID]struct{})
	for _, peers := range ipGroupPeers {
		if len(peers) > limit {
			for p := range peers {
				peersToRemove[p] = struct{}{}
			}
		}
	}
	if len(peersToRemove) == 0 {
		// No groups are overrepresented, return all peers
		return newPeers
	}

	// Filter out peers from overrepresented groups
	filteredPeers := make([]*peer.AddrInfo, 0, len(newPeers))
	for _, p := range newPeers {
		if _, ok := peersToRemove[p.ID]; !ok {
			filteredPeers = append(filteredPeers, p)
		}
	}

	return filteredPeers
}
