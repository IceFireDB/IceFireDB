package iface

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	core_iface "github.com/ipfs/interface-go-ipfs-core"

	"berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/identityprovider"
)

const KeyEncryptedLinks = "encrypted_links"
const KeyEncryptedLinksNonce = "encrypted_links_nonce"

type WriteOpts struct {
	Pin                 bool
	EncryptedLinks      string
	EncryptedLinksNonce string
}
type ExcludeFunc func(hash cid.Cid) bool

type FetchOptions struct {
	Length        *int
	ShouldExclude ExcludeFunc
	Exclude       []IPFSLogEntry
	Concurrency   int
	Timeout       time.Duration
	// @FIXME(gfanton): progress chan is close automatically by IpfsLog
	ProgressChan chan IPFSLogEntry
	Provider     identityprovider.Interface
	IO           IO
}

type IO interface {
	Write(ctx context.Context, ipfs core_iface.CoreAPI, obj interface{}, opts *WriteOpts) (cid.Cid, error)
	Read(ctx context.Context, ipfs core_iface.CoreAPI, contentIdentifier cid.Cid) (format.Node, error)
	DecodeRawEntry(node format.Node, hash cid.Cid, p identityprovider.Interface) (IPFSLogEntry, error)
	DecodeRawJSONLog(node format.Node) (*JSONLog, error)
}

type IOPreSign interface {
	IO
	PreSign(entry IPFSLogEntry) (IPFSLogEntry, error)
}

type LogOptions struct {
	ID               string
	AccessController accesscontroller.Interface
	Entries          IPFSLogOrderedEntries
	Heads            []IPFSLogEntry
	Clock            IPFSLogLamportClock
	SortFn           func(a, b IPFSLogEntry) (int, error)
	Concurrency      uint
	IO               IO
}

type CreateEntryOptions struct {
	Pin       bool
	PreSigned bool
}

type JSONLog struct {
	ID    string
	Heads []cid.Cid
}

type IteratorOptions struct {
	GT     cid.Cid
	GTE    cid.Cid
	LT     []cid.Cid
	LTE    []cid.Cid
	Amount *int
}

type Snapshot struct {
	ID     string
	Heads  []cid.Cid
	Values []IPFSLogEntry
	Clock  IPFSLogLamportClock
}

type AppendOptions struct {
	PointerCount int
	Pin          bool
}

type IPFSLog interface {
	GetID() string
	Append(ctx context.Context, payload []byte, opts *AppendOptions) (IPFSLogEntry, error)
	Iterator(options *IteratorOptions, output chan<- IPFSLogEntry) error
	Join(otherLog IPFSLog, size int) (IPFSLog, error)
	ToString(payloadMapper func(IPFSLogEntry) string) string
	ToSnapshot() *Snapshot
	ToMultihash(ctx context.Context) (cid.Cid, error)
	Values() IPFSLogOrderedEntries
	ToJSONLog() *JSONLog
	Heads() IPFSLogOrderedEntries
	GetEntries() IPFSLogOrderedEntries
	RawHeads() IPFSLogOrderedEntries
	SetIdentity(identity *identityprovider.Identity)
	IO() IO

	Len() int
	Get(c cid.Cid) (IPFSLogEntry, bool)
}

type EntrySortFn func(IPFSLogEntry, IPFSLogEntry) (int, error)

type IPFSLogOrderedEntries interface {
	// Merge will fusion two OrderedMap of entries.
	Merge(other IPFSLogOrderedEntries) IPFSLogOrderedEntries

	// Copy creates a copy of an OrderedMap.
	Copy() IPFSLogOrderedEntries

	// Get retrieves an Entry using its key.
	Get(key string) (IPFSLogEntry, bool)

	// UnsafeGet retrieves an Entry using its key, returns nil if not found.
	UnsafeGet(key string) IPFSLogEntry

	// Set defines an Entry in the map for a given key.
	Set(key string, value IPFSLogEntry)

	// Slice returns an ordered slice of the values existing in the map.
	Slice() []IPFSLogEntry

	// Keys retrieves the ordered list of keys in the map.
	Keys() []string

	// Len gets the length of the map.
	Len() int

	// At gets an item at the given index in the map, returns nil if not found.
	At(index uint) IPFSLogEntry

	Reverse() IPFSLogOrderedEntries
}

type IPFSLogEntry interface {
	accesscontroller.LogEntry

	New() IPFSLogEntry
	Copy() IPFSLogEntry

	GetLogID() string
	GetNext() []cid.Cid
	GetRefs() []cid.Cid
	GetV() uint64
	GetKey() []byte
	GetSig() []byte
	GetHash() cid.Cid
	GetClock() IPFSLogLamportClock
	GetAdditionalData() map[string]string

	SetPayload([]byte)
	SetLogID(string)
	SetNext([]cid.Cid)
	SetRefs([]cid.Cid)
	SetV(uint64)
	SetKey([]byte)
	SetSig([]byte)
	SetIdentity(*identityprovider.Identity)
	SetHash(cid.Cid)
	SetClock(IPFSLogLamportClock)
	SetAdditionalDataValue(key string, value string)

	IsValid() bool
	Verify(identity identityprovider.Interface, io IO) error
	Equals(b IPFSLogEntry) bool
	IsParent(b IPFSLogEntry) bool
	Defined() bool
}

type IPFSLogLamportClock interface {
	New() IPFSLogLamportClock
	Defined() bool

	GetID() []byte
	GetTime() int

	SetID([]byte)
	SetTime(int)

	Tick() IPFSLogLamportClock
	Merge(clock IPFSLogLamportClock) IPFSLogLamportClock
	Compare(b IPFSLogLamportClock) int
}

type Hashable struct {
	Hash           interface{}
	ID             string
	Payload        []byte
	Next           []string
	Refs           []string
	V              uint64
	Clock          IPFSLogLamportClock
	Key            []byte
	AdditionalData map[string]string
}
