// Package dspinner implements structures and methods to keep track of
// which objects a user wants to keep stored locally.  This implementation
// stores pin data in a datastore.
package dspinner

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/merkledag/dagutils"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	"github.com/ipfs/boxo/pinning/pinner/dsindex"
	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/polydawn/refmt/cbor"
	"github.com/polydawn/refmt/obj/atlas"
)

const (
	basePath     = "/pins"
	pinKeyPath   = "/pins/pin"
	indexKeyPath = "/pins/index"
	dirtyKeyPath = "/pins/state/dirty"
)

var (
	log logging.StandardLogger = logging.Logger("pin")

	linkDirect, linkRecursive string

	pinCidDIndexPath string
	pinCidRIndexPath string
	pinNameIndexPath string

	dirtyKey = ds.NewKey(dirtyKeyPath)

	pinAtl atlas.Atlas
)

func init() {
	directStr, ok := ipfspinner.ModeToString(ipfspinner.Direct)
	if !ok {
		panic("could not find Direct pin enum")
	}
	linkDirect = directStr

	recursiveStr, ok := ipfspinner.ModeToString(ipfspinner.Recursive)
	if !ok {
		panic("could not find Recursive pin enum")
	}
	linkRecursive = recursiveStr

	pinCidRIndexPath = path.Join(indexKeyPath, "cidRindex")
	pinCidDIndexPath = path.Join(indexKeyPath, "cidDindex")
	pinNameIndexPath = path.Join(indexKeyPath, "nameIndex")

	pinAtl = atlas.MustBuild(
		atlas.BuildEntry(pin{}).StructMap().
			AddField("Cid", atlas.StructMapEntry{SerialName: "cid"}).
			AddField("Metadata", atlas.StructMapEntry{SerialName: "metadata", OmitEmpty: true}).
			AddField("Mode", atlas.StructMapEntry{SerialName: "mode"}).
			AddField("Name", atlas.StructMapEntry{SerialName: "name", OmitEmpty: true}).
			Complete(),
		atlas.BuildEntry(cid.Cid{}).Transform().
			TransformMarshal(atlas.MakeMarshalTransformFunc(func(live cid.Cid) ([]byte, error) { return live.MarshalBinary() })).
			TransformUnmarshal(atlas.MakeUnmarshalTransformFunc(func(serializable []byte) (cid.Cid, error) {
				c := cid.Cid{}
				err := c.UnmarshalBinary(serializable)
				if err != nil {
					return cid.Cid{}, err
				}
				return c, nil
			})).Complete(),
	)
	pinAtl = pinAtl.WithMapMorphism(atlas.MapMorphism{KeySortMode: atlas.KeySortMode_Strings})
}

// pinner implements the Pinner interface
type pinner struct {
	autoSync bool
	lock     sync.RWMutex

	dserv  ipld.DAGService
	dstore ds.Datastore

	cidDIndex dsindex.Indexer
	cidRIndex dsindex.Indexer
	nameIndex dsindex.Indexer

	clean int64
	dirty int64

	rootsProvider  provider.MultihashProvider
	pinnedProvider provider.MultihashProvider
}

var _ ipfspinner.Pinner = (*pinner)(nil)

type pin struct {
	Id       string
	Cid      cid.Cid
	Metadata map[string]any
	Mode     ipfspinner.Mode
	Name     string
}

func (p *pin) dsKey() ds.Key {
	return ds.NewKey(path.Join(pinKeyPath, p.Id))
}

func newPin(c cid.Cid, mode ipfspinner.Mode, name string) *pin {
	return &pin{
		Id:   path.Base(ds.RandomKey().String()),
		Cid:  c,
		Name: name,
		Mode: mode,
	}
}

type syncDAGService interface {
	ipld.DAGService
	Sync() error
}

type Option struct {
	f func(p *pinner)
}

// WithPinnedProvider sets a provider for all pinned CIDs to be provided
// (directly or recursively).
func WithPinnedProvider(prov provider.MultihashProvider) Option {
	return Option{func(p *pinner) {
		log.Debug("pinned-providing configured")
		p.pinnedProvider = prov
	}}
}

// WithRootsProvider sets a provider for root CIDs and direct pins to be
// provided.
func WithRootsProvider(prov provider.MultihashProvider) Option {
	return Option{func(p *pinner) {
		log.Debug("roots-providing configured")
		p.rootsProvider = prov
	}}
}

// New creates a new pinner and loads its keysets from the given datastore. If
// there is no data present in the datastore, then an empty pinner is returned.
//
// By default, changes are automatically flushed to the datastore.  This can be
// disabled by calling SetAutosync(false), which will require that Flush be
// called explicitly.
func New(ctx context.Context, dstore ds.Datastore, dserv ipld.DAGService, opts ...Option) (*pinner, error) {
	p := &pinner{
		autoSync:  true,
		cidDIndex: dsindex.New(dstore, ds.NewKey(pinCidDIndexPath)),
		cidRIndex: dsindex.New(dstore, ds.NewKey(pinCidRIndexPath)),
		nameIndex: dsindex.New(dstore, ds.NewKey(pinNameIndexPath)),
		dserv:     dserv,
		dstore:    dstore,
	}

	for _, o := range opts {
		o.f(p)
	}

	data, err := dstore.Get(ctx, dirtyKey)
	if err != nil {
		if err == ds.ErrNotFound {
			return p, nil
		}
		return nil, fmt.Errorf("cannot load dirty flag: %v", err)
	}
	if data[0] == 1 {
		p.dirty = 1

		err = p.rebuildIndexes(ctx)
		if err != nil {
			return nil, fmt.Errorf("cannot rebuild indexes: %v", err)
		}
	}

	return p, nil
}

// SetAutosync allows auto-syncing to be enabled or disabled during runtime.
// This may be used to turn off autosync before doing many repeated pinning
// operations, and then turn it on after.  Returns the previous value.
func (p *pinner) SetAutosync(auto bool) bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.autoSync, auto = auto, p.autoSync
	return auto
}

// Pin the given node, optionally recursive
func (p *pinner) Pin(ctx context.Context, node ipld.Node, recurse bool, name string) error {
	err := p.dserv.Add(ctx, node)
	if err != nil {
		return err
	}

	if recurse {
		return p.doPinRecursive(ctx, node.Cid(), true, name)
	} else {
		return p.doPinDirect(ctx, node.Cid(), name)
	}
}

func (p *pinner) doPinRecursive(ctx context.Context, c cid.Cid, fetch bool, name string) error {
	cidKey := c.KeyString()

	p.lock.Lock()
	defer p.lock.Unlock()

	found, err := p.cidRIndex.HasAny(ctx, cidKey)
	if err != nil {
		return err
	}
	// Do not return immediately! Just remove the recursive pins for the current CID.
	// This allows the process to continue and the pin to be re-added with a new name.
	//
	// TODO: remove this to support multiple pins per CID
	if found {
		_, err = p.removePinsForCid(ctx, c, ipfspinner.Recursive)
		if err != nil {
			return err
		}
	}

	dirtyBefore := p.dirty

	if fetch {
		// temporary unlock to fetch the entire graph
		p.lock.Unlock()
		// Fetch graph starting at node identified by cid
		var opts []merkledag.WalkOption
		if p.pinnedProvider != nil {
			opts = append(opts, merkledag.WithProvider(p.pinnedProvider))
		}
		err = merkledag.FetchGraph(ctx, c, p.dserv, opts...)
		p.lock.Lock()
		if err != nil {
			return err
		}
	}

	// If autosyncing, sync dag service before making any change to pins
	err = p.flushDagService(ctx, false)
	if err != nil {
		return err
	}

	// Only look again if something has changed.
	if p.dirty != dirtyBefore {
		found, err := p.cidRIndex.HasAny(ctx, cidKey)
		if err != nil {
			return err
		}
		if found {
			return nil
		}
	}

	// TODO: remove this to support multiple pins per CID
	found, err = p.cidDIndex.HasAny(ctx, cidKey)
	if err != nil {
		return err
	}
	if found {
		_, err = p.removePinsForCid(ctx, c, ipfspinner.Direct)
		if err != nil {
			return err
		}
	}

	_, err = p.addPin(ctx, c, ipfspinner.Recursive, name)
	if err != nil {
		return err
	}
	err = p.flushPins(ctx, false)
	if err != nil {
		return err
	}

	// Provide only if we have not set pinnedProvider, as otherwise
	// we would provide the roots twice.
	if p.rootsProvider != nil && p.pinnedProvider == nil {
		log.Debugf("pinner: provide root %s", c)
		if err := p.rootsProvider.StartProviding(false, c.Hash()); err != nil {
			log.Warnf("pinner: error while providing %s: %s", c, err)
		}
	}
	return nil
}

func (p *pinner) doPinDirect(ctx context.Context, c cid.Cid, name string) error {
	cidKey := c.KeyString()

	p.lock.Lock()
	defer p.lock.Unlock()

	found, err := p.cidRIndex.HasAny(ctx, cidKey)
	if err != nil {
		return err
	}
	if found {
		return fmt.Errorf("%s already pinned recursively", c.String())
	}

	// Remove existing direct pins for this CID. This ensures that the pin will be
	// re-saved with the new name and that there aren't clashing pins for the same
	// CID.
	//
	// TODO: remove this to support multiple pins per CID.
	found, err = p.cidDIndex.HasAny(ctx, cidKey)
	if err != nil {
		return err
	}
	if found {
		_, err = p.removePinsForCid(ctx, c, ipfspinner.Direct)
		if err != nil {
			return err
		}
	}

	_, err = p.addPin(ctx, c, ipfspinner.Direct, name)
	if err != nil {
		return err
	}

	err = p.flushPins(ctx, false)
	if err != nil {
		return err
	}

	if p.rootsProvider != nil {
		log.Debugf("pinner: provide direct pin %s", c)
		if err := p.rootsProvider.StartProviding(false, c.Hash()); err != nil {
			log.Warnf("pinner: error while providing %s: %s", c, err)
		}
	}

	return nil
}

func (p *pinner) addPin(ctx context.Context, c cid.Cid, mode ipfspinner.Mode, name string) (string, error) {
	// Create new pin and store in datastore
	pp := newPin(c, mode, name)

	// Serialize pin
	pinData, err := encodePin(pp)
	if err != nil {
		return "", fmt.Errorf("could not encode pin: %v", err)
	}

	p.setDirty(ctx)

	// Store the pin
	err = p.dstore.Put(ctx, pp.dsKey(), pinData)
	if err != nil {
		return "", err
	}

	// Store CID index
	switch mode {
	case ipfspinner.Recursive:
		err = p.cidRIndex.Add(ctx, c.KeyString(), pp.Id)
	case ipfspinner.Direct:
		err = p.cidDIndex.Add(ctx, c.KeyString(), pp.Id)
	default:
		panic("pin mode must be recursive or direct")
	}
	if err != nil {
		return "", fmt.Errorf("could not add pin cid index: %v", err)
	}

	if name != "" {
		// Store name index
		err = p.nameIndex.Add(ctx, name, pp.Id)
		if err != nil {
			if mode == ipfspinner.Recursive {
				e := p.cidRIndex.Delete(ctx, c.KeyString(), pp.Id)
				if e != nil {
					log.Errorf("error deleting index: %s", e)
				}
			} else {
				e := p.cidDIndex.Delete(ctx, c.KeyString(), pp.Id)
				if e != nil {
					log.Errorf("error deleting index: %s", e)
				}
			}
			return "", fmt.Errorf("could not add pin name index: %v", err)
		}
	}

	return pp.Id, nil
}

func (p *pinner) removePin(ctx context.Context, pp *pin) error {
	p.setDirty(ctx)
	var err error

	// Remove cid index from datastore
	if pp.Mode == ipfspinner.Recursive {
		err = p.cidRIndex.Delete(ctx, pp.Cid.KeyString(), pp.Id)
	} else {
		err = p.cidDIndex.Delete(ctx, pp.Cid.KeyString(), pp.Id)
	}
	if err != nil {
		return err
	}

	if pp.Name != "" {
		// Remove name index from datastore
		err = p.nameIndex.Delete(ctx, pp.Name, pp.Id)
		if err != nil {
			return err
		}
	}

	// The pin is removed last so that an incomplete remove is detected by a
	// pin that has a missing index.
	err = p.dstore.Delete(ctx, pp.dsKey())
	if err != nil {
		return err
	}

	return nil
}

// Unpin a given key
func (p *pinner) Unpin(ctx context.Context, c cid.Cid, recursive bool) error {
	cidKey := c.KeyString()

	p.lock.Lock()
	defer p.lock.Unlock()

	// TODO: use Ls() to lookup pins when new pinning API available
	/*
		matchSpec := map[string][]string {
			"cid": []string{c.String}
		}
		matches := p.Ls(matchSpec)
	*/
	has, err := p.cidRIndex.HasAny(ctx, cidKey)
	if err != nil {
		return err
	}

	if has {
		if !recursive {
			return fmt.Errorf("%s is pinned recursively", c.String())
		}
	} else {
		has, err = p.cidDIndex.HasAny(ctx, cidKey)
		if err != nil {
			return err
		}
		if !has {
			return ipfspinner.ErrNotPinned
		}
	}

	removed, err := p.removePinsForCid(ctx, c, ipfspinner.Any)
	if err != nil {
		return err
	}
	if !removed {
		return nil
	}

	return p.flushPins(ctx, false)
}

// IsPinned returns whether or not the given key is pinned
// and an explanation of why its pinned
func (p *pinner) IsPinned(ctx context.Context, c cid.Cid) (string, bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.isPinnedWithType(ctx, c, ipfspinner.Any)
}

// IsPinnedWithType returns whether or not the given cid is pinned with the
// given pin type, as well as returning the type of pin its pinned with.
func (p *pinner) IsPinnedWithType(ctx context.Context, c cid.Cid, mode ipfspinner.Mode) (string, bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.isPinnedWithType(ctx, c, mode)
}

func (p *pinner) isPinnedWithType(ctx context.Context, c cid.Cid, mode ipfspinner.Mode) (string, bool, error) {
	cidKey := c.KeyString()
	switch mode {
	case ipfspinner.Recursive:
		has, err := p.cidRIndex.HasAny(ctx, cidKey)
		if err != nil {
			return "", false, err
		}
		if has {
			return linkRecursive, true, nil
		}
		return "", false, nil
	case ipfspinner.Direct:
		has, err := p.cidDIndex.HasAny(ctx, cidKey)
		if err != nil {
			return "", false, err
		}
		if has {
			return linkDirect, true, nil
		}
		return "", false, nil
	case ipfspinner.Internal:
		return "", false, nil
	case ipfspinner.Indirect:
	case ipfspinner.Any:
		has, err := p.cidRIndex.HasAny(ctx, cidKey)
		if err != nil {
			return "", false, err
		}
		if has {
			return linkRecursive, true, nil
		}
		has, err = p.cidDIndex.HasAny(ctx, cidKey)
		if err != nil {
			return "", false, err
		}
		if has {
			return linkDirect, true, nil
		}
	default:
		err := fmt.Errorf(
			"invalid Pin Mode '%d', must be one of {%d, %d, %d, %d, %d}",
			mode, ipfspinner.Direct, ipfspinner.Indirect, ipfspinner.Recursive,
			ipfspinner.Internal, ipfspinner.Any)
		return "", false, err
	}

	// Default is Indirect
	visitedSet := cid.NewSet()

	// No index for given CID, so search children of all recursive pinned CIDs
	var has bool
	var rc cid.Cid
	var e error
	err := p.cidRIndex.ForEach(ctx, "", func(key, value string) bool {
		rc, e = cid.Cast([]byte(key))
		if e != nil {
			return false
		}
		has, e = hasChild(ctx, p.dserv, rc, c, visitedSet.Visit)
		if e != nil {
			return false
		}
		if has {
			return false
		}
		return true
	})
	if err != nil {
		return "", false, err
	}
	if e != nil {
		return "", false, e
	}

	if has {
		return rc.String(), true, nil
	}

	return "", false, nil
}

// CheckIfPinned checks if a set of keys are pinned, more efficient than
// calling IsPinned for each key, returns the pinned status of cid(s)
//
// TODO: If a CID is pinned by multiple pins, should they all be reported?
func (p *pinner) CheckIfPinned(ctx context.Context, cids ...cid.Cid) ([]ipfspinner.Pinned, error) {
	// Simply delegate to CheckIfPinnedWithType with Any mode and no names
	return p.CheckIfPinnedWithType(ctx, ipfspinner.Any, false, cids...)
}

// loadPinName attempts to load the pin name if includeNames is true.
// It logs errors but doesn't fail the operation if name loading fails.
func (p *pinner) loadPinName(ctx context.Context, pin *ipfspinner.Pinned, pinID string, includeNames bool) {
	if !includeNames {
		return
	}
	pinData, err := p.loadPin(ctx, pinID)
	if err != nil {
		log.Errorf("failed to load pin %s: %v", pinID, err)
		return
	}
	pin.Name = pinData.Name
}

// CheckIfPinnedWithType implements the Pinner interface, checking specific pin types.
// This method is optimized to only check the requested pin type(s).
func (p *pinner) CheckIfPinnedWithType(ctx context.Context, mode ipfspinner.Mode, includeNames bool, cids ...cid.Cid) ([]ipfspinner.Pinned, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	switch mode {
	case ipfspinner.Any:
		// Check all pin types
		pinned := make([]ipfspinner.Pinned, 0, len(cids))
		toCheck := cid.NewSet()

		// First check for non-Indirect pins directly
		for _, c := range cids {
			cidKey := c.KeyString()

			// Check recursive pins
			ids, err := p.cidRIndex.Search(ctx, cidKey)
			if err != nil {
				return nil, err
			}
			if len(ids) > 0 {
				pin := ipfspinner.Pinned{Key: c, Mode: ipfspinner.Recursive}
				p.loadPinName(ctx, &pin, ids[0], includeNames)
				pinned = append(pinned, pin)
			} else {
				// Check direct pins
				ids, err = p.cidDIndex.Search(ctx, cidKey)
				if err != nil {
					return nil, err
				}
				if len(ids) > 0 {
					pin := ipfspinner.Pinned{Key: c, Mode: ipfspinner.Direct}
					p.loadPinName(ctx, &pin, ids[0], includeNames)
					pinned = append(pinned, pin)
				} else {
					toCheck.Add(c)
				}
			}
		}

		// Check for indirect pins
		if toCheck.Len() > 0 {
			if err := p.traverseIndirectPins(ctx, toCheck, &pinned); err != nil {
				return nil, err
			}
		}

		// Anything left in toCheck is not pinned
		for _, k := range toCheck.Keys() {
			pinned = append(pinned, ipfspinner.Pinned{Key: k, Mode: ipfspinner.NotPinned})
		}
		return pinned, nil

	case ipfspinner.Recursive, ipfspinner.Direct:
		// Check only the specific index
		return p.checkPinsInIndex(ctx, mode, includeNames, cids...)

	case ipfspinner.Indirect:
		// Only check for indirect pins (expensive - requires traversal of all recursive pins' graphs)
		return p.checkIndirectPins(ctx, cids...)

	case ipfspinner.Internal:
		// Internal pins are not exposed to users, return NotPinned
		// Note: this is legacy behavior kept for backward-compatibility
		pinned := make([]ipfspinner.Pinned, 0, len(cids))
		for _, c := range cids {
			pinned = append(pinned, ipfspinner.Pinned{Key: c, Mode: ipfspinner.NotPinned})
		}
		return pinned, nil

	default:
		// For unknown modes, return an error to maintain backward compatibility
		return nil, fmt.Errorf(
			"invalid Pin Mode '%d', must be one of {%d, %d, %d, %d, %d}",
			mode, ipfspinner.Direct, ipfspinner.Indirect, ipfspinner.Recursive,
			ipfspinner.Internal, ipfspinner.Any)
	}
}

// checkPinsInIndex is a helper that checks for pins in a specific index based on mode (pin type).
// It selects either the recursive or direct index depending on the mode parameter.
func (p *pinner) checkPinsInIndex(ctx context.Context, mode ipfspinner.Mode, includeNames bool, cids ...cid.Cid) ([]ipfspinner.Pinned, error) {
	pinned := make([]ipfspinner.Pinned, 0, len(cids))

	// Select the appropriate index based on mode (pin type)
	var index dsindex.Indexer
	if mode == ipfspinner.Recursive {
		index = p.cidRIndex
	} else {
		index = p.cidDIndex
	}

	for _, c := range cids {
		cidKey := c.KeyString()
		ids, err := index.Search(ctx, cidKey)
		if err != nil {
			return nil, err
		}

		if len(ids) > 0 {
			pin := ipfspinner.Pinned{Key: c, Mode: mode}
			p.loadPinName(ctx, &pin, ids[0], includeNames)
			pinned = append(pinned, pin)
		} else {
			pinned = append(pinned, ipfspinner.Pinned{Key: c, Mode: ipfspinner.NotPinned})
		}
	}

	return pinned, nil
}

// traverseIndirectPins is a helper that traverses all recursive pins to find indirect pins.
// It modifies the pinned slice and toCheck set in place.
func (p *pinner) traverseIndirectPins(ctx context.Context, toCheck *cid.Set, pinned *[]ipfspinner.Pinned) error {
	var walkErr error
	visited := cid.NewSet()
	err := p.cidRIndex.ForEach(ctx, "", func(key, value string) bool {
		// Check for context cancellation at the start of each recursive pin
		select {
		case <-ctx.Done():
			walkErr = ctx.Err()
			return false
		default:
		}

		var rk cid.Cid
		rk, walkErr = cid.Cast([]byte(key))
		if walkErr != nil {
			return false
		}
		walkErr = merkledag.Walk(ctx, merkledag.GetLinksWithDAG(p.dserv), rk, func(c cid.Cid) bool {
			if toCheck.Len() == 0 || !visited.Visit(c) {
				return false
			}
			if toCheck.Has(c) {
				*pinned = append(*pinned, ipfspinner.Pinned{Key: c, Mode: ipfspinner.Indirect, Via: rk})
				toCheck.Remove(c)
			}
			return true
		}, merkledag.Concurrent())
		if walkErr != nil {
			return false
		}
		return toCheck.Len() > 0
	})
	if err != nil {
		return err
	}
	if walkErr != nil {
		return walkErr
	}
	return nil
}

// checkIndirectPins checks if the given cids are pinned indirectly
func (p *pinner) checkIndirectPins(ctx context.Context, cids ...cid.Cid) ([]ipfspinner.Pinned, error) {
	pinned := make([]ipfspinner.Pinned, 0, len(cids))
	toCheck := cid.NewSet()

	// Filter out CIDs that are recursively pinned at the root level.
	// A recursively pinned CID is not considered indirect because recursive pins
	// are comprehensive (include all children), making "recursive" take precedence
	// over "indirect".
	//
	// However, we do NOT filter out direct pins here. Direct pins only pin a
	// single block, not its children. Therefore, a CID can legitimately be both:
	// - Directly pinned (explicitly pinned as a single block)
	// - Indirectly pinned (referenced by another pinned object's DAG)
	// This is why the asymmetry between recursive and direct pins is intentional.
	//
	// NOTE: While this behavior may feel arbitrary, we preserve it for compatibility
	// as this is how 'ipfs pin ls' has behaved for nearly a decade. The test
	// t0081-repo-pinning.sh in Kubo explicitly expects a CID to be both direct
	// and indirect, guarding this established behavior.
	for _, c := range cids {
		cidKey := c.KeyString()

		// Check if recursively pinned
		ids, err := p.cidRIndex.Search(ctx, cidKey)
		if err != nil {
			return nil, err
		}
		if len(ids) > 0 {
			// This CID is recursively pinned at root level, not indirect
			pinned = append(pinned, ipfspinner.Pinned{Key: c, Mode: ipfspinner.NotPinned})
			continue
		}

		// Still check for indirect even if directly pinned
		// A CID can be both direct and indirect
		toCheck.Add(c)
	}

	// Now check for indirect pins by traversing recursive pins
	if toCheck.Len() > 0 {
		if err := p.traverseIndirectPins(ctx, toCheck, &pinned); err != nil {
			return nil, err
		}
	}

	// Anything left in toCheck is not pinned
	for _, k := range toCheck.Keys() {
		pinned = append(pinned, ipfspinner.Pinned{Key: k, Mode: ipfspinner.NotPinned})
	}

	return pinned, nil
}

// removePinsForCid removes all pins for a cid that has the specified mode.
// Returns true if any pins, and all corresponding CID index entries, were
// removed.  Otherwise, returns false.
func (p *pinner) removePinsForCid(ctx context.Context, c cid.Cid, mode ipfspinner.Mode) (bool, error) {
	// Search for pins by CID
	var ids []string
	var err error
	cidKey := c.KeyString()
	switch mode {
	case ipfspinner.Recursive:
		ids, err = p.cidRIndex.Search(ctx, cidKey)
	case ipfspinner.Direct:
		ids, err = p.cidDIndex.Search(ctx, cidKey)
	case ipfspinner.Any:
		ids, err = p.cidRIndex.Search(ctx, cidKey)
		if err != nil {
			return false, err
		}
		dIds, err := p.cidDIndex.Search(ctx, cidKey)
		if err != nil {
			return false, err
		}
		if len(dIds) != 0 {
			ids = append(ids, dIds...)
		}
	}
	if err != nil {
		return false, err
	}

	var removed bool

	// Remove the pin with the requested mode
	for _, pid := range ids {
		var pp *pin
		pp, err = p.loadPin(ctx, pid)
		if err != nil {
			if err == ds.ErrNotFound {
				p.setDirty(ctx)
				// Fix index; remove index for pin that does not exist
				switch mode {
				case ipfspinner.Recursive:
					_, err = p.cidRIndex.DeleteKey(ctx, cidKey)
					if err != nil {
						return false, fmt.Errorf("error deleting index: %s", err)
					}
				case ipfspinner.Direct:
					_, err = p.cidDIndex.DeleteKey(ctx, cidKey)
					if err != nil {
						return false, fmt.Errorf("error deleting index: %s", err)
					}
				case ipfspinner.Any:
					_, err = p.cidRIndex.DeleteKey(ctx, cidKey)
					if err != nil {
						return false, fmt.Errorf("error deleting index: %s", err)
					}
					_, err = p.cidDIndex.DeleteKey(ctx, cidKey)
					if err != nil {
						return false, fmt.Errorf("error deleting index: %s", err)
					}
				}
				if err = p.flushPins(ctx, true); err != nil {
					return false, err
				}
				// Mark this as removed since it removed an index, which is
				// what prevents determines if an item is pinned.
				removed = true
				log.Error("found CID index with missing pin")
				continue
			}
			return false, err
		}
		if mode == ipfspinner.Any || pp.Mode == mode {
			err = p.removePin(ctx, pp)
			if err != nil {
				return false, err
			}
			removed = true
		}
	}
	return removed, nil
}

// loadPin loads a single pin from the datastore.
func (p *pinner) loadPin(ctx context.Context, pid string) (*pin, error) {
	pinData, err := p.dstore.Get(ctx, ds.NewKey(path.Join(pinKeyPath, pid)))
	if err != nil {
		return nil, err
	}
	return decodePin(pid, pinData)
}

// DirectKeys returns a slice containing the directly pinned keys
func (p *pinner) DirectKeys(ctx context.Context, detailed bool) <-chan ipfspinner.StreamedPin {
	return p.streamIndex(ctx, p.cidDIndex, detailed)
}

// RecursiveKeys returns a slice containing the recursively pinned keys
func (p *pinner) RecursiveKeys(ctx context.Context, detailed bool) <-chan ipfspinner.StreamedPin {
	return p.streamIndex(ctx, p.cidRIndex, detailed)
}

func (p *pinner) streamIndex(ctx context.Context, index dsindex.Indexer, detailed bool) <-chan ipfspinner.StreamedPin {
	out := make(chan ipfspinner.StreamedPin)

	go func() {
		defer close(out)

		p.lock.RLock()
		defer p.lock.RUnlock()

		cidSet := cid.NewSet()
		send := func(sp ipfspinner.StreamedPin) (ok bool) {
			select {
			case <-ctx.Done():
				return false
			case out <- sp:
				return true
			}
		}

		err := index.ForEach(ctx, "", func(key, value string) bool {
			c, err := cid.Cast([]byte(key))
			if err != nil {
				send(ipfspinner.StreamedPin{Err: err})
				return false
			}

			var pin ipfspinner.Pinned
			if detailed {
				pp, err := p.loadPin(ctx, value)
				if err != nil {
					send(ipfspinner.StreamedPin{Err: err})
					return false
				}

				pin.Key = pp.Cid
				pin.Mode = pp.Mode
				pin.Name = pp.Name
			} else {
				pin.Key = c
			}

			if !cidSet.Has(c) {
				if !send(ipfspinner.StreamedPin{Pin: pin}) {
					return false
				}
				cidSet.Add(c)
			}
			return true
		})
		if err != nil {
			send(ipfspinner.StreamedPin{Err: err})
			return
		}
	}()

	return out
}

// InternalPins returns all cids kept pinned for the internal state of the
// pinner
func (p *pinner) InternalPins(ctx context.Context, detailed bool) <-chan ipfspinner.StreamedPin {
	c := make(chan ipfspinner.StreamedPin)
	close(c)
	return c
}

// Update updates a recursive pin from one cid to another.  This is equivalent
// to pinning the new one and unpinning the old one.
//
// TODO: This will not work when multiple pins are supported
func (p *pinner) Update(ctx context.Context, from, to cid.Cid, unpin bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	fromValues, err := p.cidRIndex.Search(ctx, from.KeyString())
	if err != nil {
		return err
	}
	if len(fromValues) != 1 {
		return errors.New("'from' cid was not recursively pinned already")
	}

	// If `from` already recursively pinned and `to` is the same, then all done
	if from == to {
		return nil
	}

	// Check if the `to` cid is already recursively pinned
	toFound, err := p.cidRIndex.HasAny(ctx, to.KeyString())
	if err != nil {
		return err
	}
	if toFound {
		return errors.New("'to' cid was already recursively pinned")
	}

	// Temporarily unlock while we fetch the differences.
	p.lock.Unlock()
	err = dagutils.DiffEnumerate(ctx, p.dserv, from, to)
	p.lock.Lock()

	if err != nil {
		return err
	}

	// Get pin information so that we can keep the name.
	pin, err := p.loadPin(ctx, fromValues[0])
	if err != nil {
		return err
	}

	_, err = p.addPin(ctx, to, ipfspinner.Recursive, pin.Name)
	if err != nil {
		return err
	}

	if unpin {
		_, err = p.removePinsForCid(ctx, from, ipfspinner.Recursive)
		if err != nil {
			return err
		}
	}

	return p.flushPins(ctx, false)
}

func (p *pinner) flushDagService(ctx context.Context, force bool) error {
	if !p.autoSync && !force {
		return nil
	}
	if syncDServ, ok := p.dserv.(syncDAGService); ok {
		if err := syncDServ.Sync(); err != nil {
			return fmt.Errorf("cannot sync pinned data: %v", err)
		}
	}
	return nil
}

func (p *pinner) flushPins(ctx context.Context, force bool) error {
	if !p.autoSync && !force {
		return nil
	}
	if err := p.dstore.Sync(ctx, ds.NewKey(basePath)); err != nil {
		return fmt.Errorf("cannot sync pin state: %v", err)
	}
	p.setClean(ctx)
	return nil
}

// Flush encodes and writes pinner keysets to the datastore
func (p *pinner) Flush(ctx context.Context) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	err := p.flushDagService(ctx, true)
	if err != nil {
		return err
	}

	return p.flushPins(ctx, true)
}

// PinWithMode allows the user to have fine grained control over pin
// counts
func (p *pinner) PinWithMode(ctx context.Context, c cid.Cid, mode ipfspinner.Mode, name string) error {
	// TODO: remove his to support multiple pins per CID
	switch mode {
	case ipfspinner.Recursive:
		return p.doPinRecursive(ctx, c, false, name)
	case ipfspinner.Direct:
		return p.doPinDirect(ctx, c, name)
	default:
		return errors.New("unrecognized pin mode")
	}
}

// hasChild recursively looks for a Cid among the children of a root Cid.
// The visit function can be used to shortcut already-visited branches.
func hasChild(ctx context.Context, ng ipld.NodeGetter, root cid.Cid, child cid.Cid, visit func(cid.Cid) bool) (bool, error) {
	links, err := ipld.GetLinks(ctx, ng, root)
	if err != nil {
		return false, err
	}
	for _, lnk := range links {
		c := lnk.Cid
		if lnk.Cid.Equals(child) {
			return true, nil
		}
		if visit(c) {
			has, err := hasChild(ctx, ng, c, child, visit)
			if err != nil {
				return false, err
			}

			if has {
				return has, nil
			}
		}
	}
	return false, nil
}

func encodePin(p *pin) ([]byte, error) {
	b, err := cbor.MarshalAtlased(p, pinAtl)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func decodePin(pid string, data []byte) (*pin, error) {
	p := &pin{Id: pid}
	err := cbor.UnmarshalAtlased(cbor.DecodeOptions{}, data, p, pinAtl)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// setDirty updates the dirty counter and saves a dirty state in the datastore
// if the state was previously clean
func (p *pinner) setDirty(ctx context.Context) {
	wasClean := p.dirty == p.clean
	p.dirty++

	if !wasClean {
		return // do not save; was already dirty
	}

	data := []byte{1}
	err := p.dstore.Put(ctx, dirtyKey, data)
	if err != nil {
		log.Errorf("failed to set pin dirty flag: %s", err)
		return
	}
	err = p.dstore.Sync(ctx, dirtyKey)
	if err != nil {
		log.Errorf("failed to sync pin dirty flag: %s", err)
	}
}

// setClean saves a clean state value in the datastore if the state was
// previously dirty
func (p *pinner) setClean(ctx context.Context) {
	if p.dirty == p.clean {
		return // already clean
	}

	data := []byte{0}
	err := p.dstore.Put(ctx, dirtyKey, data)
	if err != nil {
		log.Errorf("failed to set clear dirty flag: %s", err)
		return
	}
	if err = p.dstore.Sync(ctx, dirtyKey); err != nil {
		log.Errorf("failed to sync cleared pin dirty flag: %s", err)
		return
	}
	p.clean = p.dirty // set clean
}

// sync datastore after every 50 cid repairs
const syncRepairFrequency = 50

// rebuildIndexes uses the stored pins to rebuild secondary indexes.  This
// resolves any discrepancy between secondary indexes and pins that could
// result from a program termination between saving the two.
func (p *pinner) rebuildIndexes(ctx context.Context) error {
	// Load all pins from the datastore.
	q := query.Query{
		Prefix: pinKeyPath,
	}
	results, err := p.dstore.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	var checkedCount, repairedCount int

	// Iterate all pins and check if the corresponding recursive or direct
	// index is missing.  If the index is missing then create the index.
	for r := range results.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.Error != nil {
			return fmt.Errorf("cannot read index: %v", r.Error)
		}
		ent := r.Entry
		pp, err := decodePin(path.Base(ent.Key), ent.Value)
		if err != nil {
			return err
		}

		indexKey := pp.Cid.KeyString()

		var indexer, staleIndexer dsindex.Indexer
		var idxrName, staleIdxrName string
		if pp.Mode == ipfspinner.Recursive {
			indexer = p.cidRIndex
			staleIndexer = p.cidDIndex
			idxrName = linkRecursive
			staleIdxrName = linkDirect
		} else if pp.Mode == ipfspinner.Direct {
			indexer = p.cidDIndex
			staleIndexer = p.cidRIndex
			idxrName = linkDirect
			staleIdxrName = linkRecursive
		} else {
			log.Error("unrecognized pin mode:", pp.Mode)
			continue
		}

		// Remove any stale index from unused indexer
		ok, err := staleIndexer.HasValue(ctx, indexKey, pp.Id)
		if err != nil {
			return err
		}
		if ok {
			// Delete any stale index
			log.Errorf("deleting stale %s pin index for cid %v", staleIdxrName, pp.Cid.String())
			if err = staleIndexer.Delete(ctx, indexKey, pp.Id); err != nil {
				return err
			}
		}

		// Check that the indexer indexes this pin
		ok, err = indexer.HasValue(ctx, indexKey, pp.Id)
		if err != nil {
			return err
		}

		var repaired bool
		if !ok {
			// Do not rebuild if index has an old value with leading slash
			ok, err = indexer.HasValue(ctx, indexKey, "/"+pp.Id)
			if err != nil {
				return err
			}
			if !ok {
				log.Errorf("repairing %s pin index for cid: %s", idxrName, pp.Cid.String())
				// There was no index found for this pin.  This was either an
				// incomplete add or and incomplete delete of a pin.  Either
				// way, restore the index to complete the add or to undo the
				// incomplete delete.
				if err = indexer.Add(ctx, indexKey, pp.Id); err != nil {
					return err
				}
				repaired = true
			}
		}
		// Check for missing name index
		if pp.Name != "" {
			ok, err = p.nameIndex.HasValue(ctx, pp.Name, pp.Id)
			if err != nil {
				return err
			}
			if !ok {
				log.Errorf("repairing name pin index for cid: %s", pp.Cid.String())
				if err = p.nameIndex.Add(ctx, pp.Name, pp.Id); err != nil {
					return err
				}
			}
			repaired = true
		}

		if repaired {
			repairedCount++
		}
		checkedCount++
		if checkedCount%syncRepairFrequency == 0 {
			p.flushPins(ctx, true)
		}
	}

	log.Errorf("checked %d pins for invalid indexes, repaired %d pins", checkedCount, repairedCount)
	return p.flushPins(ctx, true)
}
