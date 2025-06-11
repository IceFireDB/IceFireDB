package io

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/alecthomas/units"
	mdag "github.com/ipfs/boxo/ipld/merkledag"
	format "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	"github.com/ipfs/boxo/ipld/unixfs/private/linksize"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("unixfs")

// HAMTShardingSize is a global option that allows switching to a HAMTDirectory
// when the BasicDirectory grows above the size (in bytes) signalled by this
// flag. The default size of 0 disables the option.
// The size is not the *exact* block size of the encoded BasicDirectory but just
// the estimated size based byte length of links name and CID (BasicDirectory's
// ProtoNode doesn't use the Data field so this estimate is pretty accurate).
var HAMTShardingSize = int(256 * units.KiB)

// DefaultShardWidth is the default value used for hamt sharding width.
// Needs to be a power of two (shard entry size) and multiple of 8 (bitfield size).
var DefaultShardWidth = 256

// Directory defines a UnixFS directory. It is used for creating, reading and
// editing directories. It allows to work with different directory schemes,
// like the basic or the HAMT implementation.
//
// It just allows to perform explicit edits on a single directory, working with
// directory trees is out of its scope, they are managed by the MFS layer
// (which is the main consumer of this interface).
type Directory interface {
	// SetCidBuilder sets the CID Builder of the root node.
	SetCidBuilder(cid.Builder)

	// AddChild adds a (name, key) pair to the root node.
	AddChild(context.Context, string, ipld.Node) error

	// ForEachLink applies the given function to Links in the directory.
	ForEachLink(context.Context, func(*ipld.Link) error) error

	// EnumLinksAsync returns a channel which will receive Links in the directory
	// as they are enumerated, where order is not guaranteed
	EnumLinksAsync(context.Context) <-chan format.LinkResult

	// Links returns the all the links in the directory node.
	Links(context.Context) ([]*ipld.Link, error)

	// Find returns the root node of the file named 'name' within this directory.
	// In the case of HAMT-directories, it will traverse the tree.
	//
	// Returns os.ErrNotExist if the child does not exist.
	Find(context.Context, string) (ipld.Node, error)

	// RemoveChild removes the child with the given name.
	//
	// Returns os.ErrNotExist if the child doesn't exist.
	RemoveChild(context.Context, string) error

	// GetNode returns the root of this directory.
	GetNode() (ipld.Node, error)

	// GetCidBuilder returns the CID Builder used.
	GetCidBuilder() cid.Builder

	// GetMaxLinks returns the configured value for MaxLinks.
	GetMaxLinks() int

	// SetMaxLinks sets the number of links for the directory. Used when converting
	// between Basic and HAMT.
	SetMaxLinks(n int)

	// GetMaxHAMTFanout returns the configured value for MaxHAMTFanout.
	GetMaxHAMTFanout() int

	// SetMaxHAMTFanout sets the max width of shards when using a HAMT.
	// It must be a power of 2 and multiple of 8. Used when converting
	// between Basic and HAMT.
	SetMaxHAMTFanout(n int)

	// SetStat sets the stat information for the directory. Used when
	// converting between Basic and HAMT.
	SetStat(os.FileMode, time.Time)
}

// A DirectoryOption can be used to initialize directories.
type DirectoryOption func(d Directory)

// WithMaxLinks stablishes the max number of links allowed for a Basic
// directory or a Dynamic directory with an underlying Basic directory:
//
// - On Dynamic directories using a BasicDirectory, it can trigger conversion
// to HAMT when set and exceeded. The subsequent HAMT nodes will use
// MaxHAMTFanout as ShardWidth when set, or DefaultShardWidth
// otherwise. Conversion can be triggered too based on HAMTShardingSize.
//
// - On Dynamic directories using a HAMTDirectory, it can trigger conversion
// to BasicDirectory when the number of directory entries is below MaxLinks (and
// HAMTShardingSize allows).
//
// - On pure Basic directories, it causes an error when adding more than
// MaxLinks children.
func WithMaxLinks(n int) DirectoryOption {
	return func(d Directory) {
		d.SetMaxLinks(n)
	}
}

// WithMaxHAMTFanout stablishes the maximum fanout factor (number of links) for
// a HAMT directory or a Dynamic directory with an underlying HAMT directory:
//
// - On Dynamic directories, it specifies the HAMT fanout when a HAMT
// used. When unset, DefaultShardWidth applies.
//
// - On pure HAMT directories, it sets the ShardWidth, otherwise DefaultShardWidth
// is used.
//
// HAMT directories require this value to be a power of 2 and a multiple of
// 8. If it is not the case, it will not be used and DefaultHAMTWidth will be
// used instead.
func WithMaxHAMTFanout(n int) DirectoryOption {
	return func(d Directory) {
		d.SetMaxHAMTFanout(n)
	}
}

// WithCidBuilder sets the CidBuilder for new Directories.
func WithCidBuilder(cb cid.Builder) DirectoryOption {
	return func(d Directory) {
		d.SetCidBuilder(cb)
	}
}

// WithStat can be used to set the empty directory node permissions.
func WithStat(mode os.FileMode, mtime time.Time) DirectoryOption {
	return func(d Directory) {
		d.SetStat(mode, mtime)
	}
}

// TODO: Evaluate removing `dserv` from this layer and providing it in MFS.
// (The functions should in that case add a `DAGService` argument.)

// Link size estimation function. For production it's usually the one here
// but during test we may mock it to get fixed sizes.
func productionLinkSize(linkName string, linkCid cid.Cid) int {
	return len(linkName) + linkCid.ByteLen()
}

func init() {
	linksize.LinkSizeFunction = productionLinkSize
}

var (
	_ Directory = (*DynamicDirectory)(nil)
	_ Directory = (*BasicDirectory)(nil)
	_ Directory = (*HAMTDirectory)(nil)
)

// BasicDirectory is the basic implementation of `Directory`. All the entries
// are stored in a single node.
type BasicDirectory struct {
	node  *mdag.ProtoNode
	dserv ipld.DAGService

	// Internal variable used to cache the estimated size of the basic directory:
	// for each link, aggregate link name + link CID. DO NOT CHANGE THIS
	// as it will affect the HAMT transition behavior in HAMTShardingSize.
	// (We maintain this value up to date even if the HAMTShardingSize is off
	// since potentially the option could be activated on the fly.)
	estimatedSize int
	totalLinks    int

	// opts
	// maxNumberOfLinks. If set, can trigger conversion to HAMT directory.
	maxLinks      int
	maxHAMTFanout int
	cidBuilder    cid.Builder
	mode          os.FileMode
	mtime         time.Time
}

// HAMTDirectory is the HAMT implementation of `Directory`.
// (See package `hamt` for more information.)
type HAMTDirectory struct {
	shard *hamt.Shard
	dserv ipld.DAGService

	// opts
	maxLinks      int
	maxHAMTFanout int
	cidBuilder    cid.Builder
	mode          os.FileMode
	mtime         time.Time

	// Track the changes in size by the AddChild and RemoveChild calls
	// for the HAMTShardingSize option.
	sizeChange int
	totalLinks int
}

// NewBasicDirectory creates an empty basic directory with the given options.
func NewBasicDirectory(dserv ipld.DAGService, opts ...DirectoryOption) (*BasicDirectory, error) {
	basicDir := &BasicDirectory{
		dserv:         dserv,
		maxHAMTFanout: DefaultShardWidth,
	}

	for _, o := range opts {
		o(basicDir)
	}

	var node *mdag.ProtoNode
	if basicDir.mode > 0 || !basicDir.mtime.IsZero() {
		node = format.EmptyDirNodeWithStat(basicDir.mode, basicDir.mtime)
	} else {
		node = format.EmptyDirNode()
	}
	basicDir.node = node
	err := basicDir.node.SetCidBuilder(basicDir.cidBuilder)
	if err != nil {
		return nil, err
	}

	// Scan node links (if any) to restore estimated size.
	basicDir.computeEstimatedSizeAndTotalLinks()

	return basicDir, nil
}

// NewBasicDirectoryFromNode creates a basic directory wrapping the given
// ProtoNode.
func NewBasicDirectoryFromNode(dserv ipld.DAGService, node *mdag.ProtoNode) *BasicDirectory {
	basicDir := new(BasicDirectory)
	basicDir.node = node
	basicDir.dserv = dserv

	// Scan node links (if any) to restore estimated size.
	basicDir.computeEstimatedSizeAndTotalLinks()

	return basicDir
}

// NewHAMTDirectory creates an empty HAMT directory with the given options.
func NewHAMTDirectory(dserv ipld.DAGService, sizeChange int, opts ...DirectoryOption) (*HAMTDirectory, error) {
	dir := &HAMTDirectory{
		dserv:         dserv,
		sizeChange:    sizeChange,
		maxHAMTFanout: DefaultShardWidth,
	}

	for _, opt := range opts {
		opt(dir)
	}

	// FIXME: do shards not support mtime and mode?
	shard, err := hamt.NewShard(dir.dserv, dir.maxHAMTFanout)
	if err != nil {
		return nil, err
	}

	shard.SetCidBuilder(dir.cidBuilder)
	dir.shard = shard

	return dir, nil
}

// NewHAMTDirectoryFromNode creates a HAMT directory from the given node,
// which must correspond to an existing HAMT.
func NewHAMTDirectoryFromNode(dserv ipld.DAGService, node ipld.Node) (*HAMTDirectory, error) {
	dir := &HAMTDirectory{
		dserv: dserv,
	}

	shard, err := hamt.NewHamtFromDag(dserv, node)
	if err != nil {
		return nil, err
	}
	dir.shard = shard
	dir.totalLinks = len(node.Links())

	return dir, nil
}

// NewDirectory returns a Directory implemented by DynamicDirectory containing
// a BasicDirectory that automatically converts to a from a HAMTDirectory
// based on HAMTShardingSize, MaxLinks and MaxHAMTFanout (when set).
func NewDirectory(dserv ipld.DAGService, opts ...DirectoryOption) (Directory, error) {
	bd, err := NewBasicDirectory(dserv, opts...)
	if err != nil {
		return nil, err
	}
	return &DynamicDirectory{bd}, nil
}

// ErrNotADir implies that the given node was not a unixfs directory
var ErrNotADir = errors.New("merkledag node was not a directory or shard")

// NewDirectoryFromNode loads a unixfs directory from the given IPLD node and
// DAGService.
func NewDirectoryFromNode(dserv ipld.DAGService, node ipld.Node) (Directory, error) {
	protoBufNode, ok := node.(*mdag.ProtoNode)
	if !ok {
		return nil, ErrNotADir
	}

	fsNode, err := format.FSNodeFromBytes(protoBufNode.Data())
	if err != nil {
		return nil, err
	}

	switch fsNode.Type() {
	case format.TDirectory:
		return &DynamicDirectory{NewBasicDirectoryFromNode(dserv, protoBufNode.Copy().(*mdag.ProtoNode))}, nil
	case format.THAMTShard:
		hamtDir, err := NewHAMTDirectoryFromNode(dserv, node)
		if err != nil {
			return nil, err
		}
		return &DynamicDirectory{hamtDir}, nil
	}

	return nil, ErrNotADir
}

// GetMaxLinks returns the configured MaxLinks.
func (d *BasicDirectory) GetMaxLinks() int {
	return d.maxLinks
}

// SetMaxLinks sets the maximum number of links for the Directory, but has no
// side effects until new children are added (in which case the new limit
// applies).
func (d *BasicDirectory) SetMaxLinks(n int) {
	d.maxLinks = n
}

// GetMaxLinks returns the configured HAMTFanout.
func (d *BasicDirectory) GetMaxHAMTFanout() int {
	return d.maxHAMTFanout
}

// SetMAXHAMTFanout has no relevance for BasicDirectories.
func (d *BasicDirectory) SetMaxHAMTFanout(n int) {
	if n > 0 && !validShardWidth(n) {
		log.Warnf("Invalid HAMTMaxFanout: %d. Using default (%d)", n, DefaultShardWidth)
		n = DefaultShardWidth
	}
	if n == 0 {
		n = DefaultShardWidth
	}

	d.maxHAMTFanout = n
}

// SetStat has no effect and only exists to support dynamic directories. Use
// WithStat when creating a new directory instead.
func (d *BasicDirectory) SetStat(mode os.FileMode, mtime time.Time) {
	if mode > 0 {
		d.mode = mode
	}
	if !mtime.IsZero() {
		d.mtime = mtime
	}
}

func (d *BasicDirectory) computeEstimatedSizeAndTotalLinks() {
	d.estimatedSize = 0
	// err is just breaking the iteration and we always return nil
	_ = d.ForEachLink(context.TODO(), func(l *ipld.Link) error {
		d.addToEstimatedSize(l.Name, l.Cid)
		d.totalLinks++
		return nil
	})
	// ForEachLink will never fail traversing the BasicDirectory
	// and neither the inner callback `addToEstimatedSize`.
}

func (d *BasicDirectory) addToEstimatedSize(name string, linkCid cid.Cid) {
	d.estimatedSize += linksize.LinkSizeFunction(name, linkCid)
}

func (d *BasicDirectory) removeFromEstimatedSize(name string, linkCid cid.Cid) {
	d.estimatedSize -= linksize.LinkSizeFunction(name, linkCid)
	if d.estimatedSize < 0 {
		// Something has gone very wrong. Log an error and recompute the
		// size from scratch.
		log.Error("BasicDirectory's estimatedSize went below 0")
		d.computeEstimatedSizeAndTotalLinks()
	}
}

// SetCidBuilder implements the `Directory` interface.
func (d *BasicDirectory) SetCidBuilder(builder cid.Builder) {
	d.cidBuilder = builder
	if d.node != nil {
		d.node.SetCidBuilder(builder)
	}
}

// AddChild implements the `Directory` interface. It adds (or replaces)
// a link to the given `node` under `name`.
func (d *BasicDirectory) AddChild(ctx context.Context, name string, node ipld.Node) error {
	link, err := ipld.MakeLink(node)
	if err != nil {
		return err
	}

	return d.addLinkChild(ctx, name, link)
}

func (d *BasicDirectory) needsToSwitchToHAMTDir(name string, nodeToAdd ipld.Node) (bool, error) {
	if HAMTShardingSize == 0 { // Option disabled.
		return false, nil
	}

	operationSizeChange := 0
	// Find if there is an old entry under that name that will be overwritten.
	entryToRemove, err := d.node.GetNodeLink(name)
	if err != mdag.ErrLinkNotFound {
		if err != nil {
			return false, err
		}
		operationSizeChange -= linksize.LinkSizeFunction(name, entryToRemove.Cid)
	}
	if nodeToAdd != nil {
		operationSizeChange += linksize.LinkSizeFunction(name, nodeToAdd.Cid())
	}

	switchShardingSize := d.estimatedSize+operationSizeChange >= HAMTShardingSize
	switchMaxLinks := false
	// We should switch if maxLinks is set, we have reached it and a new link is being
	// added.
	if nodeToAdd != nil && entryToRemove == nil && d.maxLinks > 0 &&
		(d.totalLinks+1) > d.maxLinks {
		switchMaxLinks = true
	}
	return switchShardingSize || switchMaxLinks, nil
}

// addLinkChild adds the link as an entry to this directory under the given
// name. Plumbing function for the AddChild API.
func (d *BasicDirectory) addLinkChild(ctx context.Context, name string, link *ipld.Link) error {
	// Remove old link and account for size change (if it existed; ignore
	// `ErrNotExist` otherwise).
	err := d.RemoveChild(ctx, name)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else { // existed
		d.totalLinks--
	}

	if d.maxLinks > 0 && d.totalLinks+1 > d.maxLinks {
		return errors.New("BasicDirectory: cannot add child: maxLinks reached")
	}

	err = d.node.AddRawLink(name, link)
	if err != nil {
		return err
	}
	d.addToEstimatedSize(name, link.Cid)
	d.totalLinks++
	return nil
}

// EnumLinksAsync returns a channel which will receive Links in the directory
// as they are enumerated, where order is not guaranteed
func (d *BasicDirectory) EnumLinksAsync(ctx context.Context) <-chan format.LinkResult {
	linkResults := make(chan format.LinkResult)
	go func() {
		defer close(linkResults)
		for _, l := range d.node.Links() {
			select {
			case linkResults <- format.LinkResult{
				Link: l,
				Err:  nil,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return linkResults
}

// ForEachLink implements the `Directory` interface.
func (d *BasicDirectory) ForEachLink(_ context.Context, f func(*ipld.Link) error) error {
	for _, l := range d.node.Links() {
		if err := f(l); err != nil {
			return err
		}
	}
	return nil
}

// Links implements the `Directory` interface.
func (d *BasicDirectory) Links(ctx context.Context) ([]*ipld.Link, error) {
	return d.node.Links(), nil
}

// Find implements the `Directory` interface.
func (d *BasicDirectory) Find(ctx context.Context, name string) (ipld.Node, error) {
	lnk, err := d.node.GetNodeLink(name)
	if err == mdag.ErrLinkNotFound {
		err = os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	return d.dserv.Get(ctx, lnk.Cid)
}

// RemoveChild implements the `Directory` interface.
func (d *BasicDirectory) RemoveChild(ctx context.Context, name string) error {
	// We need to *retrieve* the link before removing it to update the estimated
	// size. This means we may iterate the links slice twice: if traversing this
	// becomes a problem, a factor of 2 isn't going to make much of a difference.
	// We'd likely need to cache a link resolution map in that case.
	link, err := d.node.GetNodeLink(name)
	if err == mdag.ErrLinkNotFound {
		return os.ErrNotExist
	}
	if err != nil {
		return err // at the moment there is no other error besides ErrLinkNotFound
	}

	// The name actually existed so we should update the estimated size.
	d.removeFromEstimatedSize(link.Name, link.Cid)
	d.totalLinks--

	return d.node.RemoveNodeLink(name)
	// GetNodeLink didn't return ErrLinkNotFound so this won't fail with that
	// and we don't need to convert the error again.
}

// GetNode implements the `Directory` interface.
func (d *BasicDirectory) GetNode() (ipld.Node, error) {
	return d.node, nil
}

// GetCidBuilder implements the `Directory` interface.
func (d *BasicDirectory) GetCidBuilder() cid.Builder {
	return d.node.CidBuilder()
}

// switchToSharding returns a HAMT implementation of this directory.
func (d *BasicDirectory) switchToSharding(ctx context.Context, opts ...DirectoryOption) (*HAMTDirectory, error) {
	hamtDir, err := NewHAMTDirectory(d.dserv, 0, opts...)
	if err != nil {
		return nil, err
	}

	for _, lnk := range d.node.Links() {
		err = hamtDir.shard.SetLink(ctx, lnk.Name, lnk)
		if err != nil {
			return nil, err
		}
		hamtDir.totalLinks++
	}

	return hamtDir, nil
}

// SetCidBuilder implements the `Directory` interface.
func (d *HAMTDirectory) SetCidBuilder(builder cid.Builder) {
	d.cidBuilder = builder
	if d.shard != nil {
		d.shard.SetCidBuilder(builder)
	}
}

// GetMaxLinks returns the maxLinks value from a HAMTDirectory.
func (d *HAMTDirectory) GetMaxLinks() int {
	return d.maxLinks
}

// SetMaxLinks has no effect and only exists to support Dynamic directories.
func (d *HAMTDirectory) SetMaxLinks(n int) {
	d.maxLinks = n
}

// GetMaxHAMTFanout returns the maxHAMTFanout value from a HAMTDirectory.
func (d *HAMTDirectory) GetMaxHAMTFanout() int {
	return d.maxHAMTFanout
}

// SetMaxHAMTFanout has no effect and only exists to support Dynamic
// directories. Max fanout can be set during creation using
// WithMaxHAMTFanout().
func (d *HAMTDirectory) SetMaxHAMTFanout(n int) {
	if n > 0 && !validShardWidth(n) {
		log.Warnf("Invalid HAMTMaxFanout: %d. Using default (%d)", n, DefaultShardWidth)
		n = DefaultShardWidth
	}
	if n == 0 {
		n = DefaultShardWidth
	}

	d.maxHAMTFanout = n
}

// SetStat has no effect and only exists to support Dynamic directories.
func (d *HAMTDirectory) SetStat(mode os.FileMode, mtime time.Time) {
	if mode > 0 {
		d.mode = mode
	}
	if !mtime.IsZero() {
		d.mtime = mtime
	}
}

// AddChild implements the `Directory` interface.
func (d *HAMTDirectory) AddChild(ctx context.Context, name string, nd ipld.Node) error {
	oldChild, err := d.shard.Swap(ctx, name, nd)
	if err != nil {
		return err
	}

	if oldChild != nil {
		d.removeFromSizeChange(oldChild.Name, oldChild.Cid)
	}
	d.addToSizeChange(name, nd.Cid())
	if oldChild == nil {
		d.totalLinks++
	}
	return nil
}

// ForEachLink implements the `Directory` interface.
func (d *HAMTDirectory) ForEachLink(ctx context.Context, f func(*ipld.Link) error) error {
	return d.shard.ForEachLink(ctx, f)
}

// EnumLinksAsync returns a channel which will receive Links in the directory
// as they are enumerated, where order is not guaranteed
func (d *HAMTDirectory) EnumLinksAsync(ctx context.Context) <-chan format.LinkResult {
	return d.shard.EnumLinksAsync(ctx)
}

// Links implements the `Directory` interface.
func (d *HAMTDirectory) Links(ctx context.Context) ([]*ipld.Link, error) {
	return d.shard.EnumLinks(ctx)
}

// Find implements the `Directory` interface. It will traverse the tree.
func (d *HAMTDirectory) Find(ctx context.Context, name string) (ipld.Node, error) {
	lnk, err := d.shard.Find(ctx, name)
	if err != nil {
		return nil, err
	}

	return lnk.GetNode(ctx, d.dserv)
}

// RemoveChild implements the `Directory` interface.
func (d *HAMTDirectory) RemoveChild(ctx context.Context, name string) error {
	oldChild, err := d.shard.Take(ctx, name)
	if err != nil {
		return err
	}

	if oldChild != nil {
		d.removeFromSizeChange(oldChild.Name, oldChild.Cid)
		d.totalLinks--
	}

	return nil
}

// GetNode implements the `Directory` interface.
func (d *HAMTDirectory) GetNode() (ipld.Node, error) {
	return d.shard.Node()
}

// GetCidBuilder implements the `Directory` interface.
func (d *HAMTDirectory) GetCidBuilder() cid.Builder {
	return d.shard.CidBuilder()
}

// switchToBasic returns a BasicDirectory implementation of this directory.
func (d *HAMTDirectory) switchToBasic(ctx context.Context, opts ...DirectoryOption) (*BasicDirectory, error) {
	// needsoSwichToBasicDir checks d.maxLinks is appropiate. No check is
	// performed here.
	basicDir, err := NewBasicDirectory(d.dserv, opts...)
	if err != nil {
		return nil, err
	}

	err = d.ForEachLink(ctx, func(lnk *ipld.Link) error {
		err := basicDir.addLinkChild(ctx, lnk.Name, lnk)
		if err != nil {
			return err
		}

		return nil
		// This function enumerates all the links in the Directory requiring all
		// shards to be accessible but it is only called *after* sizeBelowThreshold
		// returns true, which means we have already enumerated and fetched *all*
		// shards in the first place (that's the only way we can be really sure
		// we are actually below the threshold).
	})
	if err != nil {
		return nil, err
	}

	return basicDir, nil
}

func (d *HAMTDirectory) addToSizeChange(name string, linkCid cid.Cid) {
	d.sizeChange += linksize.LinkSizeFunction(name, linkCid)
}

func (d *HAMTDirectory) removeFromSizeChange(name string, linkCid cid.Cid) {
	d.sizeChange -= linksize.LinkSizeFunction(name, linkCid)
}

// Evaluate a switch from HAMTDirectory to BasicDirectory in case the size will
// go above the threshold when we are adding or removing an entry.
// In both the add/remove operations any old name will be removed, and for the
// add operation in particular a new entry will be added under that name (otherwise
// nodeToAdd is nil). We compute both (potential) future subtraction and
// addition to the size change.
func (d *HAMTDirectory) needsToSwitchToBasicDir(ctx context.Context, name string, nodeToAdd ipld.Node) (switchToBasic bool, err error) {
	if HAMTShardingSize == 0 { // Option disabled.
		return false, nil
	}

	operationSizeChange := 0

	// Find if there is an old entry under that name that will be overwritten
	// (AddEntry) or flat out removed (RemoveEntry).
	entryToRemove, err := d.shard.Find(ctx, name)
	if err != os.ErrNotExist {
		if err != nil {
			return false, err
		}
		operationSizeChange -= linksize.LinkSizeFunction(name, entryToRemove.Cid)
	}

	// For the AddEntry case compute the size addition of the new entry.
	if nodeToAdd != nil {
		operationSizeChange += linksize.LinkSizeFunction(name, nodeToAdd.Cid())
	}

	// We must switch if size and maxlinks are below threshold
	canSwitchSize := false
	// Directory size reduced, perhaps below limit.
	if d.sizeChange+operationSizeChange < 0 {
		canSwitchSize, err = d.sizeBelowThreshold(ctx, operationSizeChange)
		if err != nil {
			return false, err
		}
	}

	canSwitchMaxLinks := true
	if d.maxLinks > 0 {
		total := d.totalLinks
		if nodeToAdd != nil {
			total++
		}
		if entryToRemove != nil {
			total--
		}
		if total > d.maxLinks {
			// prevent switching as we would end up with too many links
			canSwitchMaxLinks = false
		}
	}

	return canSwitchSize && canSwitchMaxLinks, nil

}

// Evaluate directory size and a future sizeChange and check if it will be below
// HAMTShardingSize threshold (to trigger a transition to a BasicDirectory).
// Instead of enumerating the entire tree we eagerly call EnumLinksAsync
// until we either reach a value above the threshold (in that case no need
// to keep counting) or an error occurs (like the context being canceled
// if we take too much time fetching the necessary shards).
func (d *HAMTDirectory) sizeBelowThreshold(ctx context.Context, sizeChange int) (bool, error) {
	if HAMTShardingSize == 0 {
		panic("asked to compute HAMT size with HAMTShardingSize option off (0)")
	}

	// We don't necessarily compute the full size of *all* shards as we might
	// end early if we already know we're above the threshold or run out of time.
	partialSize := 0
	var err error
	below := true

	// We stop the enumeration once we have enough information and exit this function.
	ctx, cancel := context.WithCancel(ctx)
	linkResults := d.EnumLinksAsync(ctx)

	for linkResult := range linkResults {
		if linkResult.Err != nil {
			below = false
			err = linkResult.Err
			break
		}

		partialSize += linksize.LinkSizeFunction(linkResult.Link.Name, linkResult.Link.Cid)
		if partialSize+sizeChange >= HAMTShardingSize {
			// We have already fetched enough shards to assert we are above the
			// threshold, so no need to keep fetching.
			below = false
			break
		}
	}
	cancel()

	if !below {
		// Wait for channel to close so links are not being read after return.
		for range linkResults {
		}
		return false, err
	}

	// Enumerated all links in all shards before threshold reached.
	return true, nil
}

// DynamicDirectory wraps a Directory interface and provides extra logic
// to switch from BasicDirectory to HAMTDirectory and backwards based on
// size.
type DynamicDirectory struct {
	Directory
}

var _ Directory = (*DynamicDirectory)(nil)

// SetMaxLinks sets the maximum number of links for the underlying Basic
// directory when used. This operation does not produce any side effects, but
// the new value may trigger Basic-to-HAMT conversions when adding new
// children to Basic directories, or HAMT-to-Basic conversion when operating
// with HAMT directories.
func (d *DynamicDirectory) SetMaxLinks(n int) {
	d.Directory.SetMaxLinks(n)
}

// SetMaxHAMTFanout sets the maximum shard width for HAMT directories. This
// operation does not produce any side effect and only takes effect on a
// conversion from Basic to HAMT directory.
func (d *DynamicDirectory) SetMaxHAMTFanout(n int) {
	d.Directory.SetMaxHAMTFanout(n)
}

// SetStat sets stats information. This operation does not produce any side
// effects. It is taken into account when converting from HAMT to basic
// directory. Mode or mtime can be set independently by using zero for mtime
// or mode respectively.
func (d *DynamicDirectory) SetStat(mode os.FileMode, mtime time.Time) {
	d.Directory.SetStat(mode, mtime)
}

// AddChild implements the `Directory` interface. We check when adding new entries
// if we should switch to HAMTDirectory according to global option(s).
func (d *DynamicDirectory) AddChild(ctx context.Context, name string, nd ipld.Node) error {
	hamtDir, ok := d.Directory.(*HAMTDirectory)
	if ok {
		// We evaluate a switch in the HAMTDirectory case even for an AddChild
		// as it may overwrite an existing entry and end up actually reducing
		// the directory size.
		switchToBasic, err := hamtDir.needsToSwitchToBasicDir(ctx, name, nd)
		if err != nil {
			return err
		}

		if switchToBasic {
			basicDir, err := hamtDir.switchToBasic(ctx,
				WithMaxLinks(hamtDir.maxLinks),
				WithMaxHAMTFanout(hamtDir.maxHAMTFanout),
				WithCidBuilder(hamtDir.GetCidBuilder()),
				WithStat(hamtDir.mode, hamtDir.mtime),
			)
			if err != nil {
				return err
			}
			err = basicDir.AddChild(ctx, name, nd)
			if err != nil {
				return err
			}
			d.Directory = basicDir
			return nil
		}

		return d.Directory.AddChild(ctx, name, nd)
	}

	// BasicDirectory
	basicDir := d.Directory.(*BasicDirectory)
	switchToHAMT, err := basicDir.needsToSwitchToHAMTDir(name, nd)
	if err != nil {
		return err
	}
	if !switchToHAMT {
		return basicDir.AddChild(ctx, name, nd)
	}

	hamtFanout := DefaultShardWidth
	// Verify that our maxLinks is usuable for ShardWidth (power of 2, multiple of 8)
	if validShardWidth(basicDir.maxHAMTFanout) {
		hamtFanout = basicDir.maxHAMTFanout
	}

	hamtDir, err = basicDir.switchToSharding(ctx, WithMaxHAMTFanout(hamtFanout), WithMaxLinks(basicDir.maxLinks), WithCidBuilder(basicDir.GetCidBuilder()))
	if err != nil {
		return err
	}
	err = hamtDir.AddChild(ctx, name, nd)
	if err != nil {
		return err
	}
	d.Directory = hamtDir
	return nil
}

// RemoveChild implements the `Directory` interface. Used in the case where we wrap
// a HAMTDirectory that might need to be downgraded to a BasicDirectory. The
// upgrade path is in AddChild.
func (d *DynamicDirectory) RemoveChild(ctx context.Context, name string) error {
	hamtDir, ok := d.Directory.(*HAMTDirectory)
	if !ok {
		return d.Directory.RemoveChild(ctx, name)
	}

	switchToBasic, err := hamtDir.needsToSwitchToBasicDir(ctx, name, nil)
	if err != nil {
		return err
	}

	if !switchToBasic {
		return hamtDir.RemoveChild(ctx, name)
	}

	maxLinks := hamtDir.maxLinks
	// We have not removed the element that violates MaxLinks, so we have to +1 the limit. We -1 below.
	if maxLinks > 0 {
		maxLinks++
	}

	basicDir, err := hamtDir.switchToBasic(ctx, WithMaxLinks(maxLinks), WithCidBuilder(hamtDir.GetCidBuilder()))
	if err != nil {
		return err
	}

	err = basicDir.RemoveChild(ctx, name)
	if err != nil {
		return err
	}

	if maxLinks > 0 {
		basicDir.maxLinks--
	}
	d.Directory = basicDir
	return nil
}

// validShardWidth verifies that the given number is positive, a power of 2
// and a multiple of 8.
func validShardWidth(n int) bool {
	return n > 0 && (n&(n-1)) == 0 && n&7 == 0
}
