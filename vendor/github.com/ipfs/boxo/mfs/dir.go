package mfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	chunker "github.com/ipfs/boxo/chunker"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/provider"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

var (
	ErrNotYetImplemented = errors.New("not yet implemented")
	ErrInvalidChild      = errors.New("invalid child node")
	ErrDirExists         = errors.New("directory already has entry by that name")
)

// TODO: There's too much functionality associated with this structure,
// let's organize it (and if possible extract part of it elsewhere)
// and document the main features of `Directory` here.
type Directory struct {
	inode

	// Internal cache with added entries to the directory, its contents
	// are synched with the underlying `unixfsDir` node in `sync()`.
	entriesCache map[string]FSNode

	lock sync.Mutex
	// TODO: What content is being protected here exactly? The entire directory?

	ctx context.Context

	// UnixFS directory implementation used for creating,
	// reading and editing directories.
	unixfsDir uio.Directory

	prov    provider.MultihashProvider
	chunker chunker.SplitterGen // inherited from parent, nil means default
}

// NewDirectory constructs a new MFS directory.
//
// You probably don't want to call this directly. Instead, construct a new root
// using NewRoot.
func NewDirectory(ctx context.Context, name string, node ipld.Node, parent parent, dserv ipld.DAGService, prov provider.MultihashProvider) (*Directory, error) {
	db, err := uio.NewDirectoryFromNode(dserv, node)
	if err != nil {
		return nil, err
	}

	nd, err := db.GetNode()
	if err != nil {
		return nil, err
	}

	err = dserv.Add(ctx, nd)
	if err != nil {
		return nil, err
	}

	if prov != nil {
		log.Debugf("mfs: provide: %s", nd.Cid())
		if err := prov.StartProviding(false, nd.Cid().Hash()); err != nil {
			log.Warnf("mfs: error while providing %s: %s", nd.Cid(), err)
		}
	}

	return &Directory{
		inode: inode{
			name:       name,
			parent:     parent,
			dagService: dserv,
		},
		ctx:          ctx,
		unixfsDir:    db,
		prov:         prov,
		entriesCache: make(map[string]FSNode),
		chunker:      parent.getChunker(), // inherit from parent
	}, nil
}

// NewEmptyDirectory creates an empty MFS directory with the given [Option]s.
// The directory is added to the DAGService. To create a new MFS
// root use [NewEmptyRoot] instead.
func NewEmptyDirectory(ctx context.Context, name string, p parent, dserv ipld.DAGService, prov provider.MultihashProvider, opts ...Option) (*Directory, error) {
	return newEmptyDirectory(ctx, name, p, dserv, prov, resolveOpts(opts))
}

func newEmptyDirectory(ctx context.Context, name string, p parent, dserv ipld.DAGService, prov provider.MultihashProvider, o options) (*Directory, error) {
	dirOpts := []uio.DirectoryOption{
		uio.WithMaxLinks(o.maxLinks),
		uio.WithMaxHAMTFanout(o.maxHAMTFanout),
		uio.WithStat(o.mode, o.modTime),
		uio.WithCidBuilder(o.cidBuilder),
	}
	if o.sizeEstimationMode != nil {
		dirOpts = append(dirOpts, uio.WithSizeEstimationMode(*o.sizeEstimationMode))
	}
	db, err := uio.NewDirectory(dserv, dirOpts...)
	if err != nil {
		return nil, err
	}
	// Set HAMTShardingSize after creation (not a DirectoryOption)
	if o.hamtShardingSize > 0 {
		db.SetHAMTShardingSize(o.hamtShardingSize)
	}

	nd, err := db.GetNode()
	if err != nil {
		return nil, err
	}

	err = dserv.Add(ctx, nd)
	if err != nil {
		return nil, err
	}

	// note: we don't provide the empty unixfs dir as it is always local.

	// Use chunker from opts if set, otherwise inherit from parent
	c := o.chunker
	if c == nil {
		c = p.getChunker()
	}

	return &Directory{
		inode: inode{
			name:       name,
			parent:     p,
			dagService: dserv,
		},
		ctx:          ctx,
		unixfsDir:    db,
		prov:         prov,
		entriesCache: make(map[string]FSNode),
		chunker:      c,
	}, nil
}

// GetCidBuilder gets the CID builder of the root node
func (d *Directory) GetCidBuilder() cid.Builder {
	return d.unixfsDir.GetCidBuilder()
}

// getChunker implements the parent interface.
func (d *Directory) getChunker() chunker.SplitterGen {
	return d.chunker
}

// SetCidBuilder sets the CID builder
func (d *Directory) SetCidBuilder(b cid.Builder) {
	d.unixfsDir.SetCidBuilder(b)
}

// This method implements the `parent` interface. It first does the local
// update of the child entry in the underlying UnixFS directory and saves
// the newly created directory node with the updated entry in the DAG
// service. Then it propagates the update upwards (through this same
// interface) repeating the whole process in the parent.
func (d *Directory) updateChildEntry(c child) error {
	newDirNode, err := d.localUpdate(c)
	if err != nil {
		return err
	}

	// Continue to propagate the update process upwards
	// (all the way up to the root).
	return d.parent.updateChildEntry(child{d.name, newDirNode})
}

// This method implements the part of `updateChildEntry` that needs
// to be locked around: in charge of updating the UnixFS layer and
// generating the new node reflecting the update. It also stores the
// new node in the DAG layer.
func (d *Directory) localUpdate(c child) (*dag.ProtoNode, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.unixfsDir.AddChild(d.ctx, c.Name, c.Node)
	if err != nil {
		return nil, err
	}

	// TODO: Clearly define how are we propagating changes to lower layers
	// like UnixFS.

	nd, err := d.unixfsDir.GetNode()
	if err != nil {
		return nil, err
	}

	pbnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}

	err = d.dagService.Add(d.ctx, nd)
	if err != nil {
		return nil, err
	}

	if d.prov != nil {
		log.Debugf("mfs: provide: %s", nd.Cid())
		if err := d.prov.StartProviding(false, nd.Cid().Hash()); err != nil {
			log.Warnf("mfs: error while providing %s: %s", nd.Cid(), err)
		}
	}

	return pbnd.Copy().(*dag.ProtoNode), nil
	// TODO: Why do we need a copy?
}

func (d *Directory) Type() NodeType {
	return TDir
}

// cacheNode caches a node into d.childDirs or d.files and returns the FSNode.
func (d *Directory) cacheNode(name string, nd ipld.Node) (FSNode, error) {
	switch nd := nd.(type) {
	case *dag.ProtoNode:
		fsn, err := ft.FSNodeFromBytes(nd.Data())
		if err != nil {
			return nil, err
		}

		switch fsn.Type() {
		case ft.TDirectory, ft.THAMTShard:
			ndir, err := NewDirectory(d.ctx, name, nd, d, d.dagService, d.prov)
			if err != nil {
				return nil, err
			}

			// these options are not persisted so they need to be
			// inherited from the parent.
			parentMaxLinks := d.unixfsDir.GetMaxLinks()
			parentMode := d.unixfsDir.GetSizeEstimationMode()
			ndir.unixfsDir.SetMaxLinks(parentMaxLinks)
			ndir.unixfsDir.SetMaxHAMTFanout(d.unixfsDir.GetMaxHAMTFanout())
			ndir.unixfsDir.SetHAMTShardingSize(d.unixfsDir.GetHAMTShardingSize())
			ndir.unixfsDir.SetSizeEstimationMode(parentMode)
			d.entriesCache[name] = ndir
			return ndir, nil
		case ft.TFile, ft.TRaw, ft.TSymlink:
			nfi, err := NewFile(name, nd, d, d.dagService, d.prov)
			if err != nil {
				return nil, err
			}
			d.entriesCache[name] = nfi
			return nfi, nil
		case ft.TMetadata:
			return nil, ErrNotYetImplemented
		default:
			return nil, ErrInvalidChild
		}
	case *dag.RawNode:
		nfi, err := NewFile(name, nd, d, d.dagService, d.prov)
		if err != nil {
			return nil, err
		}
		d.entriesCache[name] = nfi
		return nfi, nil
	default:
		return nil, errors.New("unrecognized node type in cache node")
	}
}

// Child returns the child of this directory by the given name
func (d *Directory) Child(name string) (FSNode, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.childUnsync(name)
}

func (d *Directory) Uncache(name string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.entriesCache, name)
}

// childFromDag searches through this directories dag node for a child link
// with the given name
func (d *Directory) childFromDag(name string) (ipld.Node, error) {
	return d.unixfsDir.Find(d.ctx, name)
}

// childUnsync returns the child under this directory by the given name
// without locking, useful for operations which already hold a lock
func (d *Directory) childUnsync(name string) (FSNode, error) {
	entry, ok := d.entriesCache[name]
	if ok {
		return entry, nil
	}

	nd, err := d.childFromDag(name)
	if err != nil {
		return nil, err
	}

	return d.cacheNode(name, nd)
}

type NodeListing struct {
	Name string
	Type int
	Size int64
	Hash string
}

func (d *Directory) ListNames(ctx context.Context) ([]string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	var out []string
	err := d.unixfsDir.ForEachLink(ctx, func(l *ipld.Link) error {
		out = append(out, l.Name)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (d *Directory) List(ctx context.Context) ([]NodeListing, error) {
	var out []NodeListing
	err := d.ForEachEntry(ctx, func(nl NodeListing) error {
		out = append(out, nl)
		return nil
	})
	return out, err
}

func (d *Directory) ForEachEntry(ctx context.Context, f func(NodeListing) error) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.unixfsDir.ForEachLink(ctx, func(l *ipld.Link) error {
		c, err := d.childUnsync(l.Name)
		if err != nil {
			return err
		}

		nd, err := c.GetNode()
		if err != nil {
			return err
		}

		child := NodeListing{
			Name: l.Name,
			Type: int(c.Type()),
			Hash: nd.Cid().String(),
		}

		if c, ok := c.(*File); ok {
			size, err := c.Size()
			if err != nil {
				return err
			}
			child.Size = size
		}

		return f(child)
	})
}

// Mkdir creates a child directory that inherits settings from this directory.
func (d *Directory) Mkdir(name string) (*Directory, error) {
	var o options
	o.fillFrom(d)
	return d.mkdirWithOpts(name, o)
}

// MkdirWithOpts creates a child directory with explicit [Option]s.
func (d *Directory) MkdirWithOpts(name string, opts ...Option) (*Directory, error) {
	return d.mkdirWithOpts(name, resolveOpts(opts))
}

func (d *Directory) mkdirWithOpts(name string, o options) (*Directory, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	fsn, err := d.childUnsync(name)
	if err == nil {
		switch fsn := fsn.(type) {
		case *Directory:
			return fsn, os.ErrExist
		case *File:
			return nil, os.ErrExist
		default:
			return nil, fmt.Errorf("unrecognized type: %#v", fsn)
		}
	}

	dirobj, err := newEmptyDirectory(d.ctx, name, d, d.dagService, d.prov, o)
	if err != nil {
		return nil, err
	}

	ndir, err := dirobj.GetNode()
	if err != nil {
		return nil, err
	}

	err = d.unixfsDir.AddChild(d.ctx, name, ndir)
	if err != nil {
		return nil, err
	}

	d.entriesCache[name] = dirobj
	return dirobj, nil
}

func (d *Directory) Unlink(name string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if child, ok := d.entriesCache[name]; ok {
		switch c := child.(type) {
		case *File:
			c.unlinked.Store(true)
		case *Directory:
			c.unlinked.Store(true)
		}
	}
	delete(d.entriesCache, name)

	return d.unixfsDir.RemoveChild(d.ctx, name)
}

func (d *Directory) Flush() error {
	nd, err := d.getNode(true)
	if err != nil {
		return err
	}

	return d.parent.updateChildEntry(child{d.name, nd})
}

// AddChild adds the node 'nd' under this directory giving it the name 'name'
func (d *Directory) AddChild(name string, nd ipld.Node) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	_, err := d.childUnsync(name)
	if err == nil {
		return ErrDirExists
	}

	err = d.dagService.Add(d.ctx, nd)
	if err != nil {
		return err
	}

	if d.prov != nil {
		log.Debugf("mfs: provide: %s", nd.Cid())
		if err := d.prov.StartProviding(false, nd.Cid().Hash()); err != nil {
			log.Warnf("mfs: error while providing %s: %s", nd.Cid(), err)
		}
	}

	return d.unixfsDir.AddChild(d.ctx, name, nd)
}

func (d *Directory) cacheSync(clean bool) error {
	for name, entry := range d.entriesCache {
		nd, err := entry.GetNode()
		if err != nil {
			return err
		}

		err = d.unixfsDir.AddChild(d.ctx, name, nd)
		if err != nil {
			return err
		}
	}
	if clean {
		d.entriesCache = make(map[string]FSNode)
	}
	return nil
}

func (d *Directory) Path() string {
	cur := d
	var out string
	for cur != nil {
		switch parent := cur.parent.(type) {
		case *Directory:
			out = path.Join(cur.name, out)
			cur = parent
		case *Root:
			return "/" + out
		default:
			panic("directory parent neither a directory nor a root")
		}
	}
	return out
}

func (d *Directory) GetNode() (ipld.Node, error) {
	return d.getNode(false)
}

func (d *Directory) getNode(cacheClean bool) (ipld.Node, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.cacheSync(cacheClean)
	if err != nil {
		return nil, err
	}

	nd, err := d.unixfsDir.GetNode()
	if err != nil {
		return nil, err
	}

	err = d.dagService.Add(d.ctx, nd)
	if err != nil {
		return nil, err
	}

	if d.prov != nil {
		log.Debugf("mfs: provide: %s", nd.Cid())
		if err := d.prov.StartProviding(false, nd.Cid().Hash()); err != nil {
			log.Warnf("mfs: error while providing %s: %s", nd.Cid(), err)
		}
	}

	return nd.Copy(), err
}

// Mode returns the directory's POSIX permission bits from UnixFS metadata.
// Returns 0 when no mode is stored.
func (d *Directory) Mode() (os.FileMode, error) {
	nd, err := d.GetNode()
	if err != nil {
		return 0, err
	}
	fsn, err := ft.ExtractFSNode(nd)
	if err != nil {
		return 0, err
	}
	return fsn.Mode() & 0xFFF, nil
}

func (d *Directory) SetMode(mode os.FileMode) error {
	nd, err := d.GetNode()
	if err != nil {
		return err
	}

	fsn, err := ft.ExtractFSNode(nd)
	if err != nil {
		return err
	}

	fsn.SetMode(mode)
	data, err := fsn.GetBytes()
	if err != nil {
		return err
	}

	err = d.setNodeData(data, nd.Links())
	if err != nil {
		return err
	}

	d.unixfsDir.SetStat(mode, time.Time{})
	return nil
}

// ModTime returns the directory's last modification time from UnixFS metadata.
// Returns zero time when no mtime is stored.
func (d *Directory) ModTime() (time.Time, error) {
	nd, err := d.GetNode()
	if err != nil {
		return time.Time{}, err
	}
	fsn, err := ft.ExtractFSNode(nd)
	if err != nil {
		return time.Time{}, err
	}
	return fsn.ModTime(), nil
}

func (d *Directory) SetModTime(ts time.Time) error {
	nd, err := d.GetNode()
	if err != nil {
		return err
	}

	fsn, err := ft.ExtractFSNode(nd)
	if err != nil {
		return err
	}

	fsn.SetModTime(ts)
	data, err := fsn.GetBytes()
	if err != nil {
		return err
	}

	err = d.setNodeData(data, nd.Links())
	if err != nil {
		return err
	}
	d.unixfsDir.SetStat(0, ts)
	return nil
}

func (d *Directory) setNodeData(data []byte, links []*ipld.Link) error {
	nd := dag.NodeWithData(data)
	nd.SetLinks(links)

	// Preserve previous node's CidBuilder
	if builder := d.unixfsDir.GetCidBuilder(); builder != nil {
		nd.SetCidBuilder(builder)
	}

	err := d.dagService.Add(d.ctx, nd)
	if err != nil {
		return err
	}

	if d.prov != nil {
		log.Debugf("mfs: provide: %s", nd.Cid())
		if err := d.prov.StartProviding(false, nd.Cid().Hash()); err != nil {
			log.Warnf("mfs: error while providing %s: %s", nd.Cid(), err)
		}
	}

	err = d.parent.updateChildEntry(child{d.name, nd})
	if err != nil {
		return err
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	db, err := uio.NewDirectoryFromNode(d.dagService, nd)
	if err != nil {
		return err
	}

	// Carry over settings that are not persisted in the DAG node.
	db.SetMaxLinks(d.unixfsDir.GetMaxLinks())
	db.SetMaxHAMTFanout(d.unixfsDir.GetMaxHAMTFanout())
	db.SetHAMTShardingSize(d.unixfsDir.GetHAMTShardingSize())
	db.SetSizeEstimationMode(d.unixfsDir.GetSizeEstimationMode())
	d.unixfsDir = db

	return nil
}
