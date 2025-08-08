package nopfs

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	mhreg "github.com/multiformats/go-multihash/core"
	"go.uber.org/multierr"
	"gopkg.in/yaml.v3"
)

// ErrHeaderNotFound is returned when no header can be Decoded.
var ErrHeaderNotFound = errors.New("header not found")

const maxHeaderSize = 1 << 20 // 1MiB per the spec
const maxLineSize = 2 << 20   // 2MiB per the spec
const currentVersion = 1

// SafeCids is a map of known, innoffensive CIDs that correspond to
// empty-blocks or empty-directories. Blocking these can break applications so
// they are ignored (with a warning), when they appear on a denylist.
var SafeCids = map[cid.Cid]string{
	cid.MustParse("QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn"):                "empty unixfs directory",
	cid.MustParse("bafyaabakaieac"):                                                "empty unixfs directory inlined",
	cid.MustParse("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"):   "empty block",
	cid.MustParse("bafkqaaa"):                                                      "empty block inlined",
	cid.MustParse("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH"):                "empty block dag-pb",
	cid.MustParse("bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua"):   "empty block dag-cbor",
	cid.MustParse("baguqeeraiqjw7i2vwntyuekgvulpp2det2kpwt6cd7tx5ayqybqpmhfk76fa"): "empty block dag-json",
}

// DenylistHeader represents the header of a Denylist file.
type DenylistHeader struct {
	Version     int
	Name        string
	Description string
	Author      string
	Hints       map[string]string

	headerBytes []byte
	headerLines uint64
}

// Decode decodes a DenlistHeader from a reader. Per the specification, the
// maximum size of a header is 1KiB. If no header is found, ErrHeaderNotFound
// is returned.
func (h *DenylistHeader) Decode(r io.Reader) error {
	limRdr := &io.LimitedReader{
		R: r,
		N: maxHeaderSize,
	}
	buf := bufio.NewReader(limRdr)

	h.headerBytes = nil
	h.headerLines = 0

	for {
		line, err := buf.ReadBytes('\n')
		if err == io.EOF {
			h.headerBytes = nil
			h.headerLines = 0
			return ErrHeaderNotFound
		}
		if err != nil {
			return err
		}
		h.headerLines++
		if string(line) == "---\n" {
			break
		}
		h.headerBytes = append(h.headerBytes, line...)
	}

	err := yaml.Unmarshal(h.headerBytes, h)
	if err != nil {
		logger.Error(err)
		return err
	}

	// In the future this may need adapting to support several versions.
	if h.Version > 0 && h.Version != currentVersion {
		err = errors.New("unsupported denylist version")
		logger.Error(err)
		return err
	}
	return nil
}

// String provides a short string summary of the Header.
func (h DenylistHeader) String() string {
	return fmt.Sprintf("%s (%s) by %s", h.Name, h.Description, h.Author)
}

// A Denylist represents a denylist file and its rules. It can parse and
// follow a denylist file, and can answer questions about blocked or allowed
// items in this denylist.
type Denylist struct {
	Header   DenylistHeader
	Filename string

	Entries Entries

	IPFSBlocksDB       *BlocksDB
	IPNSBlocksDB       *BlocksDB
	DoubleHashBlocksDB map[uint64]*BlocksDB // mhCode -> blocks using that code
	PathBlocksDB       *BlocksDB
	PathPrefixBlocks   Entries
	// MimeBlocksDB

	f       io.ReadSeekCloser
	watcher *fsnotify.Watcher
}

// NewDenylist opens a denylist file and processes it (parses all its entries).
//
// If follow is false, the file handle is closed.
//
// If follow is true, the denylist file will be followed upon return. Any
// appended rules will be processed live-updated in the
// denylist. Denylist.Close() should be used when the Denylist or the
// following is no longer needed.
func NewDenylist(filepath string, follow bool) (*Denylist, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	dl := Denylist{
		Filename:           filepath,
		f:                  f,
		IPFSBlocksDB:       &BlocksDB{},
		IPNSBlocksDB:       &BlocksDB{},
		PathBlocksDB:       &BlocksDB{},
		DoubleHashBlocksDB: make(map[uint64]*BlocksDB),
	}

	err = dl.parseAndFollow(follow)
	return &dl, err
}

// NewDenylistReader processes a denylist from the given reader (parses all
// its entries).
func NewDenylistReader(r io.ReadSeekCloser) (*Denylist, error) {
	dl := Denylist{
		Filename:           "",
		f:                  r,
		IPFSBlocksDB:       &BlocksDB{},
		IPNSBlocksDB:       &BlocksDB{},
		PathBlocksDB:       &BlocksDB{},
		DoubleHashBlocksDB: make(map[uint64]*BlocksDB),
	}

	err := dl.parseAndFollow(false)
	return &dl, err
}

// read the header and make sure the reader is in the right position for
// further processing. In case of no header a default one is used.
func (dl *Denylist) readHeader() error {
	err := dl.Header.Decode(dl.f)
	if err == ErrHeaderNotFound {
		dl.Header.Version = 1
		dl.Header.Name = filepath.Base(dl.Filename)
		dl.Header.Description = "No header found"
		dl.Header.Author = "unknown"
		// reset the reader
		_, err = dl.f.Seek(0, 0)
		if err != nil {
			logger.Error(err)
			return err
		}
		logger.Warnf("Opening %s: empty header", dl.Filename)
		logger.Infof("Processing %s: %s", dl.Filename, dl.Header)
		return nil
	} else if err != nil {
		return err
	}

	logger.Infof("Processing %s: %s", dl.Filename, dl.Header)

	// We have to deal with the buffered reader reading beyond the header.
	_, err = dl.f.Seek(int64(len(dl.Header.headerBytes)+4), 0)
	if err != nil {
		logger.Error(err)
		return err
	}
	// The reader should be set at the line after ---\n now.
	// Reader to parse the rest of lines.

	return nil
}

// All closing on error is performed here.
func (dl *Denylist) parseAndFollow(follow bool) error {
	if err := dl.readHeader(); err != nil {
		dl.Close()
		return err
	}

	// we will update N as we go after every line.  Fixme: this is
	// going to play weird as the buffered reader will read-ahead
	// and consume N.
	limRdr := &io.LimitedReader{
		R: dl.f,
		N: maxLineSize,
	}
	r := bufio.NewReader(limRdr)
	lineNumber := dl.Header.headerLines

	// we finished reading the file as it EOF'ed.
	if !follow {
		return dl.followLines(r, limRdr, lineNumber, nil)
	}
	// We now wait for new lines.

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		dl.Close()
		return err
	}
	dl.watcher = watcher
	err = watcher.Add(dl.Filename)
	if err != nil {
		dl.Close()
		return err
	}

	waitForWrite := func() error {
		for {
			select {
			case event := <-dl.watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write {
					return nil
				}
			case err := <-dl.watcher.Errors:
				// TODO: log
				return err
			}
		}
	}

	go dl.followLines(r, limRdr, lineNumber, waitForWrite)
	return nil
}

// followLines reads lines from a buffered reader on top of a limited reader,
// that we reset on every line. This enforces line-length limits.
// If we pass a waitWrite() function, then it waits when finding EOF.
//
// Is this the right way of tailing a file? Pretty sure there are a
// bunch of gotchas. It seems to work when saving on top of a file
// though. Also, important that the limitedReader is there to avoid
// parsing a huge lines.  Also, this could be done by just having
// watchers on the folder, but requires a small refactoring.
func (dl *Denylist) followLines(r *bufio.Reader, limRdr *io.LimitedReader, lineNumber uint64, waitWrite func() error) error {
	line := ""
	limRdr.N = maxLineSize // reset

	for {
		partialLine, err := r.ReadString('\n')

		// limit reader exhausted
		if err == io.EOF && limRdr.N == 0 {
			err = fmt.Errorf("line too long. %s:%d", dl.Filename, lineNumber+1)
			logger.Error(err)
			dl.Close()
			return err
		}

		// Record how much of a line we have
		line += partialLine

		if err == io.EOF {
			if waitWrite != nil { // keep waiting
				err := waitWrite()
				if err != nil {
					logger.Error(err)
					dl.Close()
					return err
				}
				continue
			} else { // Finished
				return nil
			}
		}
		if err != nil {
			logger.Error(err)
			dl.Close()
			return err
		}

		// if we are here, no EOF, no error and ReadString()
		// found an \n so we have a full line.

		lineNumber++
		// we have read up to \n
		if err := dl.parseLine(line, lineNumber); err != nil {
			logger.Error(err)
			// log error and continue with next line

		}
		// reset for next line
		line = ""
		limRdr.N = maxLineSize // reset
	}
}

// parseLine processes every full-line read and puts it into the BlocksDB etc.
// so that things can be queried later. It turns lines into Entry objects.
//
// Note (hector): I'm using B58Encoded-multihash strings as keys for IPFS,
// DoubleHash BlocksDB. Why? We could be using the multihash bytes directly.
// Some reasons (this should be changed if there are better reasons to do so):
//
//   - B58Encoded-strings produce readable keys. These will be readable keys
//     in a database, readable keys in the debug logs etc. Having readable
//     multihashes instead of raw bytes is nice.
//
//   - If we assume IPFS mostly deals in CIDv0s (raw multihashes), we can
//     avoid parsing the Qmxxx multihash in /ipfs/Qmxxx/path and just use the
//     string directly for lookups.
//
//   - If we used raw bytes, we would have to decode every cidV0, but we would
//     not have to b58-encode multihashes for lookups. Chances that this is
//     better but well (decide before going with permanent storage!).
func (dl *Denylist) parseLine(line string, number uint64) error {
	line = strings.TrimSuffix(line, "\n")
	if len(line) == 0 || line[0] == '#' {
		return nil
	}

	e := Entry{
		Line:     number,
		RawValue: line,
		Hints:    make(map[string]string),
	}

	// Every entry carries the header hints. They will be
	// overwritten by rule hints below when in conflict.
	for k, v := range dl.Header.Hints {
		e.Hints[k] = v
	}

	// the rule is always field-0. Anything else is hints.
	splitFields := strings.Fields(line)
	rule := splitFields[0]
	if len(splitFields) > 1 { // we have hints
		hintSlice := splitFields[1:]
		for _, kv := range hintSlice {
			key, value, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			e.Hints[key] = value
		}
	}

	// We treat +<rule> and -<rule> the same. Both serve to declare and
	// allow-rule.
	if unprefixed, found := cutPrefix(rule, "-"); found {
		e.AllowRule = true
		rule = unprefixed
	} else if unprefixed, found := cutPrefix(rule, "+"); found {
		e.AllowRule = true
		rule = unprefixed
	} else if unprefixed, found := cutPrefix(rule, "!"); found {
		e.AllowRule = true
		rule = unprefixed
	}

	switch {
	case strings.HasPrefix(rule, "//"):
		// Double-hash rule.
		// It can be a Multihash or a sha256-hex-encoded string.

		rule = strings.TrimPrefix(rule, "//")

		parseMultihash := func(mhStr string) (uint64, multihash.Multihash, error) {
			mh, err := multihash.FromB58String(rule)
			if err != nil { // not a b58 string usually
				return 0, nil, err
			}
			dmh, err := multihash.Decode(mh)
			if err != nil { // looked like a mhash but it was not.
				return 0, nil, err
			}

			// Identity hash doesn't make sense for double
			// hashing.  In practice it is usually a hex string
			// that has been wrongly parsed as multihash.
			if dmh.Code == 0 {
				return 0, nil, errors.New("identity hash cannot be a double hash")
			}

			// if we are here it means we have something that
			// could be interpreted as a multihash but it may
			// still be a hex-encoded string that just parsed as
			// b58 fine. In any case, we should check we know how to
			// hash for this type of multihash.
			_, err = mhreg.GetVariableHasher(dmh.Code, dmh.Length)
			if err != nil {
				return 0, nil, err
			}

			return dmh.Code, mh, nil
		}

		parseHexString := func(hexStr string) (uint64, multihash.Multihash, error) {
			if len(hexStr) != 64 {
				return 0, nil, errors.New("hex string are sha2-256 hashes and must be 64 chars (32 bytes) long")
			}

			bs, err := hex.DecodeString(rule)
			if err != nil {
				return 0, nil, err
			}
			// We have a hex-encoded string and assume it is a
			// SHA2_256. TODO: could support hints here to use
			// different functions.
			mhBytes, err := multihash.Encode(bs, multihash.SHA2_256)
			if err != nil {
				return 0, nil, err
			}
			return multihash.SHA2_256, multihash.Multihash(mhBytes), nil
		}

		addRule := func(e Entry, mhType uint64, mh multihash.Multihash) error {
			bpath, _ := NewBlockedPath("")
			e.Path = bpath
			e.Multihash = mh

			// Store it in the appropriate BlocksDB (per mhtype).
			key := e.Multihash.B58String()
			if blocks := dl.DoubleHashBlocksDB[mhType]; blocks == nil {
				dl.DoubleHashBlocksDB[mhType] = &BlocksDB{}
			}
			dl.DoubleHashBlocksDB[mhType].Store(key, e)
			logger.Debugf("%s:%d: Double-hash rule. Func: %s. Key: %s. Entry: %s", filepath.Base(dl.Filename), number, multicodec.Code(mhType).String(), key, e)
			return nil
		}

		// We have to assume that perhaps one day a sha256 hex string
		// is going to parse as a valid multihash with an known
		// hashing function etc. And vice-versa perhaps.
		//
		// In a case where we cannot distinguish between a b58btc
		// multihash and a hex-string, we add rules for both, to make
		// sure we always block what should be blocked.
		code, mh, err1 := parseMultihash(rule)
		if err1 == nil {
			// clone the entry as add-rule modifies it.
			e1 := e.Clone()
			if err := addRule(e1, code, mh); err != nil {
				return err
			}
		}

		code, mh, err2 := parseHexString(rule)
		if err2 == nil {
			if err := addRule(e, code, mh); err != nil {
				return err
			}
		}

		if err1 != nil && err2 != nil {
			return fmt.Errorf("double-hash cannot be parsed as a multihash with a supported hashing function (%w) nor as a sha256 hex-encoded string (%w) (%s:%d)", err1, err2, dl.Filename, number)
		}

	case strings.HasPrefix(rule, "/ipfs/"), strings.HasPrefix(rule, "/ipld/"):
		// ipfs/ipld rule. We parse the CID and use the
		// b58-encoded-multihash as key to the Entry.

		rule = strings.TrimPrefix(rule, "/ipfs/")
		rule = strings.TrimPrefix(rule, "/ipld/")
		cidStr, subPath, _ := strings.Cut(rule, "/")

		c, err := cid.Decode(cidStr)
		if err != nil {
			return fmt.Errorf("error extracting cid %s (%s:%d): %w", cidStr, dl.Filename, number, err)
		}

		// Blocking these by mistake can break some applications (by
		// "some" we mean Kubo).
		if _, ok := SafeCids[c]; ok {
			logger.Warnf("Ignored: %s corresponds to a known empty folder or block and will not be blocked", c)
			return nil
		}

		e.Multihash = c.Hash()

		blockedPath, err := NewBlockedPath(subPath)
		if err != nil {
			return err
		}
		e.Path = blockedPath

		// Add to IPFS by component multihash
		key := e.Multihash.B58String()
		dl.IPFSBlocksDB.Store(key, e)
		logger.Debugf("%s:%d: IPFS rule. Key: %s. Entry: %s", filepath.Base(dl.Filename), number, key, e)
	case strings.HasPrefix(rule, "/ipns/"):
		// ipns rule. If it carries anything parseable as a CID, we
		// store indexed by the b58-multihash. Otherwise assume it is
		// a domain name and store that directly.
		rule, _ = cutPrefix(rule, "/ipns/")
		key, subPath, _ := strings.Cut(rule, "/")
		c, err := cid.Decode(key)
		if err == nil { // CID key handling.
			key = c.Hash().B58String()
		}
		blockedPath, err := NewBlockedPath(subPath)
		if err != nil {
			return err
		}
		e.Path = blockedPath

		// Add to IPFS by component multihash
		dl.IPNSBlocksDB.Store(key, e)
		logger.Debugf("%s:%d: IPNS rule. Key: %s. Entry: %s", filepath.Base(dl.Filename), number, key, e)
	default:
		// Blocked by path only. We store non-prefix paths directly.
		// We store prefixed paths separately as every path request
		// will have to loop them.
		blockedPath, err := NewBlockedPath(rule)
		if err != nil {
			return err
		}
		e.Path = blockedPath

		key := rule
		if blockedPath.Prefix {
			dl.PathPrefixBlocks = append(dl.PathPrefixBlocks, e)
		} else {
			dl.PathBlocksDB.Store(key, e)
		}
		logger.Debugf("%s:%d: Path rule. Key: %s. Entry: %s", filepath.Base(dl.Filename), number, key, e)
	}

	dl.Entries = append(dl.Entries, e)
	return nil

}

// Close closes the Denylist file handle and stops watching write events on it.
func (dl *Denylist) Close() error {
	var err error
	if dl.watcher != nil {
		err = multierr.Append(err, dl.watcher.Close())
	}
	if dl.f != nil {
		err = multierr.Append(err, dl.f.Close())
	}

	return err
}

// IsSubpathBlocked returns Blocking Status for the given subpath.
func (dl *Denylist) IsSubpathBlocked(subpath string) StatusResponse {
	// all "/" prefix and suffix trimming is done in BlockedPath.Matches.
	// every rule has been ingested without slashes on the ends

	logger.Debugf("IsSubpathBlocked load path: %s", subpath)
	pathBlockEntries, _ := dl.PathBlocksDB.Load(subpath)
	status, entry := pathBlockEntries.CheckPathStatus(subpath)
	if status != StatusNotFound { // hit
		return StatusResponse{
			Status:   status,
			Filename: dl.Filename,
			Entry:    entry,
		}
	}
	// Check every prefix path.  Note: this is very innefficient, we
	// should have some HAMT that we can traverse with every character if
	// we were to support a large number of subpath-prefix blocks.
	status, entry = dl.PathPrefixBlocks.CheckPathStatus(subpath)
	return StatusResponse{
		Status:   status,
		Filename: dl.Filename,
		Entry:    entry,
	}
}

func toDNSLinkFQDN(label string) string {
	var result strings.Builder
	for i := 0; i < len(label); i++ {
		char := rune(label[i])
		nextChar := rune(0)
		if i < len(label)-1 {
			nextChar = rune(label[i+1])
		}

		if char == '-' && nextChar == '-' {
			result.WriteRune('-')
			i++
			continue
		}

		if char == '-' {
			result.WriteRune('.')
			continue
		}

		result.WriteRune(char)
	}
	return result.String()
}

func (dl *Denylist) checkDoubleHashWithFn(caller string, origKey string, code uint64) (Status, Entry, error) {
	blocksdb, ok := dl.DoubleHashBlocksDB[code]
	if !ok {
		return StatusNotFound, Entry{}, nil
	}
	// Double-hash the key
	doubleHash, err := multihash.Sum([]byte(origKey), code, -1)
	if err != nil {
		// Usually this means an unsupported hash function was
		// registered. We log and ignore.
		logger.Error(err)
		return StatusNotFound, Entry{}, nil
	}
	b58DoubleHash := doubleHash.B58String()
	logger.Debugf("%s load IPNS doublehash: %d %s", caller, code, b58DoubleHash)
	entries, _ := blocksdb.Load(b58DoubleHash)
	status, entry := entries.CheckPathStatus("") // double-hashes cannot have entry-subpaths
	return status, entry, nil
}

func (dl *Denylist) checkDoubleHash(caller string, origKey string) (Status, Entry, error) {
	for mhCode := range dl.DoubleHashBlocksDB {
		status, entry, err := dl.checkDoubleHashWithFn(caller, origKey, mhCode)
		if err != nil {
			return status, entry, err
		}
		if status != StatusNotFound { // hit!
			return status, entry, nil
		}
	}
	return StatusNotFound, Entry{}, nil

}

// IsIPNSPathBlocked returns Blocking Status for a given IPNS name and its
// subpath. The name is NOT an "/ipns/name" path, but just the name.
func (dl *Denylist) IsIPNSPathBlocked(name, subpath string) StatusResponse {
	subpath = strings.TrimPrefix(subpath, "/")

	var p path.Path
	var err error
	if len(subpath) > 0 {
		p, err = path.NewPath("/ipns/" + name + "/" + subpath)
	} else {
		p, err = path.NewPath("/ipns/" + name)
	}
	if err != nil {
		return StatusResponse{
			Status: StatusErrored,
			Error:  err,
		}
	}
	key := name
	// Check if it is a CID and use the multihash as key then
	c, err := cid.Decode(key)
	if err == nil {
		key = c.Hash().B58String()
		//
	} else if !strings.ContainsRune(key, '.') {
		// not a CID. It must be a ipns-dnslink name if it does not
		// contain ".", maybe they got replaced by "-"
		// https://specs.ipfs.tech/http-gateways/subdomain-gateway/#host-request-header
		key = toDNSLinkFQDN(key)
	}
	logger.Debugf("IsIPNSPathBlocked load: %s %s", key, subpath)
	entries, _ := dl.IPNSBlocksDB.Load(key)
	status, entry := entries.CheckPathStatus(subpath)
	if status != StatusNotFound { // hit!
		return StatusResponse{
			Path:     p,
			Status:   status,
			Filename: dl.Filename,
			Entry:    entry,
		}
	}

	// Double-hash blocking, works by double-hashing "/ipns/<name>/<path>"
	// Legacy double-hashes for dnslink will hash "domain.com/" (trailing
	// slash) or "<cidV1b32>/" for ipns-key blocking
	legacyKey := name + "/" + subpath
	if c.Defined() { // we parsed a CID before
		legacyCid, err := cid.NewCidV1(c.Prefix().Codec, c.Hash()).StringOfBase(multibase.Base32)
		if err != nil {
			return StatusResponse{
				Path:     p,
				Status:   StatusErrored,
				Filename: dl.Filename,
				Error:    err,
			}
		}
		legacyKey = legacyCid + "/" + subpath
	}
	status, entry, err = dl.checkDoubleHashWithFn("IsIPNSPathBlocked (legacy)", legacyKey, multihash.SHA2_256)
	if status != StatusNotFound { // hit or error
		return StatusResponse{
			Path:     p,
			Status:   status,
			Filename: dl.Filename,
			Entry:    entry,
			Error:    err,
		}
	}

	// Modern double-hash approach
	key = p.String()
	if c.Defined() { // the ipns path is a CID The
		// b58-encoded-multihash extracted from an IPNS name
		// when the IPNS is a CID.
		key = c.Hash().B58String()
		if len(subpath) > 0 {
			key += "/" + subpath
		}
	}

	status, entry, err = dl.checkDoubleHash("IsIPNSPathBlocked", key)
	return StatusResponse{
		Path:     p,
		Status:   status,
		Filename: dl.Filename,
		Entry:    entry,
		Error:    err,
	}
}

// IsIPFSPathBlocked returns Blocking Status for a given IPFS CID and its
// subpath. The cidStr is NOT an "/ipns/cid" path, but just the cid.
func (dl *Denylist) IsIPFSPathBlocked(cidStr, subpath string) StatusResponse {
	return dl.isIPFSIPLDPathBlocked(cidStr, subpath, "ipfs")
}

// IsIPLDPathBlocked returns Blocking Status for a given IPLD CID and its
// subpath. The cidStr is NOT an "/ipld/cid" path, but just the cid.
func (dl *Denylist) IsIPLDPathBlocked(cidStr, subpath string) StatusResponse {
	return dl.isIPFSIPLDPathBlocked(cidStr, subpath, "ipld")
}

func (dl *Denylist) isIPFSIPLDPathBlocked(cidStr, subpath, protocol string) StatusResponse {
	subpath = strings.TrimPrefix(subpath, "/")

	var p path.Path
	var err error
	if len(subpath) > 0 {
		p, err = path.NewPath("/" + protocol + "/" + cidStr + "/" + subpath)
	} else {
		p, err = path.NewPath("/" + protocol + "/" + cidStr)
	}

	if err != nil {
		return StatusResponse{
			Status: StatusErrored,
			Error:  err,
		}
	}

	key := cidStr

	// This could be a shortcut to let the work to the
	// blockservice.  Assuming IsCidBlocked() is going to be
	// called later down the stack (by IPFS).
	//
	// TODO: enable this with options.
	// if p.IsJustAKey() {
	// 	return false
	// }

	var c cid.Cid
	if len(key) != 46 || key[:2] != "Qm" {
		// Key is not a CIDv0, we need to convert other CIDs.
		// convert to Multihash (cidV0)
		c, err = cid.Decode(key)
		if err != nil {
			logger.Warnf("could not decode %s as CID: %s", key, err)
			return StatusResponse{
				Path:     p,
				Status:   StatusErrored,
				Filename: dl.Filename,
				Error:    err,
			}
		}
		key = c.Hash().B58String()
	}

	logger.Debugf("isIPFSIPLDPathBlocked load: %s %s", key, subpath)
	entries, _ := dl.IPFSBlocksDB.Load(key)
	status, entry := entries.CheckPathStatus(subpath)
	if status != StatusNotFound { // hit!
		return StatusResponse{
			Path:     p,
			Status:   status,
			Filename: dl.Filename,
			Entry:    entry,
		}
	}

	// Check for double-hashed entries. We need to lookup both the
	// multihash+path and the base32-cidv1 + path
	if !c.Defined() { // if we didn't decode before...
		c, err = cid.Decode(cidStr)
		if err != nil {
			logger.Warnf("could not decode %s as CID: %s", key, err)
			return StatusResponse{
				Path:     p,
				Status:   StatusErrored,
				Filename: dl.Filename,
				Error:    err,
			}
		}
	}

	prefix := c.Prefix()
	// Checks for legacy doublehash blocking
	// <cidv1base32>/<path>
	// TODO: we should be able to disable this part with an Option
	// or a hint for denylists not using it.
	v1b32, err := cid.NewCidV1(prefix.Codec, c.Hash()).StringOfBase(multibase.Base32) // base32 string
	if err != nil {
		return StatusResponse{
			Path:     p,
			Status:   StatusErrored,
			Filename: dl.Filename,
			Error:    err,
		}
	}
	// badbits appends / on empty subpath. and hashes that
	// https://specs.ipfs.tech/compact-denylist-format/#double-hash
	v1b32path := v1b32 + "/" + subpath
	status, entry, err = dl.checkDoubleHashWithFn("IsIPFSIPLDPathBlocked (legacy)", v1b32path, multihash.SHA2_256)
	if status != StatusNotFound { // hit or error
		return StatusResponse{
			Path:     p,
			Status:   status,
			Filename: dl.Filename,
			Entry:    entry,
			Error:    err,
		}
	}

	// Otherwise just check normal double-hashing of multihash
	// for all double-hashing functions used.
	// <cidv0>/<path>
	v0path := c.Hash().B58String()
	if subpath != "" {
		v0path += "/" + subpath
	}
	status, entry, err = dl.checkDoubleHash("IsIPFSIPLDPathBlocked", v0path)
	return StatusResponse{
		Path:     p,
		Status:   status,
		Filename: dl.Filename,
		Entry:    entry,
		Error:    err,
	}
}

// IsPathBlocked provides Blocking Status for a given path.  This is done by
// interpreting the full path and checking for blocked Path, IPFS, IPNS or
// double-hashed items matching it.
//
// Matching is more efficient if:
//
//   - Paths in the form of /ipfs/Qm/... (sha2-256-multihash) are used rather than CIDv1.
//
//   - A single double-hashing pattern is used.
//
//   - A small number of path-only match rules using prefixes are used.
func (dl *Denylist) IsPathBlocked(p path.Path) StatusResponse {
	segments := p.Segments()
	if len(segments) < 2 {
		return StatusResponse{
			Path:     p,
			Status:   StatusErrored,
			Filename: dl.Filename,
			Error:    errors.New("path is too short"),
		}
	}
	proto := segments[0]
	key := segments[1]
	subpath := strings.Join(segments[2:], "/")

	// First, check that we are not blocking this subpath in general
	if len(subpath) > 0 {
		if resp := dl.IsSubpathBlocked(subpath); resp.Status != StatusNotFound {
			resp.Path = p
			return resp
		}
	}

	// Second, check that we are not blocking ipfs or ipns paths
	// like this one.

	// ["ipfs", "<cid>", ...]

	switch proto {
	case "ipns":
		return dl.IsIPNSPathBlocked(key, subpath)
	case "ipfs":
		return dl.IsIPFSPathBlocked(key, subpath)
	case "ipld":
		return dl.IsIPLDPathBlocked(key, subpath)
	default:
		return StatusResponse{
			Path:     p,
			Status:   StatusNotFound,
			Filename: dl.Filename,
		}
	}
}

// IsCidBlocked provides Blocking Status for a given CID.  This is done by
// extracting the multihash and checking if it is blocked by any rule.
func (dl *Denylist) IsCidBlocked(c cid.Cid) StatusResponse {
	b58 := c.Hash().B58String()
	logger.Debugf("IsCidBlocked load: %s", b58)
	entries, _ := dl.IPFSBlocksDB.Load(b58)
	// Look for an entry with an empty path
	// which means the Mhash itself is blocked.
	status, entry := entries.CheckPathStatus("")
	if status != StatusNotFound { // Hit!
		return StatusResponse{
			Cid:      c,
			Status:   status,
			Filename: dl.Filename,
			Entry:    entry,
		}
	}

	// Now check if a double-hash covers this CID

	// Legacy double-hashing support.
	// convert cid to v1 base32
	// the double-hash using multhash sha2-256
	// then check that
	prefix := c.Prefix()
	b32, err := cid.NewCidV1(prefix.Codec, c.Hash()).StringOfBase(multibase.Base32)
	if err != nil {
		return StatusResponse{
			Cid:      c,
			Status:   StatusErrored,
			Filename: dl.Filename,
			Error:    err,
		}
	}
	b32 += "/" // yes, needed
	status, entry, err = dl.checkDoubleHashWithFn("IsCidBlocked (legacy)", b32, multihash.SHA2_256)
	if status != StatusNotFound { // hit or error
		return StatusResponse{
			Cid:      c,
			Status:   status,
			Filename: dl.Filename,
			Entry:    entry,
			Error:    err,
		}
	}

	// Otherwise, double-hash the multihash string.
	status, entry, err = dl.checkDoubleHash("IsCidBlocked", b58)
	return StatusResponse{
		Cid:      c,
		Status:   status,
		Filename: dl.Filename,
		Entry:    entry,
		Error:    err,
	}
}
