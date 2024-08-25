package store

import (
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
)

// ShouldPut returns true if the block should be put into the CAR according to the options provided
// and the index. It returns false if the block should not be put into the CAR, either because it
// is an identity block and StoreIdentityCIDs is false, or because it already exists and
// BlockstoreAllowDuplicatePuts is false.
func ShouldPut(
	idx *index.InsertionIndex,
	c cid.Cid,
	maxIndexCidSize uint64,
	storeIdentityCIDs bool,
	blockstoreAllowDuplicatePuts bool,
	blockstoreUseWholeCIDs bool,
) (bool, error) {

	// If StoreIdentityCIDs option is disabled then treat IDENTITY CIDs like IdStore.
	if !storeIdentityCIDs {
		// Check for IDENTITY CID. If IDENTITY, ignore and move to the next block.
		if _, ok, err := IsIdentity(c); err != nil {
			return false, err
		} else if ok {
			return false, nil
		}
	}

	// Check if its size is too big.
	// If larger than maximum allowed size, return error.
	// Note, we need to check this regardless of whether we have IDENTITY CID or not.
	// Since multhihash codes other than IDENTITY can result in large digests.
	cSize := uint64(len(c.Bytes()))
	if cSize > maxIndexCidSize {
		return false, &carv2.ErrCidTooLarge{MaxSize: maxIndexCidSize, CurrentSize: cSize}
	}

	if !blockstoreAllowDuplicatePuts {
		if blockstoreUseWholeCIDs {
			has, err := idx.HasExactCID(c)
			if err != nil {
				return false, err
			}
			return !has, nil // deduplicated by CID
		}
		if !blockstoreUseWholeCIDs {
			_, err := idx.Get(c)
			if err == nil {
				return false, nil // deduplicated by hash
			}
		}
	}

	return true, nil
}

// Has returns true if the block exists in in the store according to the various
// rules associated with the options. Similar to ShouldPut, but for the simpler
// Has() case.
func Has(
	idx *index.InsertionIndex,
	c cid.Cid,
	maxIndexCidSize uint64,
	storeIdentityCIDs bool,
	blockstoreAllowDuplicatePuts bool,
	blockstoreUseWholeCIDs bool,
) (bool, error) {

	// If StoreIdentityCIDs option is disabled then treat IDENTITY CIDs like IdStore.
	if !storeIdentityCIDs {
		if _, ok, err := IsIdentity(c); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}

	if blockstoreUseWholeCIDs {
		return idx.HasExactCID(c)
	}
	return idx.HasMultihash(c.Hash())
}
