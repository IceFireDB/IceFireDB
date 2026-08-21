package path

import "strings"

// NewPathFromURI is like [NewPath] but also accepts a native IPFS URI, rewriting
// it to the equivalent canonical content path before parsing:
//
//	ipfs://{cid}/sub  -> /ipfs/{cid}/sub
//	ipns://{name}/sub -> /ipns/{name}/sub
//	ipld://{cid}/sub  -> /ipld/{cid}/sub
//
// The schemeless ipfs:{cid}, ipns:{name}, and ipld:{cid} forms are accepted too.
// The scheme is matched case-insensitively over ASCII, as URI schemes are
// case-insensitive (RFC 3986). Everything after the scheme is preserved
// byte-for-byte, so a case-sensitive CIDv0 root or a DNSLink name is not altered.
// A string that is already a content path, or is not an IPFS URI, is handed to
// [NewPath] unchanged.
//
// Use this only at input boundaries where values may be copied from a browser or
// another tool. [NewPath] stays strict, so contexts that must accept canonical
// content paths only (such as DNSLink records) are not loosened by this helper.
func NewPathFromURI(str string) (Path, error) {
	return NewPath(normalizeURIScheme(str))
}

// normalizeURIScheme rewrites a native IPFS URI into a canonical content path.
// A string that is not an IPFS URI is returned unchanged. See [NewPathFromURI].
func normalizeURIScheme(str string) string {
	for _, ns := range [...]string{IPFSNamespace, IPNSNamespace, IPLDNamespace} {
		if hasURIScheme(str, ns) {
			rest := strings.TrimPrefix(str[len(ns)+1:], "//") // drop the optional "//" authority separator
			return "/" + ns + "/" + rest
		}
	}
	return str
}

// hasURIScheme reports whether str begins with the scheme ns followed by ":"
// (e.g. "ipfs:"). ns is matched case-insensitively over ASCII.
func hasURIScheme(str, ns string) bool {
	if len(str) <= len(ns) || str[len(ns)] != ':' {
		return false
	}
	for i := range len(ns) {
		if toLowerASCII(str[i]) != ns[i] {
			return false
		}
	}
	return true
}

func toLowerASCII(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}
