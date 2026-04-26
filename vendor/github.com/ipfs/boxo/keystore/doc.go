// Package keystore provides a key management interface and a filesystem-backed
// implementation for storing cryptographic private keys.
//
// [FSKeystore] stores keys as individual files in a directory, with names
// encoded using base32. Keys are written with mode 0400 and the keystore
// directory is created with mode 0700.
package keystore
