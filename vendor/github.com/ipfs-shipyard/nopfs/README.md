# NOpfs!

NOPfs helps IPFS to say No!

<p align="center">
<img src="logo.png" alt="ipfs-lite" title="ipfs-lite" />
</p>

NOPfs is an implementation of
[IPIP-383](https://github.com/ipfs/specs/pull/383) which add supports for
content blocking to the go-ipfs stack and particularly to Kubo.

## Content-blocking in Kubo

  1. Grab a plugin release from the [releases](https://github.com/ipfs-shipyard/nopfs/releases) section matching your Kubo version and install the plugin file in `~/.ipfs/plugins`.
  2. Write a custom denylist file (see [syntax](#denylist-syntax) below) or simply download one of the supported denylists from [Denyli.st](https://denyli.st) and place them in `~/.config/ipfs/denylists/` (ensure `.deny` extension).
  3. Start Kubo (`ipfs daemon`). The plugin should be loaded automatically and existing denylists tracked for updates from that point (no restarts required).

## Denylist syntax

Denylist files must have the `.deny` extension. The content consists of an optional header and a body made of blocking rules as follows:


```
version: 1
name: IPFSorp blocking list
description: A collection of bad things we have found in the universe
author: abuse-ipfscorp@example.com
hints:
  gateway_status: 410
  double_hash_fn: sha256
  double_hash_enc: hex
---
# Blocking by CID - blocks wrapped multihash.
# Does not block subpaths.
/ipfs/bafybeihvvulpp4evxj7x7armbqcyg6uezzuig6jp3lktpbovlqfkuqeuoq

# Block all subpaths
/ipfs/QmdWFA9FL52hx3j9EJZPQP1ZUH8Ygi5tLCX2cRDs6knSf8/*

# Block some subpaths (equivalent rules)
/ipfs/Qmah2YDTfrox4watLCr3YgKyBwvjq8FJZEFdWY6WtJ3Xt2/test*
/ipfs/QmTuvSQbEDR3sarFAN9kAeXBpiBCyYYNxdxciazBba11eC/test/*

# Block some subpaths with exceptions
/ipfs/QmUboz9UsQBDeS6Tug1U8jgoFkgYxyYood9NDyVURAY9pK/blocked*
+/ipfs/QmUboz9UsQBDeS6Tug1U8jgoFkgYxyYood9NDyVURAY9pK/blockednot
+/ipfs/QmUboz9UsQBDeS6Tug1U8jgoFkgYxyYood9NDyVURAY9pK/blocked/not
+/ipfs/QmUboz9UsQBDeS6Tug1U8jgoFkgYxyYood9NDyVURAY9pK/blocked/exceptions*

# Block IPNS domain name
/ipns/domain.example

# Block IPNS domain name and path
/ipns/domain2.example/path

# Block IPNS key - blocks wrapped multihash.
/ipns/k51qzi5uqu5dhmzyv3zac033i7rl9hkgczxyl81lwoukda2htteop7d3x0y1mf

# Legacy CID double-hash block
# sha256(bafybeiefwqslmf6zyyrxodaxx4vwqircuxpza5ri45ws3y5a62ypxti42e/)
# blocks only this CID
//d9d295bde21f422d471a90f2a37ec53049fdf3e5fa3ee2e8f20e10003da429e7

# Legacy Path double-hash block
# Blocks bafybeiefwqslmf6zyyrxodaxx4vwqircuxpza5ri45ws3y5a62ypxti42e/path
# but not any other paths.
//3f8b9febd851873b3774b937cce126910699ceac56e72e64b866f8e258d09572

# Double hash CID block
# base58btc-sha256-multihash(QmVTF1yEejXd9iMgoRTFDxBv7HAz9kuZcQNBzHrceuK9HR)
# Blocks bafybeidjwik6im54nrpfg7osdvmx7zojl5oaxqel5cmsz46iuelwf5acja
# and QmVTF1yEejXd9iMgoRTFDxBv7HAz9kuZcQNBzHrceuK9HR etc. by multihash
//QmX9dhRcQcKUw3Ws8485T5a9dtjrSCQaUAHnG4iK9i4ceM

# Double hash Path block using blake3 hashing
# base58btc-blake3-multihash(gW7Nhu4HrfDtphEivm3Z9NNE7gpdh5Tga8g6JNZc1S8E47/path)
# Blocks /ipfs/bafyb4ieqht3b2rssdmc7sjv2cy2gfdilxkfh7623nvndziyqnawkmo266a/path
# /ipfs/bafyb4ieqht3b2rssdmc7sjv2cy2gfdilxkfh7623nvndziyqnawkmo266a/path
# /ipfs/f01701e20903cf61d46521b05f926ba1634628d0bba8a7ffb5b6d5a3ca310682ca63b5ef0/path etc...
# But not /path2
//QmbK7LDv5NNBvYQzNfm2eED17SNLt1yNMapcUhSuNLgkqz
```

You can create double-hashes by hand with the following command:

```
printf "QmecDgNqCRirkc3Cjz9eoRBNwXGckJ9WvTdmY16HP88768/my/path" \
  | ipfs add --raw-leaves --only-hash --quiet \
  | ipfs cid format -f '%M' -b base58btc
```

where:
  - `QmecDgNqCRirkc3Cjz9eoRBNwXGckJ9WvTdmY16HP88768` must always be a
    CidV0. If you have a CIDv1 you need to convert it to CIDv0 first. i.e
	`ipfs cid format -v0 bafybeihrw75yfhdx5qsqgesdnxejtjybscwuclpusvxkuttep6h7pkgmze`
  - `/my/path` is optional depending on whether you want to block a specific path. No wildcards supported here!
  - The command above should give `QmSju6XPmYLG611rmK7rEeCMFVuL6EHpqyvmEU6oGx3GR8`. Use it as `//QmSju6XPmYLG611rmK7rEeCMFVuL6EHpqyvmEU6oGx3GR8` on the denylist.


## Kubo plugin

NOpfs Kubo plugin pre-built binary releases are available in the
[releases](https://github.com/ipfs-shipyard/nopfs/releases) section.

Simply grab the binary for your system and drop it in the `~/.ipfs/plugins` folder.

From that point, starting Kubo should load the plugin and automatically work with denylists (files with extension `.deny`) found in `/etc/ipfs/denylists` and `$XDG_CONFIG_HOME/ipfs/denylists` (usually `~/.config/ipfs/denylists`). The plugin will log some lines as the ipfs daemon starts:

```
$ ipfs daemon --offline
Initializing daemon...
Kubo version: 0.21.0-rc1
Repo version: 14
System version: amd64/linux
Golang version: go1.19.10
2023-06-13T21:26:56.951+0200	INFO	nopfs	nopfs-kubo-plugin/plugin.go:59	Loading Nopfs plugin: content blocking
2023-06-13T21:26:56.952+0200	INFO	nopfs	nopfs@v0.0.7/denylist.go:165	Processing /home/user/.config/ipfs/denylists/badbits.deny: badbits (2023-03-27) by @Protocol Labs
```

The plugin can be manually built and installed for different versions of Kubo with:

```
git checkout nopfs-kubo-plugin/v<kubo-version>
make plugin
make install-plugin
```

## Project layout

The NOpfs contains three separate Go-modules (versioned separately):

* The main module (`github.com/ipfs-shipyard/nopfs`) provides the implementation of a Blocker that works with IPIP-383 denylists (can parse, track and answer whether CIDs/paths are blocked)
* The ipfs submodule (`github.com/ipfs-shipyard/nopfs/ipfs`) provides blocking-wrappers for types in the Boxo/stack (Resolver, BlockService etc.). It's versioning tracks Boxo tags. i.e. v0.10.0 is compatible with boxo@v0.10.0.
* The nopfs-kubo-plugin submodule (`github.com/ipfs-shipyard/nopfs/nopfs-kubo-plugin`) contains only the code of the Kubo plugin, which injects blocking-wrappers into Kubo. It is tagged tracking Kubo releases.

This allows using the Blocker separately, or relying on blocking-wrappers separately in a way that it is easy to identify and select dependency-aligned versions with your project, without specifying more dependencies that needed.

## Project status

  - [x] Support for blocking CIDs
  - [x] Support for blocking IPFS Paths
  - [x] Support for paths with wildcards (prefix paths)
  - [x] Support for blocking legacy [badbits anchors](https://badbits.dwebops.pub/denylist.json)
  - [x] Support for blocking  double-hashed CIDs, IPFS paths, IPNS paths.
  - [x] Support for blocking prefix and non-prefix sub-path
  - [x] Support for denylist headers
  - [x] Support for denylist rule hints
  - [x] Support for allow rules (undo or create exceptions to earlier rules)
  - [x] Live processing of appended rules to denylists
  - [x] Content-blocking-enabled IPFS BlockService implementation
  - [x] Content-blocking-enabled IPFS NameSystem implementation
  - [x] Content-blocking-enabled IPFS Path resolver implementation
  - [x] Kubo plugin
  - [x] Automatic, comprehensive testing of all rule types and edge cases
  - [x] Work with a stable release of Kubo
  - [x] Prebuilt plugin binaries
