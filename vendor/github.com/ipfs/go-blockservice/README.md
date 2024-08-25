go-blockservice
==================

> go-blockservice provides a seamless interface to both local and remote storage backends.

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)
[![](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23ipfs)
[![Coverage Status](https://codecov.io/gh/ipfs/go-block-format/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-block-format/branch/master)
[![Build Status](https://circleci.com/gh/ipfs/go-blockservice.svg?style=svg)](https://circleci.com/gh/ipfs/go-blockservice)

## ❗ This repo is no longer maintained.
👉 We highly recommend switching to the maintained version at https://github.com/ipfs/boxo/tree/main/blockservice.
🏎️ Good news!  There is [tooling and documentation](https://github.com/ipfs/boxo#migrating-to-boxo) to expedite a switch in your repo. 

⚠️ If you continue using this repo, please note that security fixes will not be provided (unless someone steps in to maintain it).

📚 Learn more, including how to take the maintainership mantle or ask questions, [here](https://github.com/ipfs/boxo/wiki/Copied-or-Migrated-Repos-FAQ).

## Table of Contents

- [TODO](#todo)
- [License](#license)

## TODO

The interfaces here really would like to be merged with the blockstore interfaces.
The 'dagservice' constructor currently takes a blockservice, but it would be really nice
if it could just take a blockstore, and have this package implement a blockstore.

## License

MIT © Juan Batiz-Benet
