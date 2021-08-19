// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/tidwall/redlog/v2"
)

const usage = `{{NAME}} version: {{VERSION}} ({{GITSHA}})

Usage: {{NAME}} [-n id] [-a addr] [options]

Basic options:
  -v               : display version
  -h               : display help, this screen
  -a addr          : bind to address  (default: 127.0.0.1:11001)
  -n id            : node ID  (default: 1)
  -d dir           : data directory  (default: data)
  -j addr          : leader address of a cluster to join
  -l level         : log level  (default: info) [debug,verb,info,warn,silent]

Security options:
  --tls-cert path  : path to TLS certificate
  --tls-key path   : path to TLS private key
  --auth auth      : cluster authorization, shared by all servers and clients

Networking options: 
  --advertise addr : advertise address  (default: network bound address)

Advanced options:
  --nosync         : turn off syncing data to disk after every write. This leads
                     to faster write operations but opens up the chance for data
                     loss due to catastrophic events such as power failure.
  --openreads      : allow followers to process read commands, but with the 
                     possibility of returning stale data.
  --localtime      : have the raft machine time synchronized with the local
                     server rather than the public internet. This will run the 
                     risk of time shifts when the local server time is
                     drastically changed during live operation. 
  --restore path   : restore a raft machine from a snapshot file. This will
                     start a brand new single-node cluster using the snapshot as
                     initial data. The other nodes must be re-joined. This
                     operation is ignored when a data directory already exists.
                     Cannot be used with -j flag.
`

func versline(conf Config) string {
	sha := ""
	if conf.GitSHA != "" {
		sha = " (" + conf.GitSHA + ")"
	}
	return fmt.Sprintf("%s version %s%s", conf.Name, conf.Version, sha)
}

func stateChangeFilter(line string, log *redlog.Logger) string {
	if strings.Contains(line, "entering ") {
		app := log.App()
		if strings.Contains(line, "entering candidate state") {
			app = 'C'
		} else if strings.Contains(line, "entering follower state") {
			app = 'F'
		} else if strings.Contains(line, "entering leader state") {
			app = 'L'
		} else {
			return line
		}
		log.SetApp(app)
	}
	return line
}

func rincr(seed int64) int64 {
	return int64(uint64(seed)*6364136223846793005 + 1)
}

func rgen(seed int64) uint32 {
	state := uint64(seed)
	xorshifted := uint32(((state >> 18) ^ state) >> 27)
	rot := uint32(state >> 59)
	return (xorshifted >> rot) | (xorshifted << ((-rot) & 31))
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	return append(dst, buf[:n]...)
}
