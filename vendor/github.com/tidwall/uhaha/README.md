<p align="center">
	<img src="logo.png" border=0 width=500 alt="uhaha">
</p>
<p align="center">
<a href="https://godoc.org/github.com/tidwall/uhaha"><img src="https://img.shields.io/badge/api-reference-blue.svg?style=flat-square" alt="GoDoc"></a>
</p>

<p align="center">High Availabilty Framework for Happy Data</p>

Uhaha is a framework for building highly available Raft-based data applications in Go. 
This is basically an upgrade to the [Finn](https://github.com/tidwall/finn) project, but has an updated API, better security features (TLS and auth passwords), 
customizable services, deterministic time, recalculable random numbers, simpler snapshots, a smaller network footprint, and more.
Under the hood it utilizes [hashicorp/raft](https://github.com/hashicorp/raft), [tidwall/redcon](https://github.com/tidwall/redcon), and [syndtr/goleveldb](https://github.com/syndtr/goleveldb).

## Features

- Simple API for quickly creating a custom Raft-based application.
- Deterministic monotonic time that does not drift and stays in sync with the internet.
- APIs for building custom services such as HTTP and gRPC.
  Supports the Redis protocol by default, so most Redis client library will work with Uhaha.
- [TLS](#tls) and [Auth password](#auth-password) support.
- Multiple examples to help jumpstart integration, including
  a [Key-value DB](https://github.com/tidwall/uhaha/tree/master/examples/kvdb), 
  a [Timeseries DB](https://github.com/tidwall/uhaha/tree/master/examples/timeseries), 
  and a [Ticket Service](https://github.com/tidwall/uhaha/tree/master/examples/ticket).

## Example

Below a simple example of a service for monotonically increasing tickets. 

```go
package main

import "github.com/tidwall/uhaha"

type data struct {
	Ticket int64
}

func main() {
	// Set up a uhaha configuration
	var conf uhaha.Config
	
	// Give the application a name. All servers in the cluster should use the
	// same name.
	conf.Name = "ticket"
	
	// Set the initial data. This is state of the data when first server in the 
	// cluster starts for the first time ever.
	conf.InitialData = new(data)

	// Since we are not holding onto much data we can used the built-in JSON
	// snapshot system. You just need to make sure all the important fields in
	// the data are exportable (capitalized) to JSON. In this case there is
	// only the one field "Ticket".
	conf.UseJSONSnapshots = true
	
	// Add a command that will change the value of a Ticket. 
	conf.AddWriteCommand("ticket", cmdTICKET)

	// Finally, hand off all processing to uhaha.
	uhaha.Main(conf)
}

// TICKET
// help: returns a new ticket that has a value that is at least one greater
// than the previous TICKET call.
func cmdTICKET(m uhaha.Machine, args []string) (interface{}, error) {
	// The the current data from the machine
	data := m.Data().(*data)

	// Increment the ticket
	data.Ticket++

	// Return the new ticket to caller
	return data.Ticket, nil
}
```

### Building

Using the source file from the examples directory, we'll build an application
named "ticket"

```
go build -o ticket examples/ticket/main.go
```

### Running

It's ideal to have three, five, or seven nodes in your cluster.

Let's create the first node.

```
./ticket -n 1 -a :11001
```

This will create a node named 1 and bind the address to :11001

Now let's create two more nodes and add them to the cluster.

```
./ticket -n 2 -a :11002 -j :11001
./ticket -n 3 -a :11003 -j :11001
```

Now we have a fault-tolerant three node cluster up and running.

### Using

You can use any Redis compatible client, such as the redis-cli, telnet, 
or netcat.

I'll use the redis-cli in the example below.

Connect to the leader. This will probably be the first node you created.

```
redis-cli -p 11001
```

Send the server a TICKET command and receive the first ticket.

```
> TICKET
"1"
```

From here on every TICKET command will guarentee to generate a value larger
than the previous TICKET command.

```
> TICKET
"2"
> TICKET
"3"
> TICKET
"4"
> TICKET
"5"
```


## Built-in Commands

There are a number built-in commands for managing and monitor the cluster.

```sh
VERSION                                 # show the application version
MACHINE                                 # show information about the state machine
RAFT LEADER                             # show the address of the current raft leader
RAFT INFO [pattern]                     # show information about the raft server and cluster
RAFT SERVER LIST                        # show all servers in cluster
RAFT SERVER ADD id address              # add a server to cluster
RAFT SERVER REMOVE id                   # remove a server from the cluster
RAFT SNAPSHOT NOW                       # make a snapshot of the data
RAFT SNAPSHOT LIST                      # show a list of all snapshots on server
RAFT SNAPSHOT FILE id                   # show the file path of a snapshot on server
RAFT SNAPSHOT READ id [RANGE start end] # download all or part of a snapshot
```

And also some client commands.

```sh
QUIT                                    # close the client connection
PING                                    # ping the server
ECHO [message]                          # echo a message to the server
AUTH password                           # authenticate with a password
```

## Network and security considerations (TLS and Auth password)

By default a single Uhaha instance is bound to the local `127.0.0.1` IP address. Thus nothing outside that machine, including other servers in the cluster or machines on the same local network will be able communicate with this instance. 

### Network security

To open up the service you will need to provide an IP address that can be reached from the outside.
For example, let's say you want to set up three servers on a local `10.0.0.0` network.

On server 1:

```sh
./ticket -n 1 -a 10.0.0.1:11001
```

On server 2:

```sh
./ticket -n 2 -a 10.0.0.2:11001 -j 10.0.0.1:11001
```

On server 3:

```sh
./ticket -n 3 -a 10.0.0.3:11001 -j 10.0.0.1:11001
```

Now you have a Raft cluster running on three distinct servers in the same local network. This may be enough for applications that only require a [network security policy](https://en.wikipedia.org/wiki/Network_security). Basically any server on the local network can access the cluster.

### Auth password

If you want to lock down the cluster further you can provide a secret auth, which is more or less a password that the cluster and client will need to communicate with each other.

```sh
./ticket -n 1 -a 10.0.0.1:11001 --auth my-secret
```

All the servers will need to be started with the same auth.

```sh
./ticket -n 2 -a 10.0.0.2:11001 --auth my-secret -j 10.0.0.1:11001
```

```sh
./ticket -n 2 -a 10.0.0.3:11001 --auth my-secret -j 10.0.0.1:11001
```

The client will also need the same auth to talk with cluster. All redis clients support an auth password, such as:

```sh
redis-cli -h 10.0.0.1 -p 11001 -a my-secret
```

This may be enough if you keep all your machines on the same private network, but you don't want all machines or applications to have unfettered access to the cluster.

### TLS

Finally you can use TLS, which I recommend along with an auth password.

In this example a custom cert and key are created using the [`mkcert`](https://github.com/FiloSottile/mkcert) tool.

```sh
mkcert uhaha-example
# produces uhaha-example.pem, uhaha-example-key.pem, and a rootCA.pem
```

Then create a cluster using the cert & key files. Along with an auth.

```sh
./ticket -n 1 -a 10.0.0.1:11001 --tls-cert uhaha-example.pem --tls-key uhaha-example-key.pem --auth my-secret
```

```sh
./ticket -n 2 -a 10.0.0.2:11001 --tls-cert uhaha-example.pem --tls-key uhaha-example-key.pem --auth my-secret -j 10.0.0.1:11001
```

```sh
./ticket -n 2 -a 10.0.0.3:11001 --tls-cert uhaha-example.pem --tls-key uhaha-example-key.pem --auth my-secret -j 10.0.0.1:11001
```

Now you can connect to the server from a client that has the `rootCA.pem`. 
You can find the location of your rootCA.pem file in the running `ls "$(mkcert -CAROOT)/rootCA.pem"`.

```sh
redis-cli -h 10.0.0.1 -p 11001 --tls --cacert rootCA.pem -a my-secret
```

## Command-line options

Below are all of the command line options.

```
Usage: my-uhaha-app [-n id] [-a addr] [options]

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
```





