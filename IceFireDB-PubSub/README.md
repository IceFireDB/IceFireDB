# IceFireDB-PubSub

IceFireDB-PubSub is a high performance, high availability and decentralized subscription system.

It can seamlessly migrate web2 applications using redis publish and subscribe into a decentralized p2p subscription network.

## How does it work?

### scene introduction
![image](https://user-images.githubusercontent.com/34047788/179438925-16fce624-4711-4f5d-9207-1e80397e3b55.png)

### system structure
![image](https://user-images.githubusercontent.com/34047788/179438955-31a05dd8-e33d-42f2-a0e2-867f5e566251.png)

The application works with multiple nodes on the same network or on different networks. Nodes behind a NAT on a private network can communicate with each other. Peer discovery and routing using Kademlia DHT and IPFs network discovery. By supporting the redis pub-sub protocol, a globally distributed Web3 publish-subscribe system is constructed.

You can use it just like redis publish subscribe function.

## Getting Started

### Configuration

In the config directory, the user stores the project configuration file, the file name: config.yaml, which can be modified according to their own needs

```yaml
# Project configuration
proxy:
  local_port: 16379
  enable_mtls: false

# p2p config
p2p:
  enable: true

...
```

### Quickstart

https://user-images.githubusercontent.com/52234994/173171008-8c73ce17-4ba7-42ec-8257-025e98d2e647.mp4

Run the binary file directly, if you need to run in the background, you can add it to the systemd system management

```shell
$ make
$ ./bin/IceFireDB-PubSub -c ./config/config.yaml
```

### Usage
IceFireDB-PubSub is mainly used for two commands: SUBSCRIBE and PUBLISH, which are mainly implemented in[pubsub](./pkg/router/redisNode/ppubsub.go)

Secondary development can be carried out according to requirements, or other instructions can be added.

SUBSCRIBE
```shell
$ redis-cli
127.0.0.1:16379> SUBSCRIBE name
...
...
```
PUBLISH
```shell
$ redis-cli
127.0.0.1:16379> PUBLISH name hello
...
...
```

## License
Icefiredb proxy uses the Apache 2.0 license. See [LICENSE](.LICENSE) for details.

## Disclaimers
When you use this software, you have agreed and stated that the author, maintainer and contributor of this software are not responsible for any risks, costs or problems you encounter. If you find a software defect or BUG, ​​please submit a patch to help improve it!
