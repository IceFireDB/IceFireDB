# IceFireDB-PubSub

IceFireDB-PubSub is a high-performance, highly available, and decentralized subscription system designed to seamlessly migrate web2 applications using Redis's publish and subscribe functionality into a decentralized peer-to-peer (p2p) subscription network.

## Key Features

- **High Performance**: Optimized for speed and efficiency.
- **High Availability**: Ensures continuous operation with decentralized architecture.
- **Decentralized**: Built on a peer-to-peer network for enhanced resilience and scalability.
- **Seamless Migration**: Supports Redis pub-sub protocol, making it easy to transition from traditional centralized systems.

## How It Works

### Scene Introduction
![Scene Introduction](https://user-images.githubusercontent.com/34047788/179438925-16fce624-4711-4f5d-9207-1e80397e3b55.png)

### System Structure
![System Structure](https://user-images.githubusercontent.com/34047788/179438955-31a05dd8-e33d-42f2-a0e2-867f5e566251.png)

IceFireDB-PubSub operates with multiple nodes across various networks. Nodes behind NATs on private networks can communicate with each other. Peer discovery and routing are facilitated using Kademlia DHT and IPFS network discovery. By supporting the Redis pub-sub protocol, it constructs a globally distributed Web3 publish-subscribe system.

You can use it just like the Redis publish-subscribe function.

## Getting Started

### Configuration

In the `config` directory, you'll find the project configuration file named `config.yaml`. Modify it according to your needs.

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

Watch the quickstart video [here](https://user-images.githubusercontent.com/52234994/173171008-8c73ce17-4ba7-42ec-8257-025e98d2e647.mp4).

Run the binary file directly. For background execution, consider adding it to the systemd system management.

```shell
$ make
$ ./bin/IceFireDB-PubSub -c ./config/config.yaml
```

### Usage

IceFireDB-PubSub primarily supports two commands: `SUBSCRIBE` and `PUBLISH`, implemented in [`pubsub`](./pkg/router/redisNode/ppubsub.go).

#### SUBSCRIBE
```shell
$ redis-cli
127.0.0.1:16379> SUBSCRIBE name
...
...
```

#### PUBLISH
```shell
$ redis-cli
127.0.0.1:16379> PUBLISH name hello
```

## License

IceFireDB-PubSub is licensed under the MIT license. See [LICENSE](LICENSE) for details.

### Disclaimers

By using this software, you agree that the author, maintainer, and contributors are not responsible for any risks, costs, or issues you may encounter. If you find a software defect or bug, please submit a patch to help improve it!
