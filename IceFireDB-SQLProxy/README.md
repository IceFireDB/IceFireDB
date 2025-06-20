# IceFireDB-SQLProxy

IceFireDB-SQLProxy is a powerful middleware that facilitates the decentralization of traditional SQL database data. It provides a seamless mechanism to construct a globally distributed storage system with automatic networking capabilities. Commands are automatically synchronized across SQL agents within the network, and the IceFireDB SQL agent writes data directly to MySQL storage.

By leveraging the decentralized middleware network proxy, IceFireDB-SQLProxy enables decentralized data synchronization for MySQL databases, which are commonly used in web2 applications. This makes it an ideal solution for bridging the gap between traditional web2 applications and the emerging web3 architecture.

## Key Features

- **Decentralized Data Storage**: Distribute your MySQL data across multiple nodes, ensuring high availability and fault tolerance.
- **Automatic Networking**: Automatically connect and synchronize SQL agents within the network.
- **Web2 to Web3 Transition**: Facilitate the migration of traditional web2 applications to a decentralized web3 architecture.
- **Easy Integration**: Seamlessly integrate with existing MySQL databases without requiring extensive modifications.

## How It Works

![Framework](./docs/icefiredb-sqlproxy.png)

The IceFireDB-SQLProxy operates by creating a decentralized network of SQL agents that communicate and synchronize data in real-time. Each agent is responsible for writing data to a local MySQL instance, ensuring that the data is replicated across the network. This decentralized approach ensures that your data remains accessible and consistent even in the face of network partitions or node failures.

## Getting Started

### Prerequisites

- Go (version 1.19 or later)
- MySQL (version 5.6 or later)

### Installation

1. Clone the repository:
   ```shell
   git clone https://github.com/IceFireDB/IceFireDB.git
   cd IceFireDB-SQLProxy
   ```

2. Compile the project:
   ```shell
   make
   ```

3. Run the proxy:
   ```shell
   ./IceFireDB-SQLProxy -h
   ```

### Configuration

Before running the proxy, ensure that you have configured the necessary MySQL connection settings in the configuration file (`config.yaml`).

```yaml
server:
  addr: ":33306" # The port on which the proxy listens, mysql-server, supports direct connection of mysql-client

debug:  # Control to enable debug mode
  enable: true
  port: 17878

# mysql configurations for different access levels
mysql:
  admin:
    addr: "127.0.0.1:3306"
    user: "admin_user"
    password: "admin_pass"
    dbname: "exampledb"
    minAlive: 1 # Specifies the minimum number of open connections the pool will attempt to maintain
    maxAlive: 64 # Specifies the maximum number of open connections the pool will attempt to maintain
    maxIdle: 4 # Maximum number of idle connections
  readonly:
    addr: "127.0.0.1:3306"
    user: "readonly_user"
    password: "readonly_pass"
    dbname: "exampledb"
    minAlive: 1 # Specifies the minimum number of open connections the pool will attempt to maintain
    maxAlive: 64 # Specifies the maximum number of open connections the pool will attempt to maintain
    maxIdle: 4 # Maximum number of idle connections

# Tenant list
userlist:
  - user: host1
    password: host1

# p2p config for different access levels
p2p:
  enable: true
  serviceDiscoveryId: "tanovo_sqlproxy_admin_service"
  serviceCommandTopic: "tanovo_sqlproxy_command_topic"
  adminTopic: "tanovo_sqlproxy_admin_topic"
  readonlyTopic: "tanovo_sqlproxy_readonly_topic"
  serviceDiscoverMode: "advertise" # advertise or announce
  nodeHostIp: "127.0.0.1" # local ipv4 ip
  nodeHostPort: 0 # any port

```

### Demo

For a quick demonstration of how IceFireDB-SQLProxy works, check out our [demo video](https://user-images.githubusercontent.com/21053373/173170210-df2d1539-acc1-4d93-8695-cc0ddc5d723b.mp4).

## Application Scenarios

- **Decentralized MySQL Storage**: Build a decentralized MySQL storage system that can scale horizontally and withstand node failures.
- **Web2 to Web3 Transition**: Help traditional web2 applications transition to a web3 architecture by enabling decentralized data storage and synchronization.
- **High Availability and Fault Tolerance**: Ensure that your data remains accessible and consistent even in the face of network disruptions or node failures.

### Disclaimers
By using this software, you agree that the author, maintainer, and contributors are not responsible for any risks, costs, or problems you may encounter. If you find a software defect or bug, please submit a patch to help improve it!