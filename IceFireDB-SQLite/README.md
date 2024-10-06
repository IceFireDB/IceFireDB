# IceFireDB-SQLite

IceFireDB SQLite is a decentralized SQLite database designed to facilitate the construction of a global distributed database system. It allows users to write data to IceFireDB using the MySQL protocol. The data is stored in an SQLite database and automatically synchronized among nodes through P2P networking.

## Overview

IceFireDB SQLite combines the simplicity of SQLite with the power of decentralized P2P networking, enabling seamless data synchronization across multiple nodes. This setup is ideal for applications requiring high availability, fault tolerance, and global data distribution.

## How It Works

![Framework](./docs/icefiredb-sqlite.png)

The system operates by leveraging P2P networking to synchronize data across multiple SQLite instances. Each node in the network can act as both a client and a server, ensuring that data is replicated and available across the entire network.

## Getting Started

### Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/IceFireDB/IceFireDB.git
   ```

2. **Build the Project**

   ```shell
   cd IceFireDB-SQLite
   make
   IceFireDB-SQLite -h
   ```

### Configuration

IceFireDB SQLite can be configured using a YAML file to tailor its behavior to your needs. Below is an example configuration file with detailed explanations for each parameter:

```yaml
server:
  addr: ":23306" # Network listening address, supports MySQL protocol

sqlite:
  filename: "db/sqlite.db" # Path to the SQLite database file

debug:  # Control to open pprof
  enable: false # Enable or disable pprof profiling
  port: 17878 # Port for pprof profiling

# P2P configuration
p2p:
  enable: true # Enable or disable P2P networking
  service_discovery_id: "p2p_sqlite_service" # Unique identifier for service discovery
  service_command_topic: "p2p_sqlite_service_topic" # Topic for service commands
  service_discover_mode: "advertise" # Mode for service discovery: advertise or announce
  node_host_ip: "127.0.0.1" # Can be configured as a public IP
  node_host_port: 0 # Port for the node, 0 means any available port

# Tenant list
userlist:
  - user: root # Username for the tenant
    password: 123456 # Password for the tenant
```

### Application Scenarios

1. **Decentralized SQLite Database**: Build a decentralized SQLite database using the MySQL usage protocol, suitable for applications requiring distributed data storage and synchronization.

## Feature Support

- [✔️] **P2P Support**: Enabled for decentralized data synchronization.
- [✔️] **mariadb-client Client Support**: Allows interaction via mariadb-client interface.
- [✔️] **Golang Gorm Support**: Integration with Gorm for seamless database operations in Go applications.

## Demo Video

For a visual demonstration of IceFireDB SQLite in action, please watch the [demo video](https://user-images.githubusercontent.com/21053373/173170247-74b1daeb-7bd5-4dc0-8b93-62b334859ba8.mp4).
