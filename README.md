# Distributed Hash Table (DHT) Implementation

## Overview
This project implements a **Distributed Hash Table (DHT)** using the **Chord protocol**. It supports **insert, query, and delete** operations across a distributed network of nodes. Additionally, the system supports **replication** with configurable consistency levels (**linearizability** and **eventual consistency**).

## Features
- **Chord-based DHT**: Implements a structured peer-to-peer lookup mechanism.
- **Replication**: Supports configurable replication (`k` replicas) for fault tolerance.
- **Consistency Models**:
  - **Linearizability**: Ensures strict consistency but may be slower.
  - **Eventual Consistency**: Faster writes but eventual correctness.
- **Node Join & Departure**: Dynamically add or remove nodes.
- **Parallel Execution**: Supports batch insert and query operations.

## System Components

### 1. **Bootstrap Node**
The bootstrap node initializes the Chord ring and helps new nodes join.

### 2. **DHT Nodes**
Each node stores key-value pairs and routes queries using the Chord algorithm.

### 3. **Client**
A CLI-based client for interacting with the DHT.

## Installation
### Prerequisites
Ensure you have:
- **Python 3.10+**
- **Bash** (for scripts)

### Clone the Repository
```sh
git clone <your-repo-url>
cd distributed_systems
```

## Usage

### 1. **Start the Bootstrap Node**
```sh
python3 server/bootstrap.py <bootstrap_ip> <bootstrap_port> <replication_factor> <consistency_model>
```

### 2. **Start DHT Nodes**
Run the following script to start nodes across multiple VMs:
```sh
python3 server/node.py <node_ip> <node_port> <replication_factor> <consistency_model> <bootstrap_ip> <bootstrap_port> 
```

### 3. **Insert Data**
To insert keys into the DHT:
```sh
./experiments/output/team_29/insert_songs.sh
```

### 4. **Query Data**
To query keys from the DHT:
```sh
./experiments/output/team_29/query_songs.sh
```

### 5. **Process Requests**
To execute batch requests from a file (insert/query operations):
```sh
./experiments/output/team_29/process_requests.sh
```

