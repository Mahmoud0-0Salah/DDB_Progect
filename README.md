# DDB_Project: Distributed Database System

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Language](https://img.shields.io/badge/language-Go-blue)
![License](https://img.shields.io/badge/license-MIT-green)

## 📋 Overview

DDB_Project is a robust distributed database implementation written in Go. It features a master-slave architecture designed for high availability, fault tolerance, and efficient data distribution. The system maintains data consistency through coordinated write operations via the master node while optimizing read performance by distributing queries across multiple slave nodes.

## 🏗️ Architecture

The project implements a classic master-slave distributed database architecture:

- **Master Node**: Central authority for all write operations
- **Slave Nodes**: Distributed read-only replicas (`slave1`, `slave2`)
- **Web Interface**: Simple HTML frontend (`index2.html`) for system interaction

## ✨ Key Features

### 🔄 Master-Slave Replication
- Single master handles all write operations (CREATE, UPDATE, DELETE)
- Multiple slaves process read operations for improved performance
- Binary log replication ensures data consistency across all nodes

### 🛡️ Fault Tolerance
- Automatic failure detection for offline nodes
- Pending operator queue stores operations for unavailable slaves
- Automatic synchronization when nodes rejoin the system

### 📊 System Monitoring and Recovery
- Health monitoring for all database nodes
- Automatic node exclusion during downtime
- Seamless node recovery and reintegration

### ⚖️ Load Distribution
- Read operations distributed across available slave nodes
- Optional HAProxy integration for enhanced load balancing
- Improved response times through distributed query processing

### 🔧 Technical Implementation
- Go modules for clean dependency management
- Modular codebase for easy maintenance and extension
- Simple web interface for system monitoring and control

## 🚀 Getting Started

### Prerequisites
- Go 1.18 or higher
- Git

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Mahmoud0-0Salah/DDB_Progect.git
   cd DDB_Progect
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

3. Run the master node:
   ```bash
   go run cmd/master/main.go
   ```

4. Run slave nodes:
   ```bash
   # In separate terminal windows
   go run cmd/slave/main.go --id=1
   go run cmd/slave/main.go --id=2
   ```

5. Access the web interface:
   Open `index2.html` in your browser or navigate to `http://localhost:8080` (default port)

## 🔍 System Details

### Replication Process
1. Write operations are sent to the master node
2. Master executes operations and records changes in binary logs
3. Changes are propagated to all active slave nodes
4. Slaves apply changes to maintain data consistency

### Failure Handling
1. System continuously monitors node health
2. If a slave becomes unavailable:
   - Node is marked as inactive
   - Operations are queued in the pending operator queue
   - Master continues serving requests
3. When slave reconnects:
   - Pending operations are applied sequentially
   - Node is reintegrated into the system

## 📈 Future Improvements

- Enhanced security features and authentication
- Improved web interface with real-time monitoring
- Support for complex query optimization
- Clustering capabilities for master nodes



---

*This distributed database project demonstrates key concepts in distributed systems including replication, fault tolerance, and load balancing using Go.*
