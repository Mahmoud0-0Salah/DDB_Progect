# DDB_Progect

## Overview

DDB_Progect is a Go-based project that includes an HTML interface (`index2.html`). The project is structured with Go modules (`go.mod`, `go.sum`) and is organized into multiple branches: `master`, `slave1`, and `slave2`.

## Features

- Modular Design with Go
  - Clean project structure using Go modules for scalable and maintainable development.
- HTML Interface
  - A simple web interface (index2.html) that interacts with the Go backend (can be enhanced for UI/UX or data interaction).
- Master-Slave Architecture
  - Implements a distributed database system where a single master handles all write operations, and one or more slaves handle read operations. This architecture supports scalability and improved fault tolerance.
- CRUD Operations Across Nodes
  - Create / Update / Delete (CUD): All write operations are processed through the master node to ensure consistency.
  - Read: Read operations are handled by slave nodes, reducing load on the master and improving response time.
- Pending Operator Queue
  - If a slave node is shut down or becomes unreachable, the system:
      - Automatically detects the failure.
      - Flags the node as inactive.
      - Stores any pending replication or operation logs in a temporary "Pending Queue".
      - Once the slave is restored, it automatically syncs the pending operations before resuming traffic.
  - This ensures no data loss and smooth reintegration of failed nodes.
- Real-Time Replication with Failure Tolerance
  - Uses binary log replication from the master to ensure all changes are propagated to slaves in near real-time. If replication is interrupted, logs are retained until successful delivery.
- Health Monitoring and Auto Recovery
  - Includes a basic or external monitoring system that checks the health of each slave. Upon detection of downtime:
      - Alerts are triggered (optional).
      - Load balancing excludes the downed slave.
      - Re-includes the slave after successful sync and health check.
  - Load Balancing (Optional with HAProxy)
      - Read traffic is distributed across slaves using HAProxy or similar tools to achieve high availability and fault tolerance.
