# Distributed Banking Platform

A distributed banking system developed as part of Distributed Systems – Assignment 2. The project demonstrates data sharding, application replication, shard-aware request routing, load balancing, concurrent transaction handling, and distributed transactions using the Two-Phase Commit (2PC) protocol.

The system is deployed using Docker Compose and consists of two independent banking shards, each with its own PostgreSQL database and two application replicas.

## Overview

The main objective of this project is to implement a distributed banking platform that supports multiple application replicas and independent database shards while maintaining transaction consistency.

The system provides the following banking operations:

- Check account balance
- Deposit money
- Withdraw money
- Transfer money between accounts

Clients communicate exclusively with the Gateway and do not need to know which shard or application replica handles their requests.

## Architecture

The platform consists of the following components:

| Component | Responsibility |
|---|---|
| Banking Client | Provides the interface for banking operations and communicates with the Gateway. |
| Gateway | Determines account ownership, routes requests to the appropriate shard, and selects a healthy application replica. |
| Transaction Coordinator | Coordinates cross-shard transactions using the Two-Phase Commit protocol. |
| Bank A1 and A2 | Application replicas responsible for accounts in Shard A. |
| Bank B1 and B2 | Application replicas responsible for accounts in Shard B. |
| Database A | Persistent PostgreSQL database for Shard A. |
| Database B | Persistent PostgreSQL database for Shard B. |
| Coordinator Database | Stores transaction states and durable coordination decisions. |

System architecture

```text
                         Banking Client
                               |
                               v
                            Gateway
                               |
                 +-------------+-------------+
                 |                           |
              Shard A                     Shard B
                 |                           |
           +-----+-----+               +-----+-----+
           |           |               |           |
         Bank A1    Bank A2          Bank B1    Bank B2
           |           |               |           |
           +-----+-----+               +-----+-----+
                 |                           |
             Database A                  Database B

                            Gateway
                               |
                               v
                    Transaction Coordinator
                               |
                               v
                     Coordinator Database
```

Each shard has two application replicas that share the same shard database. The replicas do not maintain independent authoritative account balances.

Technologies

Python – Application and transaction logic
Docker – Containerization
Docker Compose – Multi-container deployment and orchestration
PostgreSQL – Persistent storage for banking accounts and transaction coordination
HTTP/REST APIs – Communication between system components
Two-Phase Commit (2PC) – Distributed transaction coordination

Features

1. Data Sharding

The system divides banking accounts into two independent shards based on predefined account ranges.

Each account belongs to exactly one authoritative shard.

2. Application Replication

Each shard has two application replicas:

Shard A: Bank A1 and Bank A2
Shard B: Bank B1 and Bank B2

Replicas within the same shard share a PostgreSQL database.

3. Shard-aware Gateway

The Gateway performs two routing steps:

Determines the shard responsible for the requested account.
Selects a healthy application replica within that shard.

This ensures that requests are routed to the correct shard before load balancing takes place.

4. Replica Load Balancing

The Gateway uses a Round Robin strategy to distribute requests among healthy replicas within the selected shard.

5. Health Checking and Failover

The Gateway detects unavailable replicas and routes new requests to healthy replicas belonging to the same shard.

6. Local Transactions

Transfers between accounts belonging to the same shard are handled using a single local database transaction.

Both account updates succeed together or neither is applied.

7. Distributed Transactions

Transfers between accounts belonging to different shards are coordinated using the Two-Phase Commit protocol.

8. Durable Transaction Recovery

The Transaction Coordinator stores transaction states and decisions persistently. After a restart, it recovers unresolved transactions according to their durable decisions.

Sharding Strategy

The system uses deterministic range-based sharding.

| Shard | Account Range | Database | Application Replicas |
|---|---|---|---|
| Shard A | A0001–A1000 | db-a | Bank A1, Bank A2 |
| Shard B | A1001–A2000 | db-b | Bank B1, Bank B2 |

The shard mapping is defined in:

`config/shard-map.json`

The Gateway uses this configuration to determine the authoritative shard for each account.

Load Balancing

After identifying the correct shard, the Gateway selects an available replica using Round Robin.

For example, requests involving accounts in Shard A are distributed as follows:

```text

Request 1  -> Bank A1

Request 2  -> Bank A2

Request 3  -> Bank A1

Request 4  -> Bank A2

Request 5  -> Bank A1

Request 6  -> Bank A2

```

Load balancing is performed independently within each shard.

Distributed Transactions

The system uses the Two-Phase Commit (2PC) protocol for transfers involving accounts in different shards.

Phase 1: Prepare

The Coordinator generates a unique transaction ID.
It sends PREPARE requests to both participating shards.
Each participant validates the transaction and prepares its changes.
Each participant returns a YES or NO vote.

Phase 2: Commit or Abort

If both participants vote YES, the Coordinator durably records a COMMIT decision and sends COMMIT requests.
If any participant votes NO, the Coordinator records an ABORT decision and sends ABORT requests.

The transaction is reported as COMPLETED after the participants confirm the final decision.

Transaction States

The Coordinator maintains persistent transaction states, including:

INITIATED
PREPARING
PREPARED
COMMIT_DECIDED
ABORT_DECIDED
COMPLETED

These states support transaction tracking and recovery after Coordinator failures.

Concurrency Control

The platform supports concurrent banking operations through multiple application replicas.

Concurrency tests were performed to verify that simultaneous deposits, withdrawals, and cross-shard transfers produce the expected account balances.

The application replicas share their shard's database, ensuring that account balances are maintained in a common persistent data store rather than independently by each replica.

Project Deployment

Prerequisites

Make sure the following software is installed:

Docker Desktop
Docker Compose
Python (for running the client and test scripts)

1. Clone the repository

```bash

git clone <YOUR_GITHUB_REPOSITORY_URL>

```

Navigate to the project directory:

```bash

cd distributed-banking-platform

```

2. Start the application

Build and start the containers:

```bash

docker compose up -d --build

```

3. Check running containers

```bash

docker compose ps

```

4. View application logs

To view the Gateway logs:

```bash

docker compose logs gateway

```

To view the application replica logs:

```bash

docker compose logs bank-a1 bank-a2 bank-b1 bank-b2

```

To view the Coordinator logs:

```bash

docker compose logs coordinator

```

5. Run the Banking Client

Run the client from the project directory:

```bash

python client.py

```

The client provides access to the supported banking operations through the Gateway.

Environment-file note: Keep local secrets and credentials in `.env` out of version control. If the project needs to document required variables, provide a separate `.env.example` containing placeholder values only.
6. Stop the application

```bash

docker compose down

```

Named Docker volumes are retained when using this command. To remove the volumes and their persistent data as well, use:

```bash

docker compose down -v

```

Warning: The second command deletes the persistent database volumes and their stored data.

Testing and Experiments

The implementation was evaluated through seven experiments designed to verify the main distributed-system requirements.

| Experiment | Description | Result |
|---|---|---|
| 1. Shard Distribution and Gateway Routing | Verified account ownership, database distribution, and Gateway routing using boundary accounts. | Passed |
| 2. Replica Request Distribution | Verified Round Robin request distribution between replicas of the same shard. | Passed |
| 3. Concurrent Deposits and Withdrawals | Executed 20 deposits and 20 withdrawals concurrently using 10 workers. | Passed |
| 4. Replica Interruption and Failover | Stopped one replica during request processing and verified that requests continued through the healthy replica. | Passed |
| 5. Local and Cross-shard Transfers | Verified atomic local transfers and cross-shard transfers using 2PC. | Passed |
| 6. Interrupted Distributed Transaction | Interrupted the Coordinator after a durable COMMIT decision and verified recovery after restart. | Passed |
| 7. Concurrent Cross-shard Transactions | Executed eight concurrent cross-shard transfers involving overlapping accounts. | Passed |

Selected Test Results

Concurrent deposits and withdrawals

20 successful deposits
20 successful withdrawals
40 total requests
10 concurrent workers
0 failed requests
Initial balance: 469.00
Final balance: 469.00

Replica failover

220 balance inquiries
One replica stopped during the test
Remaining healthy replica continued processing requests
All requests returned HTTP 200
No reported failures

Concurrent cross-shard transfers

8 concurrent transactions
All 8 transactions completed successfully
Both participants voted YES for every transaction
No prepared transactions remained
Combined account balance remained 4600.00

Recovery Scenarios

In addition to the seven main experiments, three recovery scenarios were tested to evaluate the behavior of the Transaction Coordinator.

Scenario A: Participant Votes NO

One shard rejected a transaction due to insufficient account balance, while the other participant had successfully prepared.

The Coordinator selected ABORT and instructed the prepared participant to roll back its transaction.

Result: The transaction was safely aborted, and neither account balance was changed.

Scenario B: Coordinator Failure Before Final Decision

Both participants successfully prepared the transaction, but the Coordinator was interrupted before recording its final decision.

After restarting, the Coordinator recovered the unresolved transaction and selected ABORT.

Result: The transaction was resolved, and the account balances remained unchanged.

Scenario C: Coordinator Failure After Durable COMMIT

The Coordinator was interrupted after recording a durable COMMIT decision but before receiving all participant acknowledgements.

After restarting, it recovered the recorded decision and continued sending COMMIT requests.

Result: The transaction reached COMPLETED, and the Coordinator did not replace the durable COMMIT decision with ABORT.

---

Project Structure

The main project components are organized into the following directories and files:

```text

distributed-banking-platform/
│
├── banking-shard/
│   ├── app/
│   │   ├── database.py
│   │   ├── main.py
│   │   ├── operations.py
│   │   └── __init__.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── config/
│   └── shard-map.json
│
├── coordinator/
│   ├── app.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── schema.sql
│
├── database/
│   ├── shard-a.sql
│   └── shard-b.sql
│
├── docs/
│   └── architecture.md
│
├── gateway/
│   ├── app.py
│   ├── shard_router.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── tests/
│   ├── concurrency_test_wd.py
│   ├── replica_failover_test.py
│   │
│   ├── Duarable_test_scenarioB/
│   │   ├── Durable_state_B.py
│   │   └── ScenarioB_coordinator.py
│   │
│   ├── experiment6/
│   │   ├── coordinator_test.py
│   │   └── experiment 6 guid and logs/
│   │       ├── coordinator-app.py
│   │
│   └── experiment7/
│       ├── experiment 7 guide.txt
│       ├── experiment7_concurrent.py
│
├── .env
├── .gitignore
├── client.py
├── docker-compose.yml
└── README.md

```

Note: This is a simplified overview. Additional application, database, and test files are included in the repository.

---

