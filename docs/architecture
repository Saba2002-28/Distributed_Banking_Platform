\# Distributed Banking Platform — Architecture



\## 1. Project Overview



The Distributed Banking Platform is a distributed application that supports balance inquiries, deposits, withdrawals, and transfers between bank accounts.



The application uses sharding to partition accounts across two independent shards. Each shard has two application replicas that share the shard's authoritative PostgreSQL database. Docker Compose runs the complete application on a single host.



The main goals are to maintain correct account balances, distribute requests across healthy replicas, support concurrent operations, and execute cross-shard transfers using Two-Phase Commit (2PC).



\## 2. Technology Stack



\- \*\*Programming language:\*\* Python 3.12

\- \*\*Web framework:\*\* Flask

\- \*\*Database:\*\* PostgreSQL 17

\- \*\*Containerization:\*\* Docker

\- \*\*Service orchestration:\*\* Docker Compose

\- \*\*Testing framework:\*\* pytest

\- \*\*Communication:\*\* HTTP with JSON

\- \*\*Configuration:\*\* JSON and Docker Compose configuration files



\## 3. System Components



\### 3.1. Banking Client



The client initiates banking operations, including:



\- Balance inquiry

\- Deposit

\- Withdrawal

\- Transfer between accounts



The client communicates only with the Gateway. It does not need to know which shard owns an account or which replica handles a request.



\### 3.2. Shard-Aware Gateway



The Gateway is the entry point for client requests.



Its responsibilities are:



1\. Receive and validate incoming requests.

2\. Determine the owning shard using the Shard Map.

3\. Select a healthy application replica belonging to that shard.

4\. Distribute requests using a round-robin policy.

5\. Forward requests to the selected replica.

6\. Delegate cross-shard transfers to the Transaction Coordinator.

7\. Avoid routing new requests to unhealthy replicas.



The Gateway must never route an account operation to a shard that does not own that account.



\### 3.3. Banking Shards and Application Replicas



The platform contains two shards, each with two application replicas.



\*\*Shard A\*\*

\- Banking Server A1

\- Banking Server A2

\- PostgreSQL Database A

\- Account range: A0001–A1000



\*\*Shard B\*\*

\- Banking Server B1

\- Banking Server B2

\- PostgreSQL Database B

\- Account range: A1001–A2000



Both replicas of a shard share the same authoritative database. Replicas do not maintain separate authoritative balances.



The banking services execute local operations, enforce banking rules, and use database transactions and concurrency control to preserve balance correctness.



\### 3.4. PostgreSQL Databases



Each shard has an independent PostgreSQL database with persistent storage.



\- Database A stores accounts assigned to Shard A.

\- Database B stores accounts assigned to Shard B.



Account data must not be copied to every shard. Each account has exactly one authoritative shard.



Database transactions provide atomicity and durability for local operations. Same-shard transfers update both accounts within one atomic database transaction.



\### 3.5. Transaction Coordinator



The Transaction Coordinator manages cross-shard transfers using Two-Phase Commit.



Its responsibilities include:



1\. Assign a unique transaction ID.

2\. Identify the participating shards.

3\. Send PREPARE requests to the participants.

4\. Collect participant votes.

5\. Durably record a COMMIT or ABORT decision.

6\. Send the recorded decision to participants.

7\. Track transaction completion and acknowledgements.

8\. Recover unresolved transactions after a restart.

9\. Expose transaction status when the outcome is pending or unknown.



The Coordinator must not report a successful transfer until all participants confirm COMMIT.



\### 3.6. Shard Map



The Shard Map defines which shard owns each account range.



The initial mapping is:



| Shard | Account Range |

|---|---|

| Shard A | A0001–A1000 |

| Shard B | A1001–A2000 |



The initial mapping is stored in `config/shard-map.json`.



The Gateway uses this mapping to determine the correct shard before selecting a replica.



\## 4. Request Routing



For a local banking operation:



1\. The client sends a request to the Gateway.

2\. The Gateway determines the account's owning shard.

3\. The Gateway selects a healthy replica of that shard.

4\. The replica executes the operation against its shard database.

5\. The result is returned to the client through the Gateway.



For a cross-shard transfer:



1\. The client submits the transfer through the Gateway.

2\. The Gateway identifies the source and destination shards.

3\. The Gateway delegates the transfer to the Transaction Coordinator.

4\. The Coordinator executes the prepare phase.

5\. The Coordinator durably records the final decision.

6\. The Coordinator distributes the decision to the participants.

7\. The transfer is reported as successful only after both participants confirm COMMIT.



\## 5. Consistency and Concurrency Control



Concurrent requests may be handled by different replicas of the same shard. Because those replicas share one database, database-level concurrency control must prevent lost updates and inconsistent balances.



The implementation will use PostgreSQL transactions and appropriate row-level locking for balance-changing operations. Withdrawals must verify that sufficient funds are available while protecting against concurrent withdrawals.



Local transfers must update both accounts atomically.



Cross-shard transfers require the distributed transaction protocol described above. Repeated messages and recovery attempts must not apply a transfer more than once.



\## 6. Availability and Health Checks



The Gateway will maintain or obtain the health status of banking replicas.



If a replica becomes unavailable, the Gateway must stop selecting it for new requests and route requests to another healthy replica of the same shard.



Database replication and automatic database failover are outside the required scope of the assignment.



\## 7. Docker Compose Deployment



Docker Compose will run the platform's services on a shared internal network.



The initial deployment will include:



\- Gateway

\- Transaction Coordinator

\- Banking replicas A1 and A2

\- Banking replicas B1 and B2

\- PostgreSQL Database A

\- PostgreSQL Database B



The PostgreSQL databases will use separate named Docker volumes so their data persists across ordinary container restarts and recreation.



Application services will communicate using Docker service names and internal container ports. Only the Gateway needs to expose an HTTP port to the host for client access.



\## 8. Transaction Recovery



The Coordinator will persist transaction states and final decisions.



If the Coordinator restarts, it must inspect its durable transaction records and resume unresolved work.



A recorded COMMIT decision must never be replaced with ABORT because a participant is temporarily unavailable. Participants must safely handle repeated COMMIT and ABORT messages.



Unresolved transactions must expose a queryable PENDING or UNKNOWN status until their outcome is established.



\## 9. Planned Verification



The implementation will be tested for:



\- Correct account distribution across both shards.

\- Correct Gateway routing.

\- Load distribution across replicas.

\- Correct balances during concurrent deposits and withdrawals.

\- Continued operation after a replica becomes unavailable.

\- Atomic same-shard transfers.

\- Correct cross-shard transfers using 2PC.

\- Recovery after Coordinator interruption.

\- Correctness of concurrent cross-shard transfers.



\## 10. Scope



The initial implementation focuses on the required two-shard banking platform, replica routing, consistency, distributed transactions, and recovery.



Advanced online re-sharding and autonomous self-healing are optional bonus features and will be considered only after the required functionality is complete.

