# payflo

A Kafka-first payment event processing simulation, built to develop deep, hands-on Kafka fluency through a realistic fintech backbone. This is not a real payment integration — actual payment execution is mocked. The focus is entirely on the event-driven architecture between the start and end of a payment: producers, consumers, partitioning, ordering guarantees, and consumer group behavior, all running against a self-managed local Kafka cluster.

## Why this project exists

Most Kafka experience gained through managed platforms (Confluent Cloud, MSK, etc.) abstracts away the internals — cluster bootstrapping, listener configuration, partition assignment, controller quorum setup. payflo is deliberately built on a self-managed, Docker-based Kafka cluster to force direct engagement with those internals, rather than relying on a managed control plane.

The system simulates the event-driven backbone of a payment: initiation, gateway confirmation (success or failure), timeout detection, and notification dispatch — all coordinated through Kafka topics and consumer groups instead of synchronous service calls.

## System scope

payflo starts when a payment request is received and ends at termination (success, failure, or timeout). Actual payment execution — bank calls, gateway SDKs — is mocked. Everything else (producers, consumers, topics, partitioning, offset management) is real Kafka.

## Architecture

### Flow overview

1. **Payment initiation** — `POST /payment/initiate` validates the request, generates a `transaction_id`, and fires an initiation event. A consumer persists the transaction to MySQL and Redis, then fires a notification event.
2. **Gateway confirmation (mocked)** — `POST /payment/confirm` simulates a payment gateway webhook, firing an outcome event (success or failure). A consumer updates transaction state and fires a completion/failure notification.
3. **Timeout monitoring** — a scheduled job periodically scans Redis for transactions that have exceeded their processing window and fires a timeout event, independent of the gateway confirmation path.
4. **Status polling** — `GET /payment/status` is a read-only endpoint backed by Redis; it fires no Kafka events.

All state transitions are driven by consumers reacting to events, not by a synchronous orchestrator service.

### Design principles

- **One topic per lifecycle stage**, not one topic with a status field — keeps each consumer single-responsibility and avoids bloating a single topic as new stages are added.
- **Partition key = `transaction_id`** — guarantees strict per-transaction ordering while allowing unrelated transactions to process in parallel across partitions.
- **No synchronous orchestrator** — APIs fire only their own domain event; downstream reactions (termination, notification) are consumer responsibilities.
- **Dedicated timeout monitor**, not a Kafka consumer — Kafka's job is to deliver, a consumer's job is to process, and a scheduler's job is to monitor. These are kept as separate concerns.
- **Dual-write persistence** — MySQL for durable transaction history, Redis (including sorted sets) for fast in-flight state and O(log n + m) timeout range queries.
- **No automatic retries** — a failed payment is not retried within the same window; the user must reinitiate. This is a deliberate product decision, not a limitation.

### Tech stack

| Layer                    | Choice                                          |
|--------------------------|-------------------------------------------------|
| Language                 | Java 21+                                        |
| Framework                | Spring Boot                                     |
| Message broker           | Apache Kafka (self-managed, KRaft mode, Docker) |
| Primary datastore        | MySQL                                           |
| Cache / fast-state store | Redis                                           |
| Build tool               | Maven                                           |
| Containerization         | Docker + Docker Compose                         |
| Testing                  | JUnit, Mockito                                  |

The application is a single Spring Boot monolith with clearly separated packages (producers, consumers, schedulers, services, APIs). Microservices were deliberately avoided to keep the project focused on Kafka behavior rather than distributed-systems networking concerns.

## Kafka topology

Single broker, KRaft mode (ZooKeeper is unsupported as of Kafka 4.0), 3 partitions per topic, replication factor 1 (the only valid value on a single broker).

| Topic                                   | Fired by                        | Purpose                                    |
|-----------------------------------------|---------------------------------|--------------------------------------------|
| `payflo.payment-initiated`              | Payment initiation API          | Payment request accepted and validated     |
| `payflo.notification.payment-initiated` | Initiation consumer             | Initiation notification                    |
| `payflo.payment-received`               | Mocked gateway confirmation API | Gateway reported a successful outcome      |
| `payflo.payment-failed`                 | Mocked gateway confirmation API | Gateway reported a failed outcome          |
| `payflo.notification.payment-completed` | Success consumer                | Completion notification                    |
| `payflo.notification.payment-failed`    | Failure consumer                | Failure notification                       |
| `payflo.payment-timedout`               | Timeout monitor scheduler       | Transaction exceeded its processing window |
| `payflo.notification.payment-timedout`  | Timeout consumer                | Timeout notification                       |

All events for a given `transaction_id` land on the same partition (`hash(transaction_id) % partitions`), preserving strict ordering per transaction. Partition count is fixed at topic creation — resizing later would change the hash-to-partition mapping and break ordering for in-flight transactions.

## Local infrastructure

### Prerequisites

- Docker and Docker Compose
- Java 21+
- Maven
- Bash (Git Bash or WSL on Windows)

### Starting the environment

```bash
# 1. Start Kafka (KRaft mode), MySQL, and Redis
docker compose up -d

# 2. Create Kafka topics (idempotent — safe to re-run)
./scripts/create-topics.sh
```

Kafka runs a single broker acting as both broker and controller, with client traffic on `9092` and internal controller traffic on `9093`. The controller listener is intentionally not exposed to the host — only the broker itself needs to reach it.

Topic creation is treated as a one-time infrastructure step, similar to a database migration: it runs after the broker is available and before the application starts, but is not embedded in `docker-compose.yml` (infrastructure should not encode domain/business configuration) or in application startup (would couple topic topology ownership to app boot time).

### Starting the application

Once Kafka and the topics are ready:

```bash
mvn spring-boot:run
```

## Design decisions worth noting

- **Validation failures produce no Kafka event.** If `POST /payment/initiate` fails validation, the API returns an error directly — no transaction record exists yet, so there is nothing for a consumer to act on.
- **The polling API is read-only.** Status checks never fire events; only consumers fire termination and notification events, keeping that responsibility out of the API layer.
- **Idempotency is enforced at the database layer.** Kafka consumer redelivery (after a crash before offset commit) is the real source of potential duplicate processing — not producer-side duplication. A unique constraint on `transaction_id` is the primary safeguard, with duplicate-key exceptions logged and skipped rather than retried.
- **Redelivery, not duplication, is the actual risk.** Kafka never spontaneously duplicates a single `send()` call; the same message can, however, be redelivered to a consumer that crashed after processing but before committing its offset. All idempotency and ordering reasoning in this project is built around that distinction.

## Project status 

- [x] Requirements and design
- [x] Local Kafka infrastructure (Docker, KRaft, topic creation)
- [x] Producer and consumer implementation
- [ ] Dead letter handling, idempotency enforcement
- [ ] Kafka behavior experiments (consumer failure, offset replay, partition rebalancing)
- [ ] Testing

## Target use case

This project is built as a learning and portfolio artifact demonstrating hands-on Kafka fluency for backend/SDE interview preparation, with a particular focus on companies operating in the payments and fintech space.