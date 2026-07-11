# payflo

A Kafka-first payment event processing simulation, built to develop deep, hands-on Kafka fluency through a realistic fintech backbone. This is not a real payment integration — actual payment execution is mocked. The focus is entirely on the event-driven architecture between the start and end of a payment: producers, consumers, partitioning, ordering guarantees, and consumer group behavior, all running against a self-managed local Kafka cluster.

## Why this project exists

Most Kafka experience gained through managed platforms (Confluent Cloud, MSK, etc.) abstracts away the internals — cluster bootstrapping, listener configuration, partition assignment, controller quorum setup. payflo is deliberately built on a self-managed, Docker-based Kafka cluster to force direct engagement with those internals, rather than relying on a managed control plane.

The system simulates the event-driven backbone of a payment: initiation, gateway confirmation (success or failure), timeout detection, and notification dispatch — all coordinated through Kafka topics and consumer groups instead of synchronous service calls.

## System scope

payflo starts when a payment request is received and ends at termination (success, failure, or timeout). Actual payment execution — bank calls, gateway SDKs — is mocked. Everything else (producers, consumers, topics, partitioning, offset management, dead-letter handling, persistence) is real.

## Architecture

### Flow overview

1. **Payment initiation** — `POST /payment/initiate` validates the request, generates a `transactionId` (UUIDv7), and fires an initiation event. A consumer persists the transaction to MySQL (Redis integration in progress) and fires a notification event.
2. **Gateway confirmation (mocked)** — `POST /payment/confirm` simulates a payment gateway webhook, firing a success or failure event on separate topics. A consumer updates transaction state and fires a completion/failure notification.
3. **Timeout monitoring** — a scheduled job periodically scans for transactions that have exceeded their processing window and fires a timeout event, independent of the gateway confirmation path. *(Not yet implemented — see Project status.)*
4. **Status polling** — `GET /payment/status` is a read-only endpoint backed by Redis; it fires no Kafka events. *(Not yet implemented.)*

All state transitions are driven by consumers reacting to events, not by a synchronous orchestrator service.

### Design principles

- **One topic per lifecycle stage**, not one topic with a status field — keeps each consumer single-responsibility and avoids bloating a single topic as new stages are added.
- **Separate topics for success and failure outcomes**, rather than one topic with a polymorphic payload — keeps each consumer's downstream reaction purely additive (adding a new outcome type means a new topic + consumer, not a modified switch statement).
- **Partition key = `transactionId`** — guarantees strict per-transaction ordering while allowing unrelated transactions to process in parallel across partitions.
- **No synchronous orchestrator** — APIs fire only their own domain event; downstream reactions (termination, notification) are consumer responsibilities.
- **Dedicated timeout monitor**, not a Kafka consumer — Kafka's job is to deliver, a consumer's job is to process, and a scheduler's job is to monitor. These are kept as separate concerns.
- **Dual-write persistence** — MySQL for durable transaction history, Redis (including sorted sets) for fast in-flight state and O(log n + m) timeout range queries.
- **No automatic retries** — a failed payment is not retried within the same window; the user must reinitiate. This is a deliberate product decision, not a limitation.
- **Hand-rolled Kafka configuration**, not Spring Boot autoconfiguration — producer/consumer factories, serializers, and error handling are configured explicitly to make the underlying mechanics visible rather than abstracted away.

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
| ID generation            | UUIDv7 (`uuid-creator`)                         |
| Testing                  | JUnit, Mockito                                  |

The application is a single Spring Boot monolith with clearly separated packages (consumers, events, entities, configs, services). Microservices were deliberately avoided to keep the project focused on Kafka behavior rather than distributed-systems networking concerns.

## Kafka topology

Single broker, KRaft mode (ZooKeeper is unsupported as of Kafka 4.0), 3 partitions per topic, replication factor 1 (the only valid value on a single broker).

| Topic                                   | Fired by                                  | Purpose                                         |
|-----------------------------------------|-------------------------------------------|-------------------------------------------------|
| `payflo.payment-initiated`              | Payment initiation API                    | Payment request accepted and validated          |
| `payflo.notification.payment-initiated` | `PaymentInitiatedConsumer`                | Initiation notification                         |
| `payflo.payment-received`               | Mocked gateway confirmation API (success) | Gateway reported a successful outcome           |
| `payflo.notification.payment-completed` | `PaymentReceivedConsumer`                 | Completion notification                         |
| `payflo.payment-failed`                 | Mocked gateway confirmation API (failure) | Gateway reported a failed outcome, with reason  |
| `payflo.notification.payment-failed`    | `PaymentFailedConsumer`                   | Failure notification                            |
| `payflo.payment-timed-out`              | Timeout monitor scheduler *(pending)*     | Transaction exceeded its processing window      |
| `payflo.notification.payment-timed-out` | `PaymentTimedoutConsumer`                 | Timeout notification                            |
| `payflo.DLT`                            | Spring (`DeadLetterPublishingRecoverer`)  | Shared dead-letter topic for unparseable events |

All events for a given `transactionId` land on the same partition (`hash(transactionId) % partitions`), preserving strict ordering per transaction. Partition count is fixed at topic creation — resizing later would change the hash-to-partition mapping and break ordering for in-flight transactions.

## Local infrastructure

### Prerequisites

- Docker and Docker Compose
- Java 21+
- Maven
- Bash (Git Bash or WSL on Windows) for `.sh` scripts, or native `.bat` on Windows

### Starting the environment

```bash
# Checks Docker is running, starts Kafka/MySQL/Redis, and creates all topics
./scripts/start-dev.sh    # or start-dev.bat on Windows
```

Kafka runs a single broker acting as both broker and controller, with client traffic on `9092` and internal controller traffic on `9093`. The controller listener is intentionally not exposed to the host — only the broker itself needs to reach it.

Topic creation is treated as a one-time infrastructure step, similar to a database migration: it runs after the broker is available and before the application starts, but is not embedded in `docker-compose.yml` (infrastructure should not encode domain/business configuration) or in application startup (would couple topic topology ownership to app boot time).

### Starting the application

Once the environment is up:

```bash
mvn spring-boot:run
```

## Design decisions worth noting

- **Validation failures produce no Kafka event.** If `POST /payment/initiate` fails validation, the API returns an error directly — no transaction record exists yet, so there is nothing for a consumer to act on.
- **The polling API is read-only.** Status checks never fire events; only consumers fire termination and notification events, keeping that responsibility out of the API layer.
- **Idempotency is enforced at the database layer, not via `JpaRepository.save()`.** For entities with a manually-assigned primary key (UUIDv7 `transactionId`, not `@GeneratedValue`), Hibernate cannot infer "new vs. existing" from the ID alone — `save()` silently performs a select-then-merge instead of a clean insert-or-fail. Persistence is done via `EntityManager.persist()` (insert-only semantics), with the resulting `DataIntegrityViolationException` on a genuine duplicate caught, logged, and skipped.
- **Redelivery, not duplication, is the actual risk.** Kafka never spontaneously duplicates a single `send()` call; the same message can, however, be redelivered to a consumer that crashed after processing but before committing its offset. All idempotency and ordering reasoning in this project is built around that distinction.
- **Malformed messages are routed to a shared dead-letter topic, not dropped or left blocking the partition.** `ErrorHandlingDeserializer` catches deserialization failures per-record; `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` republish the failed record to `payflo.DLT` with zero retries (deserialization failures are permanent — retrying bad JSON never succeeds).
- **Notification consumers contain no business logic.** Message formatting happens upstream, in the consumer that fires the notification event; the notification consumer's only job is to log/dispatch what it's given, keeping it purely mechanical.

## Project status

- [x] Requirements and design
- [x] Local Kafka infrastructure (Docker, KRaft, topic creation, MySQL + Redis containers)
- [x] Kafka producer/consumer configuration (hand-rolled, not autoconfigured)
- [x] All 8 consumers implemented and tested (happy path, duplicate-delivery idempotency, malformed-payload DLT routing)
- [x] MySQL persistence working end-to-end via `EntityManager.persist()`
- [ ] Producer-side REST APIs (`/payment/initiate`, `/payment/confirm`) — in progress
- [ ] Real Redis integration (currently simulated in consumer logs)
- [ ] Timeout monitor scheduler
- [ ] Partial-write idempotency across DB + Redis + notification dispatch (race-condition hardening)
- [ ] Kafka behavior experiments (consumer failure, offset replay, partition rebalancing)
- [ ] Testing

## Target use case

This project is built as a learning and portfolio artifact demonstrating hands-on Kafka fluency for backend/SDE interview preparation, with a particular focus on companies operating in the payments and fintech space.