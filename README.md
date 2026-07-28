# payflo

**A Kafka-first, event-driven payment processing backbone — built from the ground up to demonstrate real distributed-systems engineering, not a payment gateway integration.**

> payflo does not process real payments. Bank calls, gateway SDKs, and NPCI lookups are intentionally mocked. The entire engineering focus is the event-driven backbone between the start and end of a payment: ordering guarantees, partitioning, consumer groups, idempotency, dead-letter handling, and cache-backed state — the mechanics that matter in a real fintech payments platform.

---

## Table of Contents

- [Core Objective](#core-objective)
- [Features](#features)
- [Architecture Overview](#architecture-overview)
- [Payment Lifecycle](#payment-lifecycle)
- [Event Catalog](#event-catalog)
- [Kafka Topics](#kafka-topics)
- [System Components](#system-components)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Local Setup](#local-setup)
- [Configuration](#configuration)
- [REST APIs](#rest-apis)
- [Testing the Event Flow](#testing-the-event-flow)
- [Design Decisions](#design-decisions)
- [Reliability & Fault Tolerance](#reliability--fault-tolerance)
- [Future Improvements](#future-improvements)
- [Conclusion](#conclusion)

---

## Core Objective

**Business problem:** Payment systems at scale (Razorpay, CRED, Groww, Juspay-class platforms) cannot afford to hold an HTTP request open while a payment is validated, dispatched to a gateway, confirmed, and settled. That workflow is inherently asynchronous, must survive partial failures, must never double-charge or double-notify a user, and must detect and terminate transactions that silently stall. A synchronous, request-response model breaks down under exactly these constraints.

**Why Event-Driven Architecture:** payflo models the payment lifecycle as a sequence of discrete, independently-processable domain events — `payment-initiated` → `payment-received` / `payment-failed` / `payment-timed-out` — each with its own topic, its own consumer, and its own downstream notification event. This buys:

- **Decoupling** — the REST layer never blocks on downstream processing; it fires an event and returns.
- **Independent scalability** — consumers for validation, persistence, and notification scale independently of the API layer.
- **Natural audit trail** — the event log _is_ the history of what happened to a transaction.
- **Resilience** — a slow or crashed consumer doesn't take down the API; messages wait in the topic.

The project exists to build genuine fluency with these mechanics — partition-key ordering, consumer group rebalancing, offset management, dead-letter routing, idempotent processing — using a **self-managed Kafka cluster (KRaft mode, no Zookeeper, no managed Confluent Cloud)**, so every piece of infrastructure is understood rather than assumed.

---

## Features

- **Polymorphic payment initiation** — UPI and Card payment types via a sealed-interface + Jackson-discriminator model, each with independent, extensible validation (VPA format + PSP handle validation; Luhn's algorithm + expiry + CVV validation for cards).
- **Fully event-driven lifecycle** — 8 Kafka topics across payment-lifecycle and notification event groups, with dedicated consumers for each stage.
- **Idempotent consumers** — insert-only persistence semantics (`EntityManager.persist()`) guarantee duplicate Kafka deliveries never produce duplicate side effects on the initiating write; termination writes are naturally idempotent updates.
- **Redis-backed status reads** — MySQL is durable storage only; the hot-path status API reads exclusively from Redis, keeping the database off the request-serving critical path.
- **Sorted-set-driven timeout detection** — a scheduler queries a Redis sorted set for transactions past their deadline and fires a `payment-timed-out` event, with a dedicated consumer owning the actual state transition.
- **Atomic partial-write & race-condition hardening** — a Lua-based atomic check-and-set (`TransactionOwnershipService`) resolves the race between competing termination consumers (`payment-received` vs. `payment-timed-out`) and guards against partial-write inconsistency, with notification delivery treated as at-least-once by design.
- **Centralized, typed configuration** — every topic name, error message, status message, notification template, and infrastructure property is externalized via `@ConfigurationProperties` records, never hardcoded.
- **Structured exception handling** — a single `PayfloException` hierarchy with per-scenario `ErrorCode`s, resolved by a global `@RestControllerAdvice` into a consistent client-facing error contract.
- **Dead-letter routing** — malformed or undecodable messages are automatically routed to a shared DLT via `DeadLetterPublishingRecoverer`.
- **TTL-based Redis self-cleanup** — terminal transaction states automatically expire out of Redis after a configurable retention window, no separate cleanup job required.

---

## Architecture Overview

payflo is a **single Spring Boot monolith** — microservices were explicitly rejected to keep the entire learning focus on Kafka behavior rather than distributed-deployment concerns. The monolith is internally organized by strict separation of concerns: REST controllers never touch Kafka directly, services never contain persistence logic, and consumers own all state-transition business logic.

**Layer responsibilities:**

- **Client** — sends payment requests, polls status.
- **REST API** — accepts requests, validates structurally, fires initiating events. Never blocks on downstream processing.
- **Producers** (`EventPublisher`) — single, centralized publish path for every event in the system.
- **Message Broker** — self-managed Apache Kafka (KRaft mode); 8 topics across 2 lifecycle groups, plus 1 shared DLT.
- **Consumers** — one consumer per event type; own all business logic, persistence, and cache mutation.
- **Database (MySQL)** — durable, authoritative transaction record; not queried on the status-read hot path.
- **Cache (Redis)** — hash for O(1) status lookups, sorted set for timeout-range queries.
- **Scheduler** (`TransactionMonitoringSchedular`) — pure trigger; detects expired transactions and fires an event, owns no mutation.
- **Notification system** — dedicated notification consumers per lifecycle stage, currently log/print, structurally ready for real dispatch.

**Request → event flow, end to end:**

1. Client calls a REST endpoint (`/payment/initiate`, `/payment/confirm`, `/payment/status/{id}`, `/payment/options`).
2. The corresponding controller delegates to its service, which validates (for initiate) and calls `EventPublisher.publish(...)`.
3. `EventPublisher` resolves the target topic via `KafkaTopicResolver` and sends through a shared `KafkaTemplate`.
4. The matching consumer (one per event type) picks up the message, executes its business logic — MySQL write, Redis hash/zset update via `TransactionOwnershipService`/`TransactionInitializationService`, then publishes a notification event.
5. A dedicated notification consumer receives that event and dispatches it (currently log/print), with a Redis-backed dedup flag to minimize duplicate sends under redelivery.
6. Independently, `TransactionMonitoringSchedular` polls the Redis sorted set on a fixed delay, and for every transaction past its deadline, publishes a `PaymentTimedOutEvent` — which flows through the same termination-consumer path as steps 4–5.
7. Any message that fails deserialization is routed straight to the shared `payflo.DLT`, regardless of which topic it came from.

---

## Payment Lifecycle

**Happy path — successful payment:**

| Step | Actor                        | Action                                                                                                                                    |
| ---- | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | Client                       | `POST /payment/initiate`                                                                                                                  |
| 2    | API → Validator              | Structural validation (VPA/card format)                                                                                                   |
| 3    | API → EventPublisher → Kafka | Publishes `PaymentInitiatedEvent` to `payflo.payment-initiated`                                                                           |
| 4    | API → Client                 | Returns `200 OK` with `transactionId` — no state change confirmed yet                                                                     |
| 5    | `PaymentInitiatedConsumer`   | Writes Redis hash (`status=PROCESSING`) + zset entry, persists to MySQL (insert), publishes initiation notification                       |
| 6    | Client                       | `GET /payment/status/{id}` → reads `PROCESSING` from Redis                                                                                |
| 7    | Client                       | `POST /payment/confirm` (mocked gateway callback)                                                                                         |
| 8    | API → EventPublisher → Kafka | Publishes `PaymentReceivedEvent` (or `PaymentFailedEvent`), returns `202 Accepted`, no body                                               |
| 9    | Termination consumer         | Atomically claims ownership (Lua CAS), updates MySQL, publishes completion notification, finalizes Redis status + TTL, removes zset entry |
| 10   | Client                       | `GET /payment/status/{id}` → reads `COMPLETED` from Redis                                                                                 |
| 11   | (later)                      | Redis key TTL expires → subsequent status check returns `404 Transaction Not Found`                                                       |

**Timeout path — independent of the happy path:**

| Step | Actor                            | Action                                                                                                                                                                    |
| ---- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | `TransactionMonitoringSchedular` | Every `fixedDelay` interval, queries the Redis sorted set for entries scored before now                                                                                   |
| 2    | Scheduler → Kafka                | Publishes a `PaymentTimedOutEvent` per expired `transactionId` — pure trigger, no mutation                                                                                |
| 3    | `PaymentTimedOutConsumer`        | Same ownership-claim + finalize flow as any other termination consumer — competes fairly against a late-arriving `payment-received`/`payment-failed` via the same Lua CAS |

---

## Event Catalog

All 8 event records implement a shared `PaymentEvent` interface exposing `topic()` and `key()`, published through a single `EventPublisher.publish(PaymentEvent)` method — no per-event publishing boilerplate.

### `PaymentInitiatedEvent`

**Producer:** `PaymentInitiationService` · **Consumer:** `PaymentInitiatedConsumer` · **Topic:** `payflo.payment-initiated`

Fired when a structurally-valid payment request is accepted. Carries `transactionId`, `amount`, `paymentType`, and `startedAt` — `startedAt` is the customer-facing clock start, used later for timeout scoring against the Redis sorted set.

### `PaymentReceivedEvent`

**Producer:** `PaymentGatewayService` · **Consumer:** `PaymentReceivedConsumer` · **Topic:** `payflo.payment-received`

Fired when the (mocked) gateway callback reports success. Terminates the transaction as `COMPLETED`, after winning the atomic ownership claim against any competing termination event for the same transaction.

### `PaymentFailedEvent`

**Producer:** `PaymentGatewayService` · **Consumer:** `PaymentFailedConsumer` · **Topic:** `payflo.payment-failed`

Fired when the gateway callback reports failure. Terminates the transaction as `FAILED`, under the same ownership-claim protection as every other termination path.

### `PaymentTimedOutEvent`

**Producer:** `TransactionMonitoringSchedular` · **Consumer:** `PaymentTimedOutConsumer` · **Topic:** `payflo.payment-timed-out`

Fired by the scheduler for any transaction whose Redis sorted-set score has passed. Carries only `transactionId` — the deadline itself is derivable from `startedAt` (in MySQL) plus the configured buffer, so it's deliberately not duplicated into the event payload.

### `PaymentInitiatedNotificationEvent`

**Producer:** `PaymentInitiatedConsumer` · **Consumer:** Notification consumer · **Topic:** `payflo.notification.payment-initiated`

Fired after the initiated-consumer's primary work completes. Message resolved from a template keyed by the _triggering_ event's topic.

### `PaymentCompletedNotificationEvent`

**Producer:** `PaymentReceivedConsumer` · **Consumer:** Notification consumer · **Topic:** `payflo.notification.payment-completed`

User-facing "payment successful" notification.

### `PaymentFailedNotificationEvent`

**Producer:** `PaymentFailedConsumer` · **Consumer:** Notification consumer · **Topic:** `payflo.notification.payment-failed`

User-facing "payment failed" notification.

### `PaymentTimedOutNotificationEvent`

**Producer:** `PaymentTimedOutConsumer` · **Consumer:** Notification consumer · **Topic:** `payflo.notification.payment-timed-out`

User-facing "payment timed out" notification.

---

## Kafka Topics

| Topic                                   | Producer                                          | Consumer                   |
| --------------------------------------- | ------------------------------------------------- | -------------------------- |
| `payflo.payment-initiated`              | `PaymentInitiationService`                        | `PaymentInitiatedConsumer` |
| `payflo.payment-received`               | `PaymentGatewayService`                           | `PaymentReceivedConsumer`  |
| `payflo.payment-failed`                 | `PaymentGatewayService`                           | `PaymentFailedConsumer`    |
| `payflo.payment-timed-out`              | `TransactionMonitoringSchedular`                  | `PaymentTimedOutConsumer`  |
| `payflo.notification.payment-initiated` | `PaymentInitiatedConsumer`                        | Notification consumer      |
| `payflo.notification.payment-completed` | `PaymentReceivedConsumer`                         | Notification consumer      |
| `payflo.notification.payment-failed`    | `PaymentFailedConsumer`                           | Notification consumer      |
| `payflo.notification.payment-timed-out` | `PaymentTimedOutConsumer`                         | Notification consumer      |
| `payflo.DLT`                            | Kafka framework (`DeadLetterPublishingRecoverer`) | Manual inspection only     |

**Partitioning strategy.** Every event is keyed by `transactionId`, guaranteeing all events for a single transaction land on the same partition and are processed in strict order by a single consumer thread. Partition count is treated as fixed at creation time — changing it later would break `hash(key) % N` ordering guarantees for any in-flight transaction, so topic creation is a deliberate, separate infrastructure step, not baked into Docker Compose or `application.yml`.

**Ordering guarantees.** Per-key ordering only (Kafka's native guarantee); no global ordering across different transactions, which is the correct and sufficient guarantee for this domain.

**Consumer groups.** Each consumer type runs in its own consumer group, so lifecycle consumers and notification consumers scale and rebalance independently.

**Retry handling.** Deserialization failures are non-retryable by nature (a malformed payload will never deserialize successfully) and are routed directly to the DLT rather than retried in a loop.

**Dead-letter topics.** A single shared `payflo.DLT` receives any message that fails deserialization, using Spring Kafka's `DeadLetterPublishingRecoverer`. Payloads that fail due to trusted-package/class-name mismatches are republished as raw base64 bytes, since the value deserializer itself couldn't decode them.

**Idempotency.** The initiating consumer relies on `EntityManager.persist()` (not `JpaRepository.save()`) for its MySQL insert, catching `DataIntegrityViolationException` on a duplicate delivery. `save()` was explicitly rejected because it performs a silent select-then-merge for manually-assigned IDs (UUIDv7), which would silently overwrite rather than reject a duplicate. Termination consumers instead perform an idempotent `UPDATE` — safe to rerun with the same value on redelivery — protected at the Redis layer by an atomic ownership claim rather than a DB-level uniqueness check.

---

## System Components

### REST API

Four `@RestController` classes (`PaymentOptionsController`, `PaymentInitiationController`, `PaymentGatewayController`, `PaymentStatusController`), all sharing the `/payment/*` namespace. Split by single-responsibility-per-class, not by URL fragmentation — the client sees one coherent `/payment` resource. Confirmed at the framework level that Spring detects true path+method collisions at startup (`IllegalStateException: Ambiguous mapping`), so class-splitting carries no silent-routing risk.

### Producer Layer

`EventPublisher` — one method, `publish(PaymentEvent)`, resolves the target topic via `KafkaTopicResolver` and sends through a shared `KafkaTemplate`. Deliberately kept as a concrete, Kafka-only class rather than an interface with swappable broker implementations — a real design discussion was had (`@ConditionalOnProperty`, `@Primary`, `@Qualifier`), but concluded to be YAGNI for a Kafka-focused learning project with no intent to swap brokers.

### Kafka Infrastructure

`KafkaTopic` (enum of all 9 topics including the DLT), `KafkaTopicsProperties` (`@ConfigurationProperties` record backing the enum with real topic-name strings from config), and `KafkaTopicResolver` (exhaustive switch bridging enum → string). This indirection exists specifically to eliminate typo/drift risk between code and topic configuration — including the DLT's own topic name, previously a hardcoded literal, now resolved the same way as every other topic.

### Consumers

One consumer class per event type, each owning 100% of that event's business logic — persistence, cache mutation, and notification-event construction. Notification consumers remain intentionally pure log/print, with zero business logic, to keep the notification _pipeline_ structurally separate from notification _dispatch_ (which is a planned future integration point).

### Validation Layer

`PaymentDetailsValidatorService` dispatches via an exhaustive pattern-matching `switch` (record deconstruction) to `UpiValidator` or `CardValidator` — two independently extensible validator classes implementing a shared `PaymentValidator<T>` interface. UPI validation runs a 5-step chain (case-normalization → separator-count check → identifier regex → PSP-handle-format regex → PSP-handle set-membership lookup). Card validation runs a 4-step chain (number-format regex → CVV-format regex → Luhn's algorithm → expiry check via `YearMonth`).

### Redis (Cache Layer)

Two independently-reasoned service+repository pairs under `cache/`:

- **`RedisHashService` / `RedisHashRepository`** — owns per-transaction status (`payflo:payment-transaction:{transactionId}` → `status` field) and per-topic notification-sent flags. No TTL while pending; TTL applied only at terminal-state finalization, so completed records self-clean without a dedicated cleanup job.
- **`RedisZSetService` / `RedisZSetRepository`** — owns the single global sorted set (`payflo:processing-payment-transactions:by-started-at`) used exclusively for timeout-range detection.

Kept as two separate pairs (not one umbrella cache service) because status-tracking and timeout-tracking have different reasons to change, even though they currently share one underlying store.

### Atomic Ownership & Initialization (Race-Condition Hardening)

Two additional service+repository pairs, each backed by a parameterized Lua script executed atomically via `RedisTemplate.execute(RedisScript, ...)`:

- **`TransactionOwnershipService` / `TransactionOwnershipRepository`** — used by all three termination consumers to atomically claim exclusive ownership of a transaction's termination, guarding against a cross-consumer race (e.g. `payment-received` and a late `payment-timed-out` firing for the same transaction) and against redelivery reprocessing a transaction that already finished. The same script is reused across all three consumers, parameterized only by which `*_PENDING` status to claim with.
- **`TransactionInitializationService` / `TransactionInitializationRepository`** — used by `PaymentInitiatedConsumer` to atomically write the initial Redis hash entry and sorted-set entry in a single round trip. No ownership check is needed here, since this is the only consumer that ever creates these entries; the script exists purely to avoid two separate network round trips.

Notification delivery itself is treated as **at-least-once, not exactly-once** — a deliberate accepted tradeoff, since no atomic mechanism can span an external Kafka publish. Duplicate-send risk is minimized (not eliminated) by pushing a dedup check to the notification-consumer, the layer closest to the actual side effect.

### MySQL

Durable, authoritative record of every transaction. No longer part of the status-read hot path as of the Redis integration phase — Redis absorbs all status reads; MySQL exists purely for durability and (in principle) audit/reporting.

### Scheduler

`TransactionMonitoringSchedular` — a deliberately thin, pure-trigger component. It queries Redis (`ZRANGEBYSCORE` up to now), and for every expired `transactionId`, constructs and publishes a `PaymentTimedOutEvent`. It performs **no mutation whatsoever** — all state transition is owned by `PaymentTimedOutConsumer`, keeping detection and business logic in separate, single-responsibility components. Runs on `@Scheduled(fixedDelayString = ...)`, chosen deliberately over `fixedRate` so each run only starts after the previous run has fully completed, giving natural cooldown headroom.

### Notification Module

Four dedicated notification consumers, one per lifecycle-terminating event, each resolving a message template via `NotificationMessageResolver` keyed by the _triggering_ event's topic (not the notification event's own topic — constructing the notification event requires the resolved message as a constructor argument, which would otherwise create a circular dependency).

### Exception Handling

An abstract `PayfloException` (carrying an `ErrorCode`) is the base for all domain exceptions. `GlobalExceptionHandler` (`@RestControllerAdvice`) provides both a generic fallback and specific handlers where HTTP status genuinely differs, and additionally normalizes two framework-level exceptions (`MethodArgumentTypeMismatchException`, `HttpMessageNotReadableException`) into the same `ErrorResponse` shape used everywhere else, so clients see one consistent error contract regardless of failure origin.

---

## Technology Stack

| Category         | Choice                                                                              |
| ---------------- | ----------------------------------------------------------------------------------- |
| Language         | Java 21                                                                             |
| Framework        | Spring Boot                                                                         |
| Database         | MySQL                                                                               |
| Cache            | Redis (self-managed, via Docker)                                                    |
| Message Broker   | Apache Kafka — KRaft mode, `apache/kafka` image (self-managed, not Confluent Cloud) |
| Build Tool       | Maven                                                                               |
| Containerization | Docker + Docker Compose                                                             |
| Testing          | JUnit, Mockito                                                                      |
| API Testing      | Postman                                                                             |
| IDE              | IntelliJ IDEA                                                                       |
| Version Control  | Git / GitHub                                                                        |

**Key libraries:** Jackson (`jackson-datatype-jsr310` for `YearMonth` support), Spring Data JPA, Spring Data Redis (Lettuce client), Spring Kafka, Lombok (`@Slf4j`).

---

## Project Structure

```
payflo/
├── src/main/java/com/vishal/payflo/
│   ├── PayfloApplication.java
│   │
│   ├── configs/
│   │   ├── KafkaConfigs.java
│   │   ├── KafkaConnectionProperties.java
│   │   ├── RedisConfigs.java
│   │   ├── RedisConnectionProperties.java
│   │   ├── RedisKeysProperties.java
│   │   ├── RedisStatusTtlProperties.java
│   │   ├── PaymentTimeoutProperties.java
│   │   ├── VpaPaymentServiceProviderProperties.java
│   │   ├── PaymentStatusMessagesProperties.java
│   │   ├── ExceptionMessagesProperties.java
│   │   ├── NotificationHashKeyProperties.java
│   │   └── NotificationMessageProperties.java
│   │
│   ├── controllers/
│   │   ├── PaymentOptionsController.java
│   │   ├── PaymentInitiationController.java
│   │   ├── PaymentGatewayController.java
│   │   └── PaymentStatusController.java
│   │
│   ├── services/
│   │   ├── PaymentInitiationService.java
│   │   ├── PaymentGatewayService.java
│   │   ├── PaymentStatusService.java
│   │   ├── PaymentTransactionService.java
│   │   └── PaymentDetailsValidatorService.java
│   │
│   ├── validators/
│   │   ├── PaymentValidator.java
│   │   ├── UpiValidator.java
│   │   └── CardValidator.java
│   │
│   ├── kafka/
│   │   ├── topics/
│   │   │   ├── KafkaTopic.java
│   │   │   ├── KafkaTopicsProperties.java
│   │   │   └── KafkaTopicResolver.java
│   │   ├── events/*.java
│   │   └── EventPublisher.java
│   │
│   ├── consumers/
│   │   ├── PaymentInitiatedConsumer.java
│   │   ├── PaymentReceivedConsumer.java
│   │   ├── PaymentFailedConsumer.java
│   │   ├── PaymentTimedOutConsumer.java
│   │   └── *NotificationConsumer.java
│   │
│   ├── cache/
│   │   ├── service/
│   │   │   ├── RedisHashService.java
│   │   │   ├── RedisZSetService.java
│   │   │   ├── TransactionOwnershipService.java
│   │   │   └── TransactionInitializationService.java
│   │   ├── repository/
│   │   │   ├── RedisHashRepository.java
│   │   │   ├── RedisZSetRepository.java
│   │   │   ├── TransactionOwnershipRepository.java
│   │   │   └── TransactionInitializationRepository.java
│   │   └── scripts/
│   │       └── LuaScripts.java
│   │
│   ├── notifications/
│   │   ├── NotificationPublisher.java
│   │   ├── NotificationHashKeyResolver.java
│   │   ├── NotificationMessageResolver.java
│   │   └── NotificationMessageTemplateBuilder.java
│   │
│   ├── scheduled/
│   │   └── TransactionMonitoringSchedular.java
│   │
│   ├── dtos/
│   │   ├── ApiResponse.java
│   │   ├── paymentdetails/
│   │   │   ├── PaymentDetails.java
│   │   │   ├── UpiDetails.java
│   │   │   └── CardDetails.java
│   │   ├── requestdtos/*.java
│   │   └── responsedtos/*.java
│   │
│   ├── advice/
│   │   ├── exceptions/*.java
│   │   ├── enums/
│   │   │   └── ErrorCode.java
│   │   ├── ErrorResponse.java
│   │   └── GlobalExceptionHandler.java
│   │
│   ├── entities/
│   │   └── PaymentTransaction.java
│   │
│   ├── repositories/
│   │   └── PaymentTransactionRepository.java
│   │
│   └── enums/
│       ├── TransactionStatus.java
│       ├── PaymentType.java
│       └── GatewayStatus.java
│
├── src/test/java/com/vishal/payflo/
│
├── docker-compose.yml
├── .env
├── application.yml
└── pom.xml
```

---

## Local Setup

### Prerequisites

- Java 21+
- Maven
- Docker & Docker Compose

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/payflo.git
cd payflo
```

### 2. Configure environment variables

Create a `.env` file in the project root:

```bash
# MySQL
MYSQL_URL=jdbc:mysql://localhost:3306/payflo
MYSQL_USERNAME=payflo_user
MYSQL_PASSWORD=your_password

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=payflo-consumer-group
KAFKA_TRUSTED_PACKAGES=com.vishal.payflo.kafka.events
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PAYMENT_TRANSACTION_HASH_PREFIX=payflo:payment-transaction:
REDIS_PROCESSING_TRANSACTIONS_ZSET_KEY=payflo:processing-payment-transactions:by-started-at
REDIS_PAYMENT_TRANSACTION_HASH_STATUS_KEY=status
PAYMENT_TIMEOUT_BUFFER_MINUTES=10
PAYMENT_STATUS_TTL_HOURS=1
TRANSACTION_MONITOR_FIXED_DELAY_MS=30000

# Notification hash keys
PAYMENT_INITIATED_NOTIFICATION_HASH_KEY=notification:payment-initiated
PAYMENT_COMPLETED_NOTIFICATION_HASH_KEY=notification:payment-completed
PAYMENT_FAILED_NOTIFICATION_HASH_KEY=notification:payment-failed
PAYMENT_TIMED_OUT_NOTIFICATION_HASH_KEY=notification:payment-timed-out

# Validation
VALIDATION_VPA_ALLOWED_HANDLES=okaxis,okhdfcbank,oksbi,okicici,ybl,axl,ibl,paytm,apl,upi,sbi,airtel,kotak,icici
```

> **Note:** `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` must be explicitly set to `1` on a single-broker setup — omitting it silently breaks all consumer group functionality.

### 3. Start infrastructure

```bash
docker compose up -d
```

This brings up Kafka (KRaft mode), MySQL, and Redis.

> **Windows / Git Bash:** prefix Docker commands with `MSYS_NO_PATHCONV=1` to prevent path mangling into Windows-style paths.

### 4. Create Kafka topics

Topic creation is a deliberate, separate infrastructure step (not part of Compose or `application.yml`), since partition count is fixed at creation and must not change under an in-flight system:

```bash
MSYS_NO_PATHCONV=1 docker exec -it <kafka_container_name> \
  /opt/kafka/bin/kafka-topics.sh --create \
  --topic payflo.payment-initiated \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

Repeat for all 9 topics listed in [Kafka Topics](#kafka-topics).

### 5. Build the application

```bash
mvn clean install
```

### 6. Run the application

```bash
mvn spring-boot:run
```

### 7. Verify services

```bash
# Kafka topics
MSYS_NO_PATHCONV=1 docker exec -it <kafka_container_name> \
  /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Redis
docker exec -it <redis_container_name> redis-cli PING

# MySQL
docker exec -it <mysql_container_name> mysql -u payflo_user -p -e "SHOW TABLES;"
```

---

## Configuration

**MySQL:** `MYSQL_URL`, `MYSQL_USERNAME`, `MYSQL_PASSWORD` — JDBC connection to the durable transaction store.

**Kafka:** `KAFKA_BOOTSTRAP_SERVERS` (broker address), `KAFKA_GROUP_ID` (consumer group), `KAFKA_TRUSTED_PACKAGES` (packages Jackson's deserializer trusts — must be kept in sync with any package refactor of event classes), `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` (must be `1` on single-broker local setups).

**Redis:** `REDIS_HOST` / `REDIS_PORT` (connection), `REDIS_PAYMENT_TRANSACTION_HASH_PREFIX` (per-transaction status hash key prefix), `REDIS_PROCESSING_TRANSACTIONS_ZSET_KEY` (global timeout-tracking sorted set key), `REDIS_PAYMENT_TRANSACTION_HASH_STATUS_KEY` (hash field name storing status).

**Timing:** `PAYMENT_TIMEOUT_BUFFER_MINUTES` (minutes after `startedAt` before a transaction is considered timed out), `PAYMENT_STATUS_TTL_HOURS` (hours a terminal-state Redis entry is retained before self-expiring), `TRANSACTION_MONITOR_FIXED_DELAY_MS` (scheduler polling interval).

**Notification hash keys:** `PAYMENT_INITIATED_NOTIFICATION_HASH_KEY`, `PAYMENT_COMPLETED_NOTIFICATION_HASH_KEY`, `PAYMENT_FAILED_NOTIFICATION_HASH_KEY`, `PAYMENT_TIMED_OUT_NOTIFICATION_HASH_KEY` — per-topic Redis hash field names used for the notification dedup flag.

**Validation:** `VALIDATION_VPA_ALLOWED_HANDLES` — comma-separated list of recognized UPI PSP handles.

---

## REST APIs

| Method | Endpoint                          | Description                                                                                                                  |
| ------ | --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `GET`  | `/payment/options`                | Returns all available `PaymentType` values                                                                                   |
| `POST` | `/payment/initiate`               | Accepts a polymorphic payment request (UPI or Card), validates it, fires `payflo.payment-initiated`, returns `transactionId` |
| `POST` | `/payment/confirm`                | Mocked gateway callback; fires `payflo.payment-received` or `payflo.payment-failed`; returns `202 Accepted` with no body     |
| `GET`  | `/payment/status/{transactionId}` | Returns current transaction status, read from Redis; `404` if the transaction is unknown or its record has expired           |

**Example — initiate a UPI payment:**

```bash
curl -X POST http://localhost:8080/payment/initiate \
  -H "Content-Type: application/json" \
  -d '{
        "amount": 499.00,
        "paymentDetails": {
          "type": "UPI",
          "vpa": "vishal@oksbi"
        }
      }'
```

**Example — confirm a payment:**

```bash
curl -X POST http://localhost:8080/payment/confirm \
  -H "Content-Type: application/json" \
  -d '{
        "transactionId": "<uuid>",
        "gatewayStatus": "SUCCESS"
      }'
```

**Example — check status:**

```bash
curl http://localhost:8080/payment/status/<uuid>
```

---

## Testing the Event Flow

### 1. Start infrastructure and the application

```bash
docker compose up -d
mvn spring-boot:run
```

### 2. Create a payment

```bash
curl -X POST http://localhost:8080/payment/initiate \
  -H "Content-Type: application/json" \
  -d '{"amount": 250.00, "paymentDetails": {"type": "UPI", "vpa": "test@oksbi"}}'
```

Note the returned `transactionId`.

### 3. Observe the Kafka event

```bash
MSYS_NO_PATHCONV=1 docker exec -it <kafka_container_name> \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic payflo.payment-initiated \
  --bootstrap-server localhost:9092 --from-beginning
```

### 4. Verify Redis state

```bash
docker exec -it <redis_container_name> redis-cli
HGETALL payflo:payment-transaction:<transactionId>
ZSCORE payflo:processing-payment-transactions:by-started-at <transactionId>
```

### 5. Verify MySQL

```bash
docker exec -it <mysql_container_name> mysql -u payflo_user -p payflo \
  -e "SELECT * FROM payment_transactions WHERE transaction_id = '<transactionId>';"
```

### 6. Confirm the payment and re-check status

```bash
curl -X POST http://localhost:8080/payment/confirm \
  -H "Content-Type: application/json" \
  -d '{"transactionId": "<transactionId>", "gatewayStatus": "SUCCESS"}'

curl http://localhost:8080/payment/status/<transactionId>
```

Verify in Redis that the hash's `status` field is now `COMPLETED`, a `TTL` is set on the key, and the transaction has been removed from the sorted set.

### 7. Trigger and observe the timeout flow

Initiate a payment and simply wait past `PAYMENT_TIMEOUT_BUFFER_MINUTES` without confirming it. Watch the scheduler fire:

```bash
MSYS_NO_PATHCONV=1 docker exec -it <kafka_container_name> \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic payflo.payment-timed-out \
  --bootstrap-server localhost:9092 --from-beginning
```

Then confirm the transaction's Redis status is `TIMED_OUT` and it has been removed from the sorted set.

### 8. Inspect the Dead Letter Topic

```bash
MSYS_NO_PATHCONV=1 docker exec -it <kafka_container_name> \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic payflo.DLT \
  --bootstrap-server localhost:9092 --from-beginning
```

To manually produce a malformed message and trigger DLT routing:

```bash
MSYS_NO_PATHCONV=1 docker exec -it <kafka_container_name> \
  /opt/kafka/bin/kafka-console-producer.sh \
  --topic payflo.payment-timed-out \
  --bootstrap-server localhost:9092
> {"malformed": "payload"}
```

---

## Design Decisions

**Why Kafka over a simpler queue.** payflo needs replayable, ordered, partition-keyed event history — properties a simple message queue doesn't provide by default. Kafka's retention model also means a stalled consumer doesn't lose events, only delays processing.

**Why asynchronous messaging over synchronous service calls.** A synchronous chain (validate → charge → persist → notify, all in one request) ties API latency to the slowest downstream step and offers no natural retry/replay boundary. Firing an event and returning immediately decouples acceptance from processing.

**Why Redis for status reads.** Polling `/payment/status` is the highest-frequency read in the system (a typical frontend polls repeatedly until a terminal state). Serving that from MySQL on every call wastes the database's capacity for genuinely durable operations; Redis absorbs the hot path entirely.

**Why idempotent consumers.** Kafka's at-least-once delivery guarantee means every consumer must tolerate redelivery. `EntityManager.persist()` was chosen deliberately over `JpaRepository.save()` specifically because `save()`'s silent select-then-merge behavior for manually-assigned IDs would silently _update_ on a duplicate delivery rather than reject it — the opposite of the desired idempotency guarantee for the initiating insert.

**Why dead-letter topics.** Deserialization failures are permanent, not transient — retrying a malformed payload will never succeed. Routing straight to a DLT avoids infinite retry loops and gives a place to inspect and diagnose bad messages without blocking the partition.

**Why event choreography over central orchestration.** Each consumer reacts to the event in front of it and produces the next event in the chain; there is no central "saga coordinator" deciding what happens next. This keeps each component small and independently testable, at the cost of the overall flow being implicit rather than centrally visible — an accepted tradeoff at this system's scale.

**Why the scheduler publishes events instead of mutating state directly.** Business logic — updating status, removing from the sorted set, updating MySQL — belongs in exactly one place: the consumer that already owns that logic for every other trigger of the same state transition. If the scheduler mutated state directly, timeout-triggered transitions would follow a different code path than gateway-triggered transitions, doubling the surface area for bugs and drift.

**Why atomic ownership claiming instead of just a DB unique constraint.** A DB constraint alone protects against duplicate _inserts_, but the termination race is fundamentally about which of two competing _update_ paths (gateway confirmation vs. scheduler timeout) gets to own a transaction's outcome — and about surviving a crash mid-cascade (MySQL write done, Redis write not yet done). A single atomic Redis check-and-set, executed via Lua, decides ownership and mutates the timeout-tracking structure in one indivisible step, closing both problems at once.

**Why notification delivery is accepted as at-least-once, not exactly-once.** No atomic mechanism can span an external Kafka publish and a local state flag — there will always be a crash window between "notification sent" and "recorded as sent." Rather than chase an unattainable guarantee, payflo deliberately publishes first and marks the flag after, since a duplicate notification is a strictly better failure mode than a payment confirmation the customer never receives.

**Why notifications are event-driven.** Keeping notification dispatch as its own consumer, subscribed to its own topic, means notification delivery can fail, retry, or be swapped for a real provider (SMS/push/email) without touching the core payment state machine at all.

**Why REST only initiates workflows.** `POST /payment/initiate` and `POST /payment/confirm` both return before any business logic executes — `202 Accepted` with no response body for confirm, specifically to avoid implying a confirmed state that doesn't yet exist. This is a deliberate signal to API consumers that the system is asynchronous by design, not an oversight.

---

## Reliability & Fault Tolerance

**Duplicate message handling — Implemented.** `EntityManager.persist()` + `DataIntegrityViolationException` catch on the initiating consumer; idempotent `UPDATE` on termination consumers.

**Idempotency, DB layer — Implemented.** Unique constraint, atomic at the storage engine.

**Idempotency, Redis/ownership layer — Implemented.** Atomic Lua check-and-set (`TransactionOwnershipService`) resolves both the cross-consumer termination race and crash-retry redelivery.

**Retry behavior — Implemented by design, not by mechanism.** Deserialization failures route to DLT rather than retrying, since retry cannot succeed on a permanently malformed payload.

**Consumer recovery — Implemented.** Kafka's own consumer-group rebalancing; no custom recovery logic required.

**Dead-letter strategy — Implemented.** Single shared `payflo.DLT` via `DeadLetterPublishingRecoverer`.

**Redis consistency under concurrent redelivery — Implemented.** Atomic ownership claim closes the partial-write and cross-consumer race scenarios that were previously out of scope under the single-threaded assumption.

**Database consistency — Implemented.** MySQL remains the durable source of truth; unique constraints enforce non-duplication on the initiating write.

**Event replay — Implemented, by Kafka's nature.** Retained topic history allows reprocessing from any offset.

**Timeout handling — Implemented.** Sorted-set score-based detection, scheduler, and dedicated consumer, with the same atomic ownership claim protecting against a race against a late gateway confirmation.

**Partial-write cascade (DB succeeds, Redis write crashes, redelivery double-notifies) — Implemented.** MySQL treated as naturally idempotent; Redis transitions made atomic via Lua; notification delivery explicitly accepted as at-least-once with dedup pushed to the notification-consumer edge.

---

## Future Improvements

- **Transactional Outbox Pattern** — eliminate the dual-write problem between MySQL and Kafka at its root, rather than relying on idempotency + atomic Redis ops as the current mitigation.
- **Saga Pattern (formalized)** — the project already implements choreography-based event flow; a future iteration could introduce explicit saga state tracking for cross-service consistency if payflo were ever split into real services.
- **Distributed Tracing / OpenTelemetry** — trace a transaction's full journey across REST → Kafka → consumers → Redis/MySQL as a single correlated trace.
- **Prometheus + Grafana** — consumer lag, DLT volume, and timeout-rate dashboards.
- **Testcontainers** — replace manual `docker compose up` preconditions for integration tests with automatically-provisioned, throwaway Kafka/MySQL/Redis containers (deliberately deferred to the project's dedicated testing phase).
- **Schema Registry (Avro/Protobuf)** — replace JSON + trusted-packages deserialization with schema-enforced, evolvable event contracts.
- **Kubernetes** — multi-instance deployment with horizontal consumer scaling per topic.
- **Multi-instance deployment** — validate consumer-group rebalancing and partition reassignment under real horizontal scale, beyond the current single-instance setup.
- **Advanced monitoring / alerting** — DLT-volume and consumer-lag-based alerting integrated with the observability stack above.

---

## Conclusion

payflo demonstrates hands-on fluency with the mechanics that separate "used Kafka" from "understands Kafka": partition-keyed ordering, consumer-group semantics, dead-letter routing, and idempotent processing under at-least-once delivery — all built on a self-managed cluster rather than a managed service, and all reasoned through deliberately rather than defaulted into. Beyond messaging, the project reflects disciplined backend engineering more broadly: single-responsibility service/repository layering, centralized and typed configuration, a consistent exception-handling contract, atomic partial-write and race-condition hardening via Redis-backed Lua scripts, and Redis introduced not as a cache-for-cache's-sake but as a considered architectural response to a specific hot-path problem.
