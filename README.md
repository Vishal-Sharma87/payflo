
# payflo

**An event-driven payment processing backbone, built on self-managed Apache Kafka.**

payflo simulates the system that sits between "a payment was requested" and "a payment reached its final state" — the part of a fintech platform that has to stay correct under duplicate deliveries, out-of-order network calls, and partial failures, without a synchronous call chain holding it together. Actual money movement (bank calls, card networks, UPI rails) is mocked. Everything around it — event production, consumption, ordering, partitioning, dead-letter handling, and idempotent persistence — is real, running against a self-managed Kafka cluster rather than a managed platform.

---

## Why this exists

Most Kafka experience gained through managed platforms (Confluent Cloud, MSK) hides the parts that actually teach you how the system behaves — cluster bootstrapping, listener configuration, consumer group rebalancing, offset management, controller quorum setup. payflo is deliberately built on a self-managed, Docker-based Kafka cluster (KRaft mode, no ZooKeeper) and hand-rolled Spring Kafka configuration — no autoconfiguration — specifically so those mechanics stay visible instead of abstracted away.

The domain is payments because it's an honest test case for event-driven design: a payment has a strict lifecycle, ordering genuinely matters per-transaction, duplicate processing has real consequences, and "eventually consistent" isn't good enough — the system has to reach one unambiguous final state, reliably, every time.

---

## The core idea: no orchestrator, just events

There is no central service that walks a payment through its lifecycle step by step. An API fires _one_ event describing what just happened; a consumer reacts to that event, does its job, and — if the lifecycle continues — fires the _next_ event. Each stage only knows about the stage immediately before it, never the whole chain.

```
POST /payment/initiate
        │
        ▼
 payflo.payment-initiated ──► PaymentInitiatedConsumer ──► persist + fire notification
                                                                    │
POST /payment/confirm                                              ▼
        │                                          payflo.notification.payment-initiated
        ▼
 payflo.payment-received / payflo.payment-failed
        │
        ▼
 PaymentReceivedConsumer / PaymentFailedConsumer ──► update state + fire notification
```

**Why this matters:** a synchronous orchestrator (`if step1 succeeds, call step2, call step3...`) puts the availability of the _entire_ payment flow at the mercy of every individual step's uptime, and makes "retry just the failed step" hard to reason about. An event-driven chain means each stage can fail, restart, or replay independently — the persisted event _is_ the record of what needs to happen next, not something held in a service's memory.

---

## Request flow

### 1. Initiate a payment — `POST /payment/initiate`

Client submits an amount and payment method details (UPI VPA or card). The API validates the request structurally (see [Validation](#validation-real-checks-not-a-format-stub)), generates a time-ordered `transactionId` (UUIDv7), and fires `payflo.payment-initiated` — nothing is written to a database at this point.

```json
{
  "amount": 1500.0,
  "paymentDetails": {
    "type": "UPI",
    "vpa": "someone@okaxis"
  }
}
```

A consumer, listening independently, picks up that event, persists the transaction, and fires a notification event. The API's job ends at "I've validated this and told the system to start" — it does not wait for persistence to complete.

**Why the API doesn't persist:** if `/payment/initiate` wrote to the database directly, the endpoint's reliability would be bound to the database's availability at that exact instant, and a slow write would make the API slow. Firing an event and returning immediately decouples "accepting the request" from "recording it" — the event itself is the durable handoff.

### 2. Confirm via gateway (mocked) — `POST /payment/confirm`

Simulates a payment gateway's webhook — the thing that would normally arrive asynchronously from a real bank or UPI switch. Reports one of two outcomes, and fires the corresponding event:

```json
{
  "transactionId": "019...",
  "status": "RECEIVED"
}
```

Returns `202 Accepted` with no response body — deliberately. The actual state change (marking the transaction complete or failed) happens later, in a consumer. Returning a "confirmed" status here would claim something the API hasn't actually verified yet.

**Why success and failure are separate topics, not one topic with a status field:** a single `payment-outcome` topic carrying a status enum would force every consumer to branch on that field, and adding a third outcome later means editing that branch. Two topics means adding a new outcome is _purely additive_ — a new topic and a new consumer, touching nothing that already works.

### 3. Poll status — `GET /payment/status/{transactionId}`

Read-only. Fires no events — a status check is an observation, not something that should ripple through the system as if it were new information.

```json
{
  "data": {
    "transactionId": "019...",
    "status": "PROCESSING",
    "message": "Your payment is being processed."
  },
  "currentTime": "2026-07-23T10:00:00Z"
}
```

### 4. Discover payment options — `GET /payment/options`

Returns the set of supported payment methods, derived directly from the system's own `PaymentType` enum — not a hardcoded list maintained separately, so the endpoint can never drift out of sync with what `/payment/initiate` actually accepts.

---

## Validation: real checks, not a format stub

Payment details are validated structurally before anything reaches Kafka — a request that fails validation produces **no event at all**, because there's nothing yet worth recording.

- **UPI VPA** — checked against the actual `identifier@handle` structure NPCI uses: identifier must be 3–60 characters (lowercase letters, digits, `.`, `-`, `_`), and the handle must match one of a known set of real PSP handles (`okaxis`, `ybl`, `paytm`, `airtel`, and others) rather than any arbitrary string.
- **Card number** — validated with **Luhn's algorithm**, the actual checksum real card issuers use to catch transcription errors, not just a length check.
- **Expiry** — modeled as `YearMonth` (no day component, matching how expiry actually works), checked against the current month.
- **CVV** — 3 or 4 digits, stored and validated as a string (not numeric) so a leading-zero CVV like `007` isn't silently corrupted.

**Why this matters for the story this project tells:** it would be easy to stub validation as "not null" and move on — the goal here isn't to build a production-grade validator, it's to demonstrate that the _shape_ of business logic is taken seriously even in a project whose real focus is Kafka. Every check is deliberately scoped to structural/local validation only — no external API calls, no real bank network lookups — because verifying a VPA or card is _actually active_ is a different problem than this project is solving.

---

## Kafka topology

Single broker, KRaft mode, 3 partitions per topic, replication factor 1 (the only valid value on a single-broker cluster).

| Topic                                   | Fired by                                 | Purpose                                         |
| --------------------------------------- | ---------------------------------------- | ----------------------------------------------- |
| `payflo.payment-initiated`              | `/payment/initiate`                      | Payment request accepted and validated          |
| `payflo.notification.payment-initiated` | `PaymentInitiatedConsumer`               | Initiation notification                         |
| `payflo.payment-received`               | `/payment/confirm` (success)             | Gateway reported a successful outcome           |
| `payflo.notification.payment-completed` | `PaymentReceivedConsumer`                | Completion notification                         |
| `payflo.payment-failed`                 | `/payment/confirm` (failure)             | Gateway reported a failed outcome               |
| `payflo.notification.payment-failed`    | `PaymentFailedConsumer`                  | Failure notification                            |
| `payflo.payment-timed-out`              | Timeout monitor _(Phase 4.3)_            | Transaction exceeded its processing window      |
| `payflo.notification.payment-timed-out` | `PaymentTimedoutConsumer`                | Timeout notification                            |
| `payflo.DLT`                            | Spring (`DeadLetterPublishingRecoverer`) | Shared dead-letter topic for unparseable events |

**Partition key is always `transactionId`.** Every event belonging to the same transaction lands on the same partition — Kafka only guarantees ordering _within_ a partition, so this is what makes "this transaction's events are processed in the order they happened" a guarantee rather than a hope. Unrelated transactions land on different partitions and process fully in parallel.

**One shared `payflo.DLT`, not eight per-topic dead-letter topics.** Handling a dead letter — log it, alert on it, replay it manually — doesn't depend on which topic it came from; the _business logic_ differs per notification topic, but dead-letter handling is generic by nature, so one shared topic avoids eight near-identical topics that would never diverge in practice.

---

## Every event knows its own topic and key

Rather than every caller hardcoding a topic string, each event type self-reports where it belongs:

```java
public interface PaymentEvent {
    KafkaTopic topic();
    String key();
}

public record PaymentReceivedEvent(UUID transactionId) implements PaymentEvent {
    public KafkaTopic topic() { return KafkaTopic.PAYMENT_RECEIVED; }
    public String key() { return transactionId.toString(); }
}
```

Publishing anywhere in the codebase is always the same one line:

```java
eventPublisher.publish(new PaymentReceivedEvent(transactionId));
```

**Why this matters:** topic names as raw strings scattered across the codebase are a typo waiting to happen — a mismatched string silently sends an event nowhere useful. Centralizing topic names in one `@ConfigurationProperties`-bound record, resolved through a single `KafkaTopicResolver`, means every topic name is defined exactly once, sourced from configuration, and never re-typed.

---

## Idempotency and failure handling

- **Persistence uses `EntityManager.persist()`, not `JpaRepository.save()`.** With a manually-assigned primary key (UUIDv7, not database-generated), Hibernate's `save()` can't tell "new" from "existing" — it silently does a select-then-merge instead of a clean insert. `persist()` is insert-only: a genuine duplicate throws `DataIntegrityViolationException`, which is caught, logged, and skipped. This is what actually makes redelivery-safe processing possible.
- **Malformed messages never block a partition.** A deserialization failure is caught per-record (`ErrorHandlingDeserializer`) and routed straight to `payflo.DLT` with zero retries — retrying malformed JSON never succeeds, so retrying it is just wasted work standing in the way of the next message.
- **Every failure path returns the same response shape.** A custom exception hierarchy (`PayfloException` → concrete exceptions, each carrying a machine-readable `ErrorCode`) feeds a single global exception handler, so a client never has to parse two different error formats depending on whether a business rule or a framework-level check failed.

```json
{
  "message": "The specified VPA payment service provider is not supported.",
  "errorCode": "VPA_UNKNOWN_PAYMENT_SERVICE_PROVIDER",
  "timestamp": "2026-07-23T10:00:00Z"
}
```

---

## Tech stack

| Layer              | Choice                                     | Why                                                                                      |
| ------------------ | ------------------------------------------ | ---------------------------------------------------------------------------------------- |
| Language           | Java 21+                                   | Sealed interfaces, pattern-matching switch, records — used throughout, not just for show |
| Framework          | Spring Boot                                | Kafka/JPA wiring hand-rolled rather than autoconfigured, to keep the mechanics visible   |
| Message broker     | Apache Kafka (self-managed, KRaft, Docker) | The entire point of the project                                                          |
| Primary datastore  | MySQL                                      | Durable transaction history                                                              |
| Cache / fast-state | Redis                                      | In-flight state, sorted-set timeout queries _(integration in progress)_                  |
| Build tool         | Maven                                      |                                                                                          |
| ID generation      | UUIDv7 (`uuid-creator`)                    | Time-ordered, sortable, no coordination required to generate                             |
| Testing            | JUnit, Mockito                             |                                                                                          |

Single Spring Boot monolith, deliberately — microservices would introduce networking and deployment concerns that have nothing to do with learning Kafka, and would dilute the actual focus of this project.

---

## Running it locally

**Prerequisites:** Docker + Docker Compose, Java 21+, Maven, Bash (Git Bash/WSL on Windows).

```bash
# Starts Kafka, MySQL, Redis, and creates all topics
./scripts/start-dev.sh    # or start-dev.bat on Windows

# Starts the application
mvn spring-boot:run
```

Topic creation runs as a one-time infrastructure step after the broker is up — not embedded in `docker-compose.yml` (infrastructure shouldn't encode business/domain configuration) and not triggered at application startup (topic topology shouldn't be owned by app boot).

---

## Project status

- [x] Requirements, design, local Kafka infrastructure (Docker, KRaft, MySQL, Redis)
- [x] Hand-rolled Kafka producer/consumer configuration
- [x] All 8 consumers — happy path, duplicate-delivery idempotency, malformed-payload DLT routing
- [x] Full REST API layer — `/payment/initiate`, `/payment/confirm`, `/payment/status`, `/payment/options`
- [x] Structural validation — UPI VPA format + PSP allow-list, card number (Luhn), expiry, CVV
- [x] Centralized Kafka topic resolution, event self-reporting, notification event pipeline
- [x] Unified exception handling — custom business exceptions and framework-level exceptions, one response contract
- [ ] Real Redis integration (currently simulated)
- [ ] Timeout monitor scheduler
- [ ] Partial-write idempotency across DB + Redis + notification dispatch (race-condition hardening)
- [ ] Kafka behavior experiments (consumer failure, offset replay, partition rebalancing)
- [ ] Test suite

---

## Built for

This project exists to demonstrate real, hands-on fluency with event-driven systems and self-managed Kafka — the kind of depth that's hard to fake in an interview — with a focus on backend/SDE roles at companies operating in payments and fintech.
