# Banking Kafka Platform – Kafka & Kafka Streams Scenarios

An event-driven core-banking system built with **Java 17, Spring Boot 3.3, Spring Kafka and
Kafka Streams**. Five microservices cooperate only through Kafka topics. Each one shows a
different set of Kafka concepts in a realistic banking use case.

```
                ┌────────────────────┐
  REST ───────► │  account-service   │  idempotent + transactional producers
                └─────────┬──────────┘
      bank.customers ◄────┤ (compacted)
      bank.accounts  ◄────┤ (compacted)
                          ▼
            bank.transactions.requested
                          │
                ┌─────────▼──────────┐   blocking retry ──► bank.transactions.requested.DLT
                │transaction-processor│  exactly-once consume→transform→produce
                └──┬──────────────┬──┘
                   │              └──────────────────────────► bank.transactions.rejected
                   ▼                                                    ▲
         bank.transactions.validated ◄──┐ (transfer credit leg)        │
                   │                    │                              │
                ┌──▼────────────────────┴──┐                           │
                │  ledger-streams-service  │  Processor API ledger ────┘ (INSUFFICIENT_FUNDS)
                │  (Kafka Streams + IQ)    │──► bank.balances (compacted)
                └──┬───────────────────────┘──► bank.transactions.enriched
                   │                         ──► bank.account.activity-summary
                   ▼
         bank.transactions.posted
            │                 │
  ┌─────────▼──────────┐   ┌──▼───────────────────┐
  │fraud-detection-svc │   │ notification-service │ ◄── rejected / alerts / summaries
  │  (Kafka Streams)   │   │ retry topics, batch  │
  └─────────┬──────────┘   └──────────────────────┘
            ├──► bank.fraud.alerts ──────────────────────────► (SMS via retry topics)
            └──► bank.fraud.cases  (HIGH / CRITICAL)
```

## Modules

| Module | Port | Role |
|---|---|---|
| `banking-common` | – | Event records, topic catalogue, JSON serdes, timestamp extractor, topic provisioning |
| `account-service` | 8081 | REST command API: customers, accounts, transactions, transfers, batch payments |
| `transaction-processor` | 8082 | Business validation with exactly-once semantics, retries and dead-letter topic |
| `ledger-streams-service` | 8083 | Kafka Streams ledger: balances, transfers, enrichment, windowed summaries, interactive queries |
| `fraud-detection-service` | 8084 | Kafka Streams real-time fraud rules |
| `notification-service` | 8085 | Customer notifications: non-blocking retries, batch consumption, manual acks |

## Scenario catalogue

### Producer scenarios (`account-service`)

| # | Scenario | Kafka concept | Where |
|---|---|---|---|
| P1 | Every transaction of an account is processed in order | **Message key → partition**, per-partition ordering | `BankingCommandService#submitTransaction` |
| P2 | Network blip must not double-charge a customer | **Idempotent producer** (`enable.idempotence`, `acks=all`, retries) | `application.yml`, `KafkaProducerConfig` |
| P3 | Payroll file: credit all employees or none | **Transactional producer**, `executeInTransaction`, `read_committed` consumers | `EventPublisher#publishAtomically` |
| P4 | Trace one request across five services | **Record headers** (correlation id, source, event type) | `EventPublisher`, propagated in `TransactionRequestListener` |
| P5 | Customer/account master data | **Log-compacted topics** (latest value per key) | `BankingTopicsConfig` |
| P6 | GDPR "right to be forgotten" | **Tombstone** (null value) on a compacted topic | `DELETE /api/customers/{id}` |
| P7 | Non-blocking REST API | **Async send** with `CompletableFuture` callbacks | `EventPublisher#publish` |
| P8 | Throughput tuning | `linger.ms`, `batch.size`, `compression.type=lz4` | `account-service/application.yml` |

### Consumer scenarios (`transaction-processor`, `notification-service`)

| # | Scenario | Kafka concept | Where |
|---|---|---|---|
| C1 | Validate and forward a payment exactly once | **Exactly-once consume-transform-produce** (`KafkaTransactionManager`, offsets in transaction) | `TransactionRequestListener` |
| C2 | Scale validation horizontally | **Consumer group** + `concurrency=3` (one consumer per partition), `CooperativeStickyAssignor` | `TransactionRequestListener`, `application.yml` |
| C3 | Account event not yet visible (eventual consistency) | **Blocking retry with exponential back-off** – keeps ordering | `ErrorHandlingConfig` (`DefaultAfterRollbackProcessor`) |
| C4 | Corrupt JSON must not block the partition | **Poison pill** handling with `ErrorHandlingDeserializer` | `application.yml`, `DltAwareJsonSerializer` |
| C5 | Unrecoverable records kept for investigation | **Dead Letter Topic** (same partition, exception headers, raw bytes preserved) | `ErrorHandlingConfig` |
| C6 | Business rejection vs. technical failure | Rejections are **events** (`bank.transactions.rejected`), errors go to the **DLT** | `TransactionValidator` |
| C7 | Customer double-clicks "Pay" | **Business-key de-duplication**, only marked after commit | `DeduplicationCache`, `TransactionRequestListener` |
| C8 | Local read model of all accounts on every instance | **Compacted-topic cache**, unique group per instance, `seekToBeginning` | `AccountCacheListener` |
| N1 | SMS provider is down – keep processing other alerts | **Non-blocking retries** with `@RetryableTopic` + `@DltHandler` | `FraudAlertListener` |
| N2 | Send push receipts efficiently | **Batch listener** + **manual acknowledgment** | `TransactionReceiptListener`, `ListenerConfig` |
| N3 | Several services read the same topic independently | **Multiple consumer groups** on `bank.transactions.posted` | ledger, fraud, notifications |
| N4 | Observe partition rebalancing | **ConsumerRebalanceListener** | `ListenerConfig.LoggingRebalanceListener` |
| N5 | Use partition/offset/header metadata | `@Header(KafkaHeaders.RECEIVED_PARTITION / OFFSET / custom)` | `RejectedTransactionListener` |

### Kafka Streams scenarios (`ledger-streams-service`)

| # | Scenario | Kafka Streams concept | Where |
|---|---|---|---|
| S1 | Debit only when funds are available | **Processor API** (`FixedKeyProcessor`) + **persistent key-value state store** | `LedgerProcessor` |
| S2 | Never double-book after a crash | `processing.guarantee=exactly_once_v2` (store changelog + output + offsets atomic) | `application.yml` |
| S3 | Route booked vs. declined transactions | **Branching** with `split()` / `Branched` | `LedgerTopology` §2 |
| S4 | Publish balances for other systems | Shared state store read by a second processor → **compacted changelog topic** | `BalanceLookupProcessor` |
| S5 | Transfer between accounts on different partitions | **Re-keying** (`map`) and writing back to the input topic (credit leg) | `LedgerTopology` §4 |
| S6 | Add account type to each transaction | **KStream–KTable join** (co-partitioned topics) | `LedgerTopology` §5 |
| S7 | Add customer name/tier (different key) | **KStream–GlobalKTable join** with foreign-key mapper | `LedgerTopology` §5 |
| S8 | Hourly account activity statement | **Tumbling window aggregation** with grace period + **`suppress()`** (one final result) | `LedgerTopology` §6 |
| S9 | Use transaction time, not arrival time | Custom **TimestampExtractor** (event-time) | `TransactionTimestampExtractor` |
| S10 | "What is my balance?" without a database | **Interactive Queries** + routing to the instance owning the key (`application.server`) | `BalanceQueryService` |
| S11 | Query in-progress windows | **Window store** interactive query | `GET /api/accounts/{id}/activity` |
| S12 | Self-healing stream threads | `StreamsUncaughtExceptionHandler` (`REPLACE_THREAD`), state listener | `StreamsConfig` |

### Kafka Streams scenarios (`fraud-detection-service`)

| # | Rule | Kafka Streams concept | Severity |
|---|---|---|---|
| F1 | **LARGE_AMOUNT** – single transaction ≥ 10 000 | Stateless `filter` + `mapValues` | HIGH |
| F2 | **VELOCITY** – more than 5 debits per minute | **Windowed `count()`** on tumbling windows, caching disabled to see every update | MEDIUM |
| F3 | **IMPOSSIBLE_TRAVEL** – two countries within 30 min | **Processor API** with custom state store, out-of-order safe | CRITICAL |
| F4 | **MONEY_MULE** – large deposit then large debit within 30 min | **KStream–KStream windowed join** (`JoinWindows.before(0)`) | HIGH |
| F5 | Fan-in of all rules, fan-out by severity | `merge()` + `filter` to `bank.fraud.cases` | – |

## Topics

| Topic | Key | Type | Producer → Consumers |
|---|---|---|---|
| `bank.customers` | customerId | compacted | account-service → ledger (GlobalKTable) |
| `bank.accounts` | accountId | compacted | account-service → processor (cache), ledger (KTable) |
| `bank.transactions.requested` | accountId | events | account-service → processor |
| `bank.transactions.requested.DLT` | accountId | events | processor (dead letters) |
| `bank.transactions.validated` | accountId | events | processor, ledger (credit legs) → ledger |
| `bank.transactions.posted` | accountId | events | ledger → fraud, notification |
| `bank.transactions.rejected` | accountId | events | processor, ledger → notification |
| `bank.transactions.enriched` | accountId | events | ledger → (analytics) |
| `bank.balances` | accountId | compacted | ledger → (other systems) |
| `bank.account.activity-summary` | accountId | events | ledger → notification |
| `bank.fraud.alerts` | accountId | events | fraud → notification |
| `bank.fraud.cases` | accountId | events, infinite retention | fraud → (case management) |

All topics have **3 partitions** (configurable via `banking.topics.partitions`). `bank.accounts`
and `bank.transactions.validated` **must** keep the same partition count because they are joined
(co-partitioning). Topics are created at start-up by whichever service starts first.

## Running locally

Requirements: JDK 17+, Maven 3.9+, Docker.

```bash
# 1. Kafka (KRaft, single broker) + Kafka UI on http://localhost:8080
docker compose up -d

# 2. Build everything and run the tests
mvn clean install

# 3. Start the services (one terminal each)
mvn -pl account-service         spring-boot:run
mvn -pl transaction-processor   spring-boot:run
mvn -pl ledger-streams-service  spring-boot:run
mvn -pl fraud-detection-service spring-boot:run
mvn -pl notification-service    spring-boot:run

# 4. Run the guided scenario walkthrough
./scripts/demo.sh
```

Point the services at another cluster with `KAFKA_BOOTSTRAP_SERVERS=host:port`.
`NOTIFICATION_FAILURE_RATE` (default `0.3`) controls how often the simulated SMS gateway fails,
so you can watch alerts travel through `bank.fraud.alerts-retry` and `bank.fraud.alerts-dlt`.

### Useful endpoints

```bash
# Command side (account-service)
POST   :8081/api/customers                 POST :8081/api/accounts
POST   :8081/api/accounts/{id}/freeze      POST :8081/api/accounts/{id}/unfreeze
POST   :8081/api/transactions              POST :8081/api/transfers
POST   :8081/api/transactions/batch        DELETE :8081/api/customers/{id}

# Query side (ledger interactive queries)
GET    :8083/api/balances/{accountId}      GET :8083/api/balances
GET    :8083/api/accounts/{id}/activity?lookBack=PT24H
GET    :8083/api/streams/state

# Notifications sent
GET    :8085/api/notifications?accountId=ACC-1
```

### Scaling out

Start a second instance of a service on another port, e.g.
`mvn -pl ledger-streams-service spring-boot:run -Dspring-boot.run.arguments=--server.port=9083`.
Partitions are rebalanced between instances; balance queries sent to either instance are routed
to the one that owns the account's partition.

## Tests

| Test | Type | What it proves |
|---|---|---|
| `LedgerTopologyTest` | `TopologyTestDriver` | balances, overdraft rejection, duplicate delivery, transfers, joins, suppressed windows |
| `FraudDetectionTopologyTest` | `TopologyTestDriver` | each fraud rule fires (and does not fire) as expected |
| `TransactionValidatorTest` | unit | validation rules |
| `TransactionProcessorIntegrationTest` | `@EmbeddedKafka` | EOS routing, de-duplication, retry → DLT, poison pill with raw bytes preserved |
| `AccountServiceIntegrationTest` | `@EmbeddedKafka` + REST | keying/partitioning, headers, transactional batch |
| `NotificationServiceIntegrationTest` | `@EmbeddedKafka` | listeners for posted, rejected and fraud topics |

```bash
mvn test
```

## Design notes

* **Events are plain JSON** without Java type headers (`spring.json.add.type.headers=false`), so
  the contract is the record schema in `banking-common`, not a Java class name. In production you
  would typically add a schema registry (Avro/Protobuf) for schema evolution.
* **Money** is `BigDecimal` end to end.
* **Validation vs. ledger**: stateless rules live in the processor; anything that needs the
  balance lives in the ledger, which owns that state and is the single writer per account.
* **Exactly-once** covers Kafka-to-Kafka processing. Side effects outside Kafka (SMS, e-mail)
  are at-least-once, so the notification service is designed to tolerate redelivery.
