# FinPay

A transaction-processing **microservices** system: three independent Spring Boot services that move money between accounts, communicating over **synchronous REST** for the funds check and **asynchronous Kafka** for the audit ledger, with an **AI layer** that categorizes each transaction. The whole stack runs with one `docker compose` command.

[![CI](https://github.com/meet244/PayFlow/actions/workflows/ci.yml/badge.svg)](https://github.com/meet244/PayFlow/actions/workflows/ci.yml)

**Stack:** Java 21 · Spring Boot 3.4 · Spring Data JPA · PostgreSQL 16 · Apache Kafka 3.9 (KRaft) · Redis 7 · Docker Compose · GitHub Actions · Google Gemini

---

## Architecture

<img width="2914" height="2125" alt="payflow arch" src="https://github.com/user-attachments/assets/00a0475c-4f8f-40a0-90f6-ca1c95618102" />


### Why sync for one hop and async for the other

The **Transactions → Accounts** hop is **synchronous REST**: a transfer needs an immediate yes/no on funds, and the debit must either succeed or fail the request right now — you can't move money on a "maybe."

The **Transactions → Ledger** hop is **asynchronous via Kafka**: the ledger is an audit trail, not part of the critical money path. It must never block or fail a transfer, and if the Ledger service is down the `transaction.completed` event simply waits on the topic and is recorded when it comes back up — nothing is lost. The AI categorizer lives here too, on the enrichment path, so a slow or unavailable LLM can never affect a payment.

---

## Services

| Service | Port | Database | Responsibility |
|---|---|---|---|
| **accounts-service** | 8081 | `accounts_db` | Owns users and balances; credit/debit with optimistic locking |
| **transactions-service** | 8082 | `transactions_db` | Orchestrates a transfer (sync debit/credit), then publishes to Kafka; idempotent |
| **ledger-service** | 8083 | `ledger_db` | Consumes events, writes immutable audit rows, categorizes via AI |
| _postgres_ | 5432 | — | One container, one database per service |
| _kafka_ (KRaft) | 9092 | — | Single broker, topic `transactions` |
| _kafka-ui_ | 8080 | — | Browse topics/messages at http://localhost:8080 |
| _redis_ | 6379 | — | Caches account and ledger reads |

---

## Run it

**Prerequisites:** Docker Desktop. (For running services from an IDE instead, you also need JDK 21.)

### Everything in Docker (one command)

```bash
docker compose -f docker-compose.yml -f docker-compose.apps.yml up --build
```

This builds all three service images and starts the full stack (Postgres + Kafka + the 3 services), waiting for health before starting dependents. Stop with:

```bash
docker compose -f docker-compose.yml -f docker-compose.apps.yml down
```

### Infra in Docker, services from your IDE

```bash
docker compose up -d          # just postgres, kafka, kafka-ui
```

Then run each service (`./mvnw spring-boot:run` in `accounts-service/`, etc.). The same jars work in both modes — hostnames are externalized (`DB_URL`, `KAFKA_BROKERS`, `ACCOUNTS_URL`) with `localhost` defaults.

### Enable AI categorization (optional)

Without a key, the ledger uses a deterministic rule-based fallback. To use Google Gemini:

```bash
export GEMINI_API_KEY=your_key_here
docker compose -f docker-compose.yml -f docker-compose.apps.yml up -d
```

---

## API

### Accounts (8081)

| Method | Path | Body |
|---|---|---|
| POST | `/accounts` | `{ "ownerName": "Alice", "currency": "INR" }` |
| GET | `/accounts/{id}` | — |
| POST | `/accounts/{id}/credit` | `{ "amount": 1000 }` |
| POST | `/accounts/{id}/debit` | `{ "amount": 100 }` (422 if insufficient) |

### Transactions (8082)

| Method | Path | Notes |
|---|---|---|
| POST | `/transactions` | `{ fromAccount, toAccount, amount, merchant?, description? }`; optional `Idempotency-Key` header |
| GET | `/transactions/{id}` | — |

### Ledger (8083)

| Method | Path |
|---|---|
| GET | `/ledger` |
| GET | `/ledger/account/{id}` |

Each service also exposes `GET /actuator/health`.

### Example flow

This walks through every service end to end: **Accounts** (create/fund/read), **Transactions** (transfer, idempotency, funds check), **Kafka** (the event), and **Ledger** (the async, AI-categorized audit rows).

```bash
# 1) ACCOUNTS — create a sender and a receiver, fund the sender, read a balance
FROM=$(curl -s -X POST localhost:8081/accounts -H 'Content-Type: application/json' \
  -d '{"ownerName":"Alice"}' | jq -r .id)
TO=$(curl -s -X POST localhost:8081/accounts -H 'Content-Type: application/json' \
  -d '{"ownerName":"Bob"}' | jq -r .id)
curl -s -X POST localhost:8081/accounts/$FROM/credit -H 'Content-Type: application/json' \
  -d '{"amount":5000}'
curl -s localhost:8081/accounts/$FROM | jq        # balance: 5000

# 2) TRANSACTIONS — transfer money. This does a SYNC debit+credit against Accounts
#    over REST, then publishes a transaction.completed event to Kafka.
#    merchant/description give the Ledger's AI categorizer something to classify on.
curl -s -X POST localhost:8082/transactions -H 'Content-Type: application/json' \
  -d "{\"fromAccount\":\"$FROM\",\"toAccount\":\"$TO\",\"amount\":780,\"merchant\":\"Swiggy\",\"description\":\"Food delivery\"}" | jq

# money moved: Alice 4220, Bob 780
curl -s localhost:8081/accounts/$FROM | jq .balance
curl -s localhost:8081/accounts/$TO   | jq .balance

# 3) IDEMPOTENCY — the same Idempotency-Key returns the ORIGINAL transaction;
#    money is never moved twice. Run this twice and compare the ids.
curl -s -X POST localhost:8082/transactions -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: rent-april' \
  -d "{\"fromAccount\":\"$FROM\",\"toAccount\":\"$TO\",\"amount\":1200,\"merchant\":\"Landlord\",\"description\":\"April rent\"}" | jq .id
curl -s -X POST localhost:8082/transactions -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: rent-april' \
  -d "{\"fromAccount\":\"$FROM\",\"toAccount\":\"$TO\",\"amount\":1200,\"merchant\":\"Landlord\",\"description\":\"April rent\"}" | jq .id

# 4) FUNDS CHECK — a transfer bigger than the balance is rejected with HTTP 422,
#    the debit never happens, and NO event is published.
curl -s -o /dev/null -w 'HTTP %{http_code}\n' -X POST localhost:8082/transactions \
  -H 'Content-Type: application/json' \
  -d "{\"fromAccount\":\"$FROM\",\"toAccount\":\"$TO\",\"amount\":999999}"

# 5) KAFKA — see the transaction.completed events on the topic.
#    In the browser: http://localhost:8080  (Kafka UI -> topic "transactions")
#    Or from the CLI:
docker exec payflow-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transactions --from-beginning --timeout-ms 5000

# 6) LEDGER — the async consumer has written immutable, AI-categorized audit rows.
curl -s localhost:8083/ledger | jq                # all entries (category=food, rent, ...)
curl -s localhost:8083/ledger/account/$FROM | jq  # just this account's history
```

> **Resilience demo:** stop the ledger (`docker compose ... stop ledger-service`), post a few transfers, then start it again — the buffered events wait on the Kafka topic and are all recorded on catch-up. The ledger being down never blocks a transfer.

---

## Design highlights

- **`BigDecimal` for money** everywhere — never `double`.
- **Optimistic locking** (`@Version`) on accounts so two concurrent transfers can't clobber a balance.
- **Idempotency** — a repeated `Idempotency-Key` returns the original transaction instead of double-charging.
- **Compensating refund** — if the credit leg fails after a successful debit, the sender is refunded so money is never lost.
- **Append-only ledger** — no update/delete paths; that immutability is the point.
- **AI on the enrichment path, not the money path** — a single Gemini call classifies each transaction into a fixed set of categories, with graceful degradation to rules when the key is unset or the call fails, and a no-cost `other` short-circuit when there's nothing to classify on.
- **Redis read caching with write-eviction** — `GET /accounts/{id}` and the ledger reads (`GET /ledger`, `/ledger/account/{id}`) are cached in Redis and **evicted on every write**: a credit/debit drops the account key; a newly recorded transaction drops the ledger list and both accounts' per-account lists. Reads scale, and a cache never serves data older than the last write.
- **Externalized config (12-factor)** — the same jar runs in the IDE and in Docker.

---

## Testing & CI

Unit tests run on every push and pull request via **GitHub Actions** (`.github/workflows/ci.yml`), building and testing all three services on Java 21:

```bash
cd accounts-service && ./mvnw clean verify   # same for the other two
```

---

## What I'd add next

- A **shared events module** instead of duplicating the `TransactionEvent` record per service.
- **Testcontainers** integration tests to bring back full-context tests in CI.
- **Observability** — distributed tracing across the REST + Kafka hops.

---

## Repo layout

```
payflow/
├── docker-compose.yml          # infra: postgres, kafka, kafka-ui
├── docker-compose.apps.yml     # the 3 services
├── init-db.sql                 # creates the 3 databases
├── .github/workflows/ci.yml    # CI
├── accounts-service/
├── transactions-service/
└── ledger-service/
```
