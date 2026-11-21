# PayFlow — Build Spec

A transaction-processing microservices system in the UBS stack: Java 21, Spring Boot, Kafka, PostgreSQL, Docker Compose, GitHub Actions. Built solo, shippable in ~1 week.

The goal is not a perfect bank. The goal is a real, running, containerized microservices system you can push to GitHub and **defend line-by-line in a UBS interview**. Every choice below is made so you have a true answer to the questions the seniors reported: microservices, REST statelessness, 2-tier vs 3-tier, caching, "walk me through your deployment."

---

## 1. What it does (one paragraph)

Three independent Spring Boot services. **Accounts** owns users and balances. **Transactions** handles a transfer request — validates funds, debits/credits via the Accounts service over REST, then publishes a `transaction.completed` event to **Kafka**. **Ledger** consumes that event asynchronously and writes an immutable audit record. A small **AI categorization** layer labels each transaction ("groceries", "rent", "salary"). Everything runs with one `docker compose up`.

---

## 2. Architecture

```
                 REST                         REST
   [ Client ] ────────▶ [ Transactions ] ───────────▶ [ Accounts ]
                              │                            │
                              │ publishes                  │ owns
                              ▼                            ▼
                          [ Kafka ]                  accounts_db
                        topic: transactions              (Postgres)
                              │
                              │ consumes (async)
                              ▼
                         [ Ledger ] ──▶ ledger_db (Postgres)
                              │
                              └──▶ [ AI categorizer ] (LLM call + rule fallback)
```

**Why this shape matters for the interview:** the Transactions→Accounts hop is *synchronous REST* (you need an immediate yes/no on funds). The Transactions→Ledger hop is *asynchronous via Kafka* (the ledger must never block or fail a transfer, and if Ledger is down the event waits on the topic instead of being lost). Being able to explain **why one is sync and the other is async** is exactly the kind of reasoning UBS probes.

---

## 3. Decisions locked (don't rethink these mid-build)

| Decision | Choice | Why |
|---|---|---|
| Java | 21 LTS | Already installed; Spring Boot 3.x target |
| Build tool | Maven | More common in enterprise/banks than Gradle |
| Inter-service calls | `RestClient` | Modern (Spring 6.1+), simpler than WebClient |
| Persistence | Spring Data JPA + PostgreSQL | Banks are relational-first |
| DB topology | One Postgres container, **one database per service** (`accounts_db`, `transactions_db`, `ledger_db`) | "Database per service" pattern, kept light for solo dev — defensible |
| Messaging | Kafka (KRaft mode, single broker, no Zookeeper) | Modern Kafka; your Big Data + async checkbox |
| Money type | `BigDecimal`, never `double` | Correct for currency — a small detail interviewers respect |
| AI | LLM API call **with rule-based fallback** | Graceful degradation; runs even without an API key |

---

## 4. Repo layout (monorepo)

```
payflow/
├── docker-compose.yml            # infra: postgres, kafka, kafka-ui
├── docker-compose.apps.yml       # the 3 services (Phase 5)
├── init-db.sql                   # creates the 3 databases
├── .github/workflows/ci.yml      # CI (Phase 6)
├── accounts-service/
├── transactions-service/
├── ledger-service/
└── README.md
```

Each `*-service/` is a full standalone Spring Boot app with its own `pom.xml`, `Dockerfile`, and port.

Ports: **Accounts 8081**, **Transactions 8082**, **Ledger 8083**.

---

## PHASE 0 — Infrastructure up first (½ day)

Get Postgres and Kafka running *before* writing any Java. If infra isn't solid, everything downstream is confusing.

### 0.1 `init-db.sql`
```sql
CREATE DATABASE accounts_db;
CREATE DATABASE transactions_db;
CREATE DATABASE ledger_db;
```

### 0.2 `docker-compose.yml` (infra only)
```yaml
services:
  postgres:
    image: postgres:16
    container_name: payflow-postgres
    environment:
      POSTGRES_USER: payflow
      POSTGRES_PASSWORD: payflow
    ports:
      - "5432:5432"
    volumes:
      - ./init-db.sql:/docker-entrypoint-initdb.d/init-db.sql
      - pgdata:/var/lib/postgresql/data

  kafka:
    image: apache/kafka:3.9.1
    container_name: payflow-kafka
    ports:
      - "9092:9092"     # host access (your IDE-run services)
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LISTENERS: HOST://:9092,DOCKER://:29092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: HOST://localhost:9092,DOCKER://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: HOST:PLAINTEXT,DOCKER:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: DOCKER
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: payflow-kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: payflow
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      - kafka

volumes:
  pgdata:
```

> **The one Kafka gotcha to understand now:** there are two listeners. Services you run from VS Code (on the host) connect to `localhost:9092`. Services running *inside* Docker later (Phase 5) connect to `kafka:29092`. Same broker, two doors. If you ever see a Kafka connection error, it's almost always the wrong door for where the client is running.

### 0.3 Bring it up
```bash
docker compose up -d
docker compose ps        # all three healthy
```
Open `http://localhost:8080` — Kafka UI should show an empty cluster. **Phase 0 done when** Kafka UI loads and `docker compose ps` shows postgres + kafka running.

---

## PHASE 1 — Accounts service (1 day)

### 1.1 Generate the skeleton
Go to **start.spring.io**:
- Project: Maven, Language: Java, Spring Boot: 3.4.x (latest stable 3.x)
- Group `com.payflow`, Artifact `accounts-service`, Java 21
- Dependencies: **Spring Web, Spring Data JPA, PostgreSQL Driver, Validation, Spring Boot Actuator, Lombok**

Unzip into `payflow/accounts-service/`.

### 1.2 `application.yml`
```yaml
server:
  port: 8081
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/accounts_db
    username: payflow
    password: payflow
  jpa:
    hibernate:
      ddl-auto: update      # dev only; fine for this project
    show-sql: true
management:
  endpoints:
    web:
      exposure:
        include: health,info    # gives you /actuator/health
```

### 1.3 Domain — `Account` entity
Fields: `id` (UUID, PK), `ownerName` (String), `balance` (BigDecimal), `currency` (String, default "INR"), `createdAt` (Instant).

### 1.4 Layers (standard Spring)
- `AccountRepository extends JpaRepository<Account, UUID>`
- `AccountService` — business logic. Two methods that matter:
  - `credit(UUID id, BigDecimal amount)` — add to balance
  - `debit(UUID id, BigDecimal amount)` — **throw `InsufficientFundsException` if balance < amount** (this exception is your interview talking point on error handling)
- `AccountController` — REST endpoints:

| Method | Path | Purpose |
|---|---|---|
| POST | `/accounts` | create account |
| GET | `/accounts/{id}` | fetch one |
| POST | `/accounts/{id}/credit` | body `{amount}` |
| POST | `/accounts/{id}/debit` | body `{amount}` |

### 1.5 Concurrency note (say this in the interview)
Two transfers hitting the same account at once can race. Add `@Version` (a `Long version` field) to `Account` for **optimistic locking** — JPA then rejects a stale write. One line of code, and it's a strong signal you think about concurrency in a financial system.

**Phase 1 done when:** you can `curl` to create an account, credit it, debit it, and get a 4xx (not a 500) when debiting more than the balance.

---

## PHASE 2 — Transactions service (1.5 days, the core)

This service does the two-hop dance: **synchronous** debit/credit via Accounts, then **asynchronous** publish to Kafka.

### 2.1 Generate skeleton
Same as Phase 1 but Artifact `transactions-service`, and add dependency **Spring for Apache Kafka** (plus Web, JPA, Postgres, Validation, Actuator, Lombok).

### 2.2 `application.yml`
```yaml
server:
  port: 8082
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/transactions_db
    username: payflow
    password: payflow
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
accounts:
  base-url: http://localhost:8081     # externalized — becomes kafka:29092-style config in Docker
```

### 2.3 The event DTO (shared shape between Transactions and Ledger)
```java
public record TransactionEvent(
    UUID transactionId,
    UUID fromAccount,
    UUID toAccount,
    BigDecimal amount,
    String currency,
    Instant timestamp
) {}
```
Copy this same record into the Ledger service (or make a tiny shared module later — for a week build, duplicating one record is fine and honest to say so).

### 2.4 `RestClient` bean to call Accounts
```java
@Bean
RestClient accountsClient(@Value("${accounts.base-url}") String baseUrl) {
    return RestClient.builder().baseUrl(baseUrl).build();
}
```

### 2.5 `TransactionService.transfer(...)` — the heart
Steps, in order:
1. Persist a `Transaction` row with status `PENDING`.
2. Call Accounts **debit** on `fromAccount` (synchronous REST). If it throws (insufficient funds / 4xx), mark `FAILED`, return error. **Do not publish.**
3. Call Accounts **credit** on `toAccount`.
4. Mark the row `COMPLETED`.
5. **Publish** a `TransactionEvent` to Kafka topic `transactions`.

```java
kafkaTemplate.send("transactions", txn.getId().toString(), event);
```

### 2.6 Endpoint
`POST /transactions` body `{ fromAccount, toAccount, amount }` → returns the transaction with its final status.

### 2.7 Idempotency (one more interview trophy)
Accept an optional `Idempotency-Key` header. Store it; if the same key arrives twice, return the original result instead of double-charging. Even a simple unique-column check demonstrates you understand a real payments problem.

**Phase 2 done when:** a `POST /transactions` moves money between two accounts, and you see the event appear in Kafka UI under the `transactions` topic.

---

## PHASE 3 — Ledger service (1 day)

Pure consumer. Never called by REST for writes — it only listens.

### 3.1 Skeleton
Artifact `ledger-service`; dependencies Web (for read endpoints), JPA, Postgres, **Spring for Apache Kafka**, Actuator, Lombok.

### 3.2 `application.yml`
```yaml
server:
  port: 8083
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/ledger_db
    username: payflow
    password: payflow
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: ledger-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
    properties:
      spring.json.trusted.packages: "*"
```

### 3.3 `LedgerEntry` entity
Fields: `id`, `transactionId`, `fromAccount`, `toAccount`, `amount`, `currency`, `category` (filled by AI in Phase 4), `recordedAt`. Treat it as **append-only** — no update/delete endpoints. That immutability is the whole point of a ledger; say so.

### 3.4 The consumer
```java
@KafkaListener(topics = "transactions", groupId = "ledger-service")
public void onTransaction(TransactionEvent event) {
    String category = categorizer.categorize(event);   // Phase 4
    ledgerRepository.save(toEntry(event, category));
}
```

### 3.5 Read endpoints
`GET /ledger` (all) and `GET /ledger/account/{id}` (per account). These prove the async write actually landed.

**Phase 3 done when:** posting a transaction in Phase 2 results, a second later, in a new row visible via `GET /ledger`. Stop the Ledger service, post 3 transactions, restart it — **all 3 still get recorded** (they waited on the topic). That demo is gold in an interview.

---

## PHASE 4 — AI categorization layer (½ day)

Lives inside Ledger. Keep it small and resilient.

### 4.1 `TransactionCategorizer`
```java
public String categorize(TransactionEvent event) {
    try {
        return llmCategorize(event);      // call an LLM API
    } catch (Exception e) {
        return ruleFallback(event);       // never let AI break the ledger
    }
}
```

- `llmCategorize`: a single REST call to any LLM API you have a key for, prompt = "Return one word: the category of this transaction ... respond with only the category." Read the key from an env var `LLM_API_KEY`; if unset, skip straight to fallback.
- `ruleFallback`: simple heuristics (amount ranges, keywords) returning `"transfer"`, `"salary"`, `"bill"`, etc.

This is your honest AI story: it enriches records with an LLM, degrades gracefully, and doesn't sit on the critical money path. That framing ("AI is on the enrichment path, not the transaction path") is a mature thing to say.

**Phase 4 done when:** ledger rows show a non-null `category`, and it still works with `LLM_API_KEY` unset (fallback).

---

## PHASE 5 — Dockerize the services (1 day)

Now the services themselves become containers. This is where `localhost` → `kafka:29092` / `postgres:5432` matters.

### 5.1 `Dockerfile` (same in each service, multi-stage)
```dockerfile
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY . .
RUN ./mvnw clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
ENTRYPOINT ["java","-jar","app.jar"]
```

### 5.2 Externalize the hostnames
Change each `application.yml` to read from env with a localhost default, e.g.:
```yaml
spring:
  datasource:
    url: ${DB_URL:jdbc:postgresql://localhost:5432/accounts_db}
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS:localhost:9092}
accounts:
  base-url: ${ACCOUNTS_URL:http://localhost:8081}
```
So the same jar runs in your IDE (defaults) and in Docker (env overrides). Explaining *why you externalized config* is textbook 12-factor and lands well.

### 5.3 `docker-compose.apps.yml`
Add the three services, each `build:`-ing its folder, on the same network as infra, with env pointing at `postgres:5432`, `KAFKA_BROKERS: kafka:29092`, and `ACCOUNTS_URL: http://accounts-service:8081`. Add `depends_on` for ordering.

Run everything:
```bash
docker compose -f docker-compose.yml -f docker-compose.apps.yml up --build
```

**Phase 5 done when:** the entire system — Postgres, Kafka, all 3 services — comes up with that one command, and a transfer + ledger read works end to end with nothing running in your IDE.

---

## PHASE 6 — GitHub Actions CI (½ day)

### 6.1 Add a few real tests first
At minimum: one unit test on `AccountService.debit` (asserts `InsufficientFundsException`), and one on the categorizer fallback. Even 3–4 tests make "we had CI running tests on every push" a true statement.

### 6.2 `.github/workflows/ci.yml`
```yaml
name: CI
on:
  push:
    branches: [ main ]
  pull_request:
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        service: [accounts-service, transactions-service, ledger-service]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '21'
          cache: maven
      - name: Build & test ${{ matrix.service }}
        working-directory: ${{ matrix.service }}
        run: ./mvnw --batch-mode clean verify
```

**Phase 6 done when:** a push to `main` shows a green check on GitHub, building and testing all three services.

---

## 7. README + GitHub (do this, it's the payoff)

The resume claim only counts once it's public. Your README should have: the architecture diagram (ASCII from section 2 is fine), the one-command run instructions, a short "why sync vs async" paragraph, and a list of the stack. Pin the repo on your profile.

---

## 8. Interview defense map — what each piece answers

| UBS question (from senior reports) | Your PayFlow answer |
|---|---|
| "Draw the system architecture" | The 3-service diagram; you built it |
| "Microservices?" | 3 independent services, own DBs, own ports |
| "How do services talk?" | Sync REST for the funds check; async Kafka for the ledger — and *why* |
| "2-tier or 3-tier?" | 3-tier: client → services → Postgres |
| "REST is stateless — how do you keep state?" | State in Postgres; idempotency key for retries |
| "Handle many users / one service down?" | Kafka buffers events; Ledger can be down and catch up |
| "Caching / distributed caching?" | Explain where you'd add Redis (e.g. account balance reads) and the tradeoff |
| "Walk me through your deployment" | `docker compose up`, multi-stage Dockerfiles, externalized config, CI on push |
| "Design patterns?" | Spring's DI (IoC container), repository pattern, factory beans |
| "Agentic vs Generative AI" | The categorizer is a single generative LLM call on the enrichment path, not agentic — and you can say what would make it agentic |

Prepare the **caching** and **"what would you improve"** answers even though you won't build them — knowing the *next* step signals seniority.

---

## 9. Suggested week schedule

| Day | Work |
|---|---|
| 1 | Phase 0 + Phase 1 (infra + Accounts) |
| 2 | Phase 2 (Transactions, REST + Kafka publish) |
| 3 | Phase 3 (Ledger consumer) + Phase 4 (AI) |
| 4 | Phase 5 (Dockerize all) |
| 5 | Phase 6 (CI) + tests + README |
| 6–7 | Buffer, polish, rehearse the interview answers in section 8 |

If you fall behind, the **non-negotiable core** is Phases 1–3 + 5 (three services, Kafka, Docker Compose). AI and CI are the polish that turns two more skills-line claims true — do them if time allows, but the microservices + Kafka + Docker spine is what has to exist.

---

## 10. Resume line this unlocks

Once it's on GitHub, your project bullet (in place of PicFolio):

**PayFlow | Spring Boot, Kafka, PostgreSQL, Docker**
- Built a transaction-processing microservices system with independent accounts, transactions, and ledger services communicating via REST and Kafka event streaming.
- Designed synchronous funds validation and asynchronous ledger recording, with idempotent transfers and optimistic locking for concurrency safety.
- Containerized the full stack with Docker Compose and automated build/test via GitHub Actions CI.

And these skills become honest: **Spring Boot, Microservices, Kafka, Docker, Docker-Compose, GitHub Actions.**