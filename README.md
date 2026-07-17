# HM Dianping — Redis High-Concurrency Edition

A local lifestyle review platform built with Spring Boot, MySQL, and Redis. In addition to shop discovery, user authentication, reviews, social following, and feeds, this version focuses on multi-level caching, dynamic hot-key detection, asynchronous flash-sale ordering, concurrent order-state control, and failure compensation.

The repository also includes reproducible benchmark scripts, raw results, and a performance report.

## Highlights

- **Multi-level caching:** Implements a `Caffeine → Redis → MySQL` lookup path with Cache Aside, null-value caching, and logical expiration to handle cache penetration and hot-key expiration.
- **Dynamic hot-key detection:** Uses a Redis Lua sliding window to atomically measure access frequency and promote hot data to Caffeine.
- **Cross-instance invalidation:** Broadcasts local-cache invalidation events through Redis Stream when shop data changes.
- **Safe cache rebuilding:** Uses Redisson distributed locks and a bounded thread pool to rebuild expired cache entries asynchronously while serving stale-but-valid data during lock contention.
- **High-concurrency flash sales:** Uses Redis Lua to atomically validate stock, enforce one order per user, reserve inventory, and append order events to Redis Stream.
- **Reliable order consumption:** Supports Consumer Groups, transaction-before-ACK semantics, Pending List recovery, bounded retries, dead-letter streams, and inventory/eligibility compensation.
- **Order-state management:** Closes expired unpaid orders with Spring Scheduling and resolves payment-versus-close races through conditional CAS updates.
- **Social and location features:** Supports review likes, following feeds, mutual follows, daily check-ins, and Redis GEO-based nearby-shop queries.

## Tech Stack

| Category | Technologies |
| --- | --- |
| Backend | Java 8+, Spring Boot 2.3.12, Spring MVC |
| Persistence | MyBatis-Plus 3.4.3, MySQL 8 |
| Cache and messaging | Redis, Lettuce, Caffeine, Redis Stream |
| Concurrency | Redis Lua, Redisson, CAS, thread pools |
| Frontend | Vue 2, Element UI, Axios, Nginx |
| Testing | JUnit, Python HTTP benchmarks |

## Architecture

```text
Browser
   │
   ▼
Nginx :8080 ── /api/* ──▶ Spring Boot :8081
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
          Caffeine / Redis             MySQL :3307
                 │                         ▲
                 └── Redis Stream ─────────┘
                    Orders / invalidation
```

## Project Structure

```text
.
├── backend/                       # Spring Boot backend
│   ├── src/main/java/com/hmdp/
│   │   ├── controller/            # HTTP endpoints
│   │   ├── service/               # Business logic and consumers
│   │   ├── mapper/                # MyBatis-Plus mappers
│   │   ├── config/                # MVC, Redisson, and database config
│   │   └── utils/                 # Cache, locks, and ID generation
│   ├── src/main/resources/
│   │   ├── db/hmdp.sql            # Schema and sample data
│   │   └── *.lua                  # Flash sale, compensation, hot keys
│   └── benchmarks/                # Scripts, raw results, and report
├── frontend/                      # Vue static pages and Nginx
└── PROJECT_TARGET_CHECKLIST.md    # Implementation and validation notes
```

## Getting Started

### 1. Prerequisites

- JDK 8 or 11 (JDK 11 recommended)
- Maven 3.6+
- MySQL 8.0
- Redis 6.2+
- Nginx
- Python 3, only for benchmarks

The default local configuration is:

| Service | Address | Credentials |
| --- | --- | --- |
| MySQL | `127.0.0.1:3307/hmdp` | `root / 123456` |
| Redis | `127.0.0.1:6380` | No password |
| Backend | `http://127.0.0.1:8081` | — |
| Frontend | `http://127.0.0.1:8080` | — |

If your local environment differs, update `backend/src/main/resources/application.yaml`. Redisson addresses are currently configured in `backend/src/main/java/com/hmdp/config/RedissonConfig.java`.

### 2. Start MySQL and Redis

Use existing local services or start containers:

```bash
docker run -d --name hmdp-mysql \
  -p 3307:3306 \
  -e MYSQL_ROOT_PASSWORD=123456 \
  -e MYSQL_DATABASE=hmdp \
  mysql:8.0

docker run -d --name hmdp-redis \
  -p 6380:6379 \
  redis:6.2-alpine
```

Import the schema and sample data:

```bash
docker exec -i hmdp-mysql \
  mysql -uroot -p123456 hmdp \
  < backend/src/main/resources/db/hmdp.sql
```

At startup, the application also adds the order uniqueness index and expired-order scan index if they do not already exist.

### 3. Start the Backend

```bash
cd backend
mvn spring-boot:run
```

Alternatively, build and run the packaged application:

```bash
cd backend
mvn clean package
java -jar target/hm-dianping-0.0.1-SNAPSHOT.jar
```

### 4. Start the Frontend

On macOS or Linux:

```bash
nginx -p "$PWD/frontend/" -c conf/nginx.conf
```

On Windows, run the bundled executable from the `frontend` directory:

```powershell
.\nginx.exe
```

Open `http://localhost:8080`. Nginx serves the frontend and proxies `/api/*` requests to `http://127.0.0.1:8081`.

> SMS delivery is simulated in development. After requesting a verification code, read it from the backend console.

## Core Workflows

### Shop Caching

```text
Request → Caffeine → Redis → MySQL
             │          │
             │          ├─ Null-value caching
             │          └─ Logical expiration
             └─ Hot-key promotion
                          └─ Redisson lock + async rebuild
```

After a successful shop update, the service deletes the Redis entry, invalidates the local cache, and broadcasts an invalidation event to other instances. Failed invalidations enter a retry flow, with TTL serving as the final consistency safeguard.

### Flash-Sale Ordering

```text
Request
  └─ Lua: validate stock + enforce one order + reserve stock
       └─ XADD stream.orders
            └─ Consumer Group
                 └─ Conditional MySQL stock update + create order
                      ├─ Success: ACK
                      └─ Failure: retry → dead letter → compensate
```

A unique `(user_id, voucher_id)` database index prevents duplicate orders. Stock updates require `stock > 0`, while order IDs and the unique index make Stream consumption idempotent.

## Testing

Run the backend test suite:

```bash
cd backend
mvn test
```

Run the cache benchmark:

```bash
cd backend
python3 benchmarks/http_benchmark.py cache \
  --requests 10000 \
  --concurrency 100 \
  --shop-id 1
```

Before running the flash-sale benchmark, create an active test voucher, initialize its Redis stock, and add tokens for distinct users to `backend/tokens.txt`:

```bash
cd backend
python3 benchmarks/http_benchmark.py seckill \
  --requests 1000 \
  --concurrency 100 \
  --voucher-id 90001 \
  --tokens tokens.txt
```

See the [benchmark instructions](backend/benchmarks/README.md) for preparation and correctness checks.

## Performance Results

The following results were measured on a local macOS development machine using JDK 11 and Docker-based MySQL/Redis. They describe a controlled benchmark and should not be interpreted as production capacity.

| Scenario | QPS | P99 |
| --- | ---: | ---: |
| Direct MySQL | 5,362.81 | 43.88 ms |
| Redis-only cache | 6,313.59 | 36.56 ms |
| Caffeine + Redis | 8,232.77 | 30.31 ms |
| Synchronous flash-sale ordering | 861.45 | 408.81 ms |
| Lua + Stream asynchronous ordering | 3,668.06 | 68.59 ms |

In a test where 1,000 distinct users competed for 200 units, the system created exactly 200 orders with zero overselling and zero duplicate orders. Both Redis and MySQL ended with zero remaining stock. The asynchronous version delivered 4.26× the throughput of the synchronous baseline and reduced P99 latency by 83.22%.

See the [performance and correctness report](backend/benchmarks/PERFORMANCE_REPORT.md) for raw results, methodology, and test limitations.

## Common Endpoints

| Feature | Method and path |
| --- | --- |
| Send code / log in | `POST /user/code`, `POST /user/login` |
| Get shop details | `GET /shop/{id}` |
| Find nearby shops | `GET /shop/of/type` |
| Publish / browse reviews | `POST /blog`, `GET /blog/hot` |
| Follow / mutual follows | `PUT /follow/{id}/{isFollow}`, `GET /follow/common/{id}` |
| Following feed | `GET /blog/of/follow` |
| Daily check-in | `POST /user/sign` |
| Place a flash-sale order | `POST /voucher-order/seckill/{id}` |
| Pay for an order | `POST /voucher-order/{id}/pay` |
| Cache metrics | `GET /shop/cache/metrics` |
| Order-consumer metrics | `GET /voucher-order/metrics` |

Prefix paths with `/api` when accessing them through the frontend. Do not add the prefix when calling the backend directly on port `8081`.

## Notes

- The repository contains local development database credentials. Use environment variables or external configuration before deployment.
- `backend/tokens.txt` is a benchmark fixture and must not contain real credentials.
- `RedissonConfig` also defines clients for ports `6381` and `6382` for multi-node locking tests. The main application flow only requires Redis on `6380`; start the additional instances when running the related Redisson tests.
- Uploaded images are stored under the frontend's static-resource directory by default. Use object storage or a dedicated file service in production.

## Acknowledgements

This project extends the HM Dianping example from the Heima Redis course. It is intended for learning cache consistency, high-concurrency flash sales, asynchronous message processing, and concurrent order-state management.
