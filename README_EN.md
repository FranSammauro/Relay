# Relay

[Versión en español](./README_ES.md)

Relay is a distributed, persistent, fault-tolerant job processing system
written in Rust. Conceptually inspired by Celery, BullMQ, and Sidekiq, it
was designed from scratch to explore real problems in concurrency and
distributed systems.

> This project was developed in phases. See [`PHASES.md`](./PHASES.md)
> for the full development history and the checklist for each phase.

## Architecture

```
Client --POST /jobs--> API (axum) --> PostgreSQL (source of truth)
          |                                  ^
          |                                  |
     Dashboard (/)                   Worker 1, Worker 2, Worker N
     Metrics (/metrics)              (concurrency + heartbeat +
     relay-cli (CLI)                  reaper + scheduler leader
                                       candidate, each one)
                                            |
                                            v
                                    Redis (heartbeats, TTL,
                                     rate limit counters)
```

- **PostgreSQL** persists the full state of every job, its attempt
  history, its leases, its cron schedules, and the API keys that have
  been issued (see `migrations/`). It is the single source of truth: if a
  worker, the API, or Redis goes down, no job is lost. It is also the
  source of the metrics exposed at `/metrics`, since there are no
  counters accumulated in any process's memory (see below).
- **API (axum)** exposes HTTP endpoints to create, query, and cancel
  jobs, view execution history, the aggregate state of the queue, which
  workers are alive at a given moment, manage cron schedules, a web
  dashboard, and metrics in Prometheus format. Every operational endpoint
  requires authentication through an API key sent in the `Authorization`
  header, and every authenticated request is resolved against a table of
  role-based permissions before it reaches the corresponding handler. In
  addition, each API key has a limit on requests per minute, enforced
  through a fixed-window counter stored in Redis. The full detail of
  authentication, authorization, and rate limiting is described in the
  corresponding section below.
- **Worker** polls the `jobs` table using
  `SELECT ... FOR UPDATE SKIP LOCKED`, which allows multiple workers to
  compete for work without blocking each other or duplicating the claim
  of the same job. Each worker runs several jobs concurrently, bounded by
  a semaphore (`CONCURRENCY`), and also runs three background tasks of
  its own:
  - **Heartbeat**: beats every `HEARTBEAT_INTERVAL_MS` into Redis with a
    TTL, so that `GET /workers` knows who is still alive (see ADR-002).
  - **Reaper**: periodically checks for `running` jobs whose lease has
    expired, meaning the worker that held them died without notice, and
    recovers them by applying the same retry, backoff, and dead-letter
    policy as a normal failure (see ADR-003 and ADR-004). It runs on
    every worker at the same time, with no single coordinator: if any
    worker is killed with `kill -9` in the middle of a job, another one
    recovers it on its own.
  - **Cron scheduler**: competes for a PostgreSQL advisory lock (see
    ADR-006). Whichever worker obtains it becomes the only one that scans
    for and fires due `cron_schedules`, creating the corresponding real
    job.
- **`relay-cli`**: a command-line binary that talks directly to
  PostgreSQL, not to the API, in order to operate the system. It remains
  useful even if the API is down, and it is also the only way to create,
  list, and revoke API keys (see ADR-007 below for the reasoning behind
  that decision).

Redis is used exclusively for ephemeral coordination: worker heartbeats
and rate limit counters. If it goes down, visibility into who is alive is
temporarily lost and rate limiting stops being enforced, but recovery of
abandoned jobs and the cron scheduler keep working exactly the same,
because both run entirely on PostgreSQL and never depend on Redis. This
is explicitly tested in `crates/common/tests/recovery.rs` and
`crates/common/tests/scheduling.rs`, neither of which ever brings up
Redis.

Both the API and the worker handle SIGTERM and Ctrl+C in an orderly way:
the API finishes in-flight requests before shutting down, and the worker
stops claiming new jobs and waits for the ones already running to
finish, up to a maximum deadline defined by `SHUTDOWN_GRACE_SECONDS`,
before exiting.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

This brings up PostgreSQL, Redis, the API on `:8080`, and three workers,
configurable with `docker compose up --build --scale worker=N`.

## Authentication and authorization

The API protects its operational endpoints with API keys. Each key is
generated in the form `dq_<8-character prefix>_<32-byte secret in
base64url>`, for example `dq_a1b2c3d4_xYz123...`. The full key is shown
only once, at the moment it is created: the database only keeps the
prefix, in plain text, and the SHA-256 hash of the full key. Verification
of an incoming key is done in constant time, so as not to leak
information through response timing, and revoking a key takes effect
immediately once its `revoked_at` column is set. The complete reasoning
behind these decisions is documented in
[ADR-007](./docs/adr/ADR-007-api-keys.md).

There are three roles, each with its own set of allowed endpoints:

| Role     | Allowed endpoints |
|----------|--------------------|
| producer | `POST /jobs`, `DELETE /jobs/:id`, and general read access (`/jobs`, `/jobs/:id`, `/stats`, `/metrics`, `/workers`) |
| worker   | Read-only: `GET /jobs`, `/jobs/:id`, `/jobs/:id/attempts`, `/stats`, `/metrics`, `/workers` |
| admin    | Everything above, plus cron schedule management (`GET`/`POST /cron`, `GET`/`DELETE /cron/:id`) |
| public   | `/`, `/health`, `/ready`, no key required |

The web dashboard, served at the root path, is itself public, but its
JavaScript prompts for an API key when it opens, stores it in the
browser's `localStorage`, and includes it in the `Authorization` header
of every request it makes to the API.

### Creating an API key

API keys are managed exclusively through `relay-cli`, which operates
directly against PostgreSQL and does not need a key of its own to work:

```bash
cargo run -p relay-cli -- api-key create --name "my-service" --role producer
# => created: my-service (id ...)
# => key (shown only once, cannot be recovered):
#    dq_xyz12345_abcdefghijklmnopqrstuvwxyz1234567890ABCD
```

The generated key should be saved immediately into the `.env` file of the
client that will use it, or into the appropriate secrets manager, since
there is no way to retrieve it again after this point.

### Rate limiting

Each API key has a limit on requests per minute, configurable through
environment variables:

| Variable                         | Default | Description |
|-----------------------------------|---------|--------------|
| `RATE_LIMIT_PRODUCER_PER_MINUTE`  | 300     | Limit for the producer role (0 means no limit) |
| `RATE_LIMIT_WORKER_PER_MINUTE`    | 300     | Limit for the worker role |
| `RATE_LIMIT_ADMIN_PER_MINUTE`     | 0       | The admin role has no limit by default |

When a key exceeds its limit, the API responds with `429 Too Many
Requests` and includes a `Retry-After` header with the number of seconds
remaining until the next window. If Redis is not available at the moment
the limit is evaluated, the request is allowed through and a warning is
logged: rate limiting is temporarily disabled rather than becoming a
reason for a 500 error across the whole API. The reasoning behind this
decision is documented in
[ADR-008](./docs/adr/ADR-008-rate-limiting.md).

### Trying worker crash recovery

```bash
# an artificially slow job, to have time to terminate the worker that picks it up
KEY=$(cargo run -p relay-cli -- api-key create --name "demo" --role producer 2>&1 | grep -A1 "key (shown" | tail -1 | xargs)
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $KEY" \
  -d '{"type": "sleep", "payload": {"seconds": 25}, "timeout_seconds": 30}'

# check which worker claimed it
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id> | grep worker_id

# terminate that worker abruptly
docker kill <container_of_the_worker_that_had_it>

# wait for lease_until plus one reaper cycle (15 seconds by default) and
# observe how another worker recovers it and runs it again
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id>/attempts
```

### Trying a cron schedule

```bash
ADMIN_KEY=$(cargo run -p relay-cli -- api-key create --name "admin" --role admin 2>&1 | grep -A1 "key (shown" | tail -1 | xargs)
curl -X POST localhost:8080/cron \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -d '{"name": "every-minute", "cron_expr": "* * * * *", "type": "noop"}'

# wait one minute plus one scheduler cycle (10 seconds by default) and
# observe that it fired on its own
curl -H "Authorization: Bearer $ADMIN_KEY" localhost:8080/cron/<id>
# => last_run_at now has a value, next_run_at advanced to the next minute
```

### Trying graceful shutdown

```bash
# submit a slow job and check which worker picked it up
KEY=$(cargo run -p relay-cli -- api-key create --name "demo" --role producer 2>&1 | grep -A1 "key (shown" | tail -1 | xargs)
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $KEY" \
  -d '{"type": "sleep", "payload": {"seconds": 8}, "timeout_seconds": 30}'

# send it the SIGTERM signal (docker stop sends this automatically)
docker kill --signal=TERM <worker_container>
# in a local environment: kill -TERM <pid>

# the worker stops claiming new work, waits for the 8-second job to
# finish completely, and only then closes. Check its logs to see the
# full sequence: worker_draining, worker_drained, worker_stopped.
```

### Using the CLI

```bash
# Jobs
cargo run -p relay-cli -- jobs list --status dead_letter
cargo run -p relay-cli -- jobs attempts <id>
cargo run -p relay-cli -- stats

# Cron
cargo run -p relay-cli -- cron create --name daily-report --expr "0 6 * * *" --type generate_report

# API keys
cargo run -p relay-cli -- api-key create --name "my-service" --role producer
cargo run -p relay-cli -- api-key list
cargo run -p relay-cli -- api-key revoke <prefix>

# Benchmarks
cargo run -p relay-cli -- bench --jobs 1000 --type noop
cargo run -p relay-cli -- --help
```

`relay-cli` talks directly to PostgreSQL rather than the API, so it keeps
working even when the API is down.

### Benchmarks and performance

`relay-cli bench` measures submission, queue, and execution latency using
real data rather than estimates, and requires at least one worker running
against the same database. See
[`docs/performance.md`](./docs/performance.md) for the full report, with
a reproducible methodology and a real finding about an index under
sustained backlog.

### Running locally without Docker

```bash
# requires a running Postgres and Redis instance (see docker-compose.yml for credentials)
cargo run -p api
CONCURRENCY=4 cargo run -p worker
```

Migrations are applied automatically when either the API or the worker
starts (`sqlx::migrate!`).

### Running the tests

```bash
docker compose up -d postgres
cargo test --workspace
```

The concurrency test (`crates/common/tests/concurrency.rs`, 100 jobs and
10 simulated workers), the reliability test
(`crates/common/tests/reliability.rs`, retry, retry, dead-letter), the
recovery test (`crates/common/tests/recovery.rs`, expired lease leading
to retry or dead-letter), and the scheduling test
(`crates/common/tests/scheduling.rs`, cron firing and leadership) all
require a real PostgreSQL instance. There is no point mocking them,
since what is being tested is the actual behavior of `SKIP LOCKED`, the
backoff and recovery logic computed in SQL, and the PostgreSQL advisory
lock, all under real conditions. Notably, neither the recovery test nor
the scheduling test needs Redis (see ADR-002 and ADR-006): both the
recovery of abandoned jobs and the cron scheduler run entirely on
PostgreSQL. If no database is available, the tests skip themselves with a
notice instead of breaking the suite. In CI they always run (see
`.github/workflows/ci.yml`).

A note on isolation: these tests share a single PostgreSQL instance and
are written to tolerate that, through a unique `job_type` or `name` mark
per run and tolerance for jobs claimed by another test running at the
same time. If the suite is run right after manually exercising the
system, for example following one of the demonstrations above, and an
unexpected failure occurs, it is worth truncating the tables and running
the suite again:

```bash
psql "$DATABASE_URL" -c "TRUNCATE jobs, job_attempts, workers, cron_schedules RESTART IDENTITY CASCADE;"
```

## Endpoints

| Method | Path                 | Description                            | Auth / Role              |
|--------|----------------------|-----------------------------------------|---------------------------|
| GET    | `/`                  | Web dashboard (live HTML)               | Public (JS prompts for key) |
| POST   | `/jobs`              | Create a job                            | producer, admin           |
| GET    | `/jobs`              | List jobs (`?status=`, `?limit=`)       | producer, worker, admin   |
| GET    | `/jobs/:id`          | Get a job                               | producer, worker, admin   |
| GET    | `/jobs/:id/attempts` | Attempt history for a job               | producer, worker, admin   |
| DELETE | `/jobs/:id`          | Cancel a job (only while pending)       | producer, admin           |
| GET    | `/stats`             | Job count by status                     | producer, worker, admin   |
| GET    | `/metrics`           | Metrics in Prometheus format            | producer, worker, admin   |
| GET    | `/workers`           | Registered workers and their liveness   | producer, worker, admin   |
| POST   | `/cron`              | Create a cron schedule                  | admin                     |
| GET    | `/cron`              | List cron schedules                     | admin                     |
| GET    | `/cron/:id`          | Get a cron schedule                     | admin                     |
| DELETE | `/cron/:id`          | Delete a cron schedule                  | admin                     |
| GET    | `/health`            | Liveness                                | Public                    |
| GET    | `/ready`             | Readiness (checks database connection)  | Public                    |

To operate without going through HTTP, see `relay-cli` above.

## Project status

See [`PHASES.md`](./PHASES.md).

## Architecture decisions

See [`docs/adr/`](./docs/adr).

## System guarantees

- **At-least-once delivery.** An accepted job is never silently lost,
  although it may end up executing more than once in the face of certain
  failures (see ADR-003). Handlers should be designed to be idempotent
  whenever possible.
- **PostgreSQL as the source of truth.** Any data critical to the
  correctness of the system lives in PostgreSQL, never solely in Redis.
  This includes the metrics exposed at `/metrics`, which are calculated
  at query time and never accumulated in process memory, so a restart
  never loses any data.
- **Concurrency without loss or duplication.** Verified with an
  integration test against a real PostgreSQL instance, running one
  hundred jobs across ten concurrent workers, not merely as a theoretical
  expectation of the design.
- **Retries with backoff, not infinite retries.** A failed job retries
  with exponential backoff and random jitter until `max_attempts` is
  exhausted; from that point it moves to `dead_letter` and stays there,
  with no silent retry loops that could hide a real problem. Verified
  with an integration test that follows the full retry sequence through
  to dead-letter.
- **Recovery from a worker crash.** If a worker terminates abruptly in
  the middle of executing a job, whether from a crash, running out of
  memory, or a forced termination of the process, another worker detects
  the expired lease and recovers the job by applying the same retry and
  dead-letter policy as a normally reported failure. Verified both with
  an integration test (`crates/common/tests/recovery.rs`) and with a
  manual test: forcibly terminating a worker in the middle of a
  twenty-five-second job and confirming that another worker picks it up
  and completes it.
- **Cron firing without duplicates.** A cron schedule is only fired by
  the current leader, determined through a PostgreSQL advisory lock (see
  ADR-006), and the job it creates also carries a derived idempotency
  identifier as an additional layer of safety: a double firing of the
  same time slot does not create two jobs. Verified with an integration
  test and with a manual test: a schedule created through the API fired
  without manual intervention, at the correct time.
- **No in-flight job is interrupted by a deployment.** The SIGTERM signal
  makes the worker stop claiming new work and wait, up to a maximum
  deadline configured through `SHUTDOWN_GRACE_SECONDS`, for jobs already
  in progress to finish before the process exits. Verified with a manual
  test: SIGTERM was sent in the middle of an eight-second job, and the
  worker waited the full eight seconds before shutting down.
- **Authentication and authorization verified end to end.** Every
  operational endpoint requires a valid API key and a role authorized for
  that specific route; a key that does not exist, has been revoked, or
  carries an insufficient role always receives a rejected response.
  Verified with integration tests that cover all three roles against the
  full table of application routes.
