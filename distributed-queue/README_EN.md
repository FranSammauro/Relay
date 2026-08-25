# Distributed Job Queue

[Versión en español](./README_ES.md)

Distributed, persistent, fault-tolerant job processing system written in
Rust. Conceptually inspired by Celery/BullMQ/Sidekiq, built from scratch to
explore real concurrency and distributed systems problems.

> This project is developed in phases. See [`PHASES.md`](./PHASES.md) for
> current status and the checklist for each phase.

## Architecture (Phase 4)

```
Client --POST /jobs--> API (axum) --> PostgreSQL (source of truth)
                                            ^
                                            |
                                    Worker 1, Worker 2, Worker N
                                    (concurrency + heartbeat +
                                     reaper, each one)
                                            |
                                            v
                                    Redis (heartbeats, TTL)
```

- **PostgreSQL** persists the full state of every job, its attempt
  history, and its leases (see `migrations/`). It's the single source of
  truth — if a worker, the API, or Redis go down, no job is lost.
- **API (axum)** exposes HTTP endpoints to create/query/cancel jobs, check
  their execution history, the queue's aggregate state, and which workers
  are currently alive.
- **Worker** polls the `jobs` table using
  `SELECT ... FOR UPDATE SKIP LOCKED`, which allows multiple workers to
  compete for work without blocking each other or claiming the same job
  twice. Each worker runs several jobs at once (semaphore, `CONCURRENCY`),
  and also runs two background tasks of its own:
  - **Heartbeat**: beats every `HEARTBEAT_INTERVAL_MS` into Redis with a
    TTL, so `GET /workers` knows who's still alive (see ADR-002).
  - **Reaper**: periodically checks for `running` jobs whose lease expired
    (the worker holding them died without saying so) and recovers them
    using the same retry/backoff/DLQ policy as a normal failure (see
    ADR-003 and ADR-004). Runs on **every** worker at once, with no single
    coordinator — kill any of them with `kill -9` mid-job and another one
    recovers it on its own.

Redis is used exclusively for ephemeral coordination (heartbeats). If it
goes down, visibility into who's alive is lost, but recovery of abandoned
jobs keeps working exactly the same, since it runs entirely on PostgreSQL
(`lease_until`) and never depends on Redis — this is explicitly tested in
`crates/common/tests/recovery.rs`, which never spins up Redis.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

This brings up PostgreSQL, Redis, the API on `:8080`, and 3 workers
(configurable with `docker compose up --build --scale worker=N`).

### Try the flow

```bash
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "resize_image", "payload": {"width": 1920, "height": 1080}}'

# => {"id": "...", "status": "pending"}

curl localhost:8080/jobs/<id>
# => status goes pending -> running -> completed

curl localhost:8080/stats
# => {"by_status": {"pending": 0, "completed": 1, ...}}

curl localhost:8080/jobs/<id>/attempts
# => full history of every attempt, with worker, error and outcome

curl localhost:8080/workers
# => who's registered and who's alive right now
```

### Try recovering from a crashed worker

```bash
# artificially slow job, to have time to kill the worker that picks it up
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "sleep", "payload": {"seconds": 25}, "timeout_seconds": 30}'

# check which worker claimed it
curl localhost:8080/jobs/<id> | grep worker_id

# kill it the ugly way
docker kill <container_of_the_worker_that_had_it>

# wait for lease_until plus one reaper cycle (~15s by default) and watch
# another worker recover it
curl localhost:8080/jobs/<id>/attempts
```

### Running locally without Docker

```bash
# requires a running Postgres and Redis instance (see docker-compose.yml for credentials)
cargo run -p api
CONCURRENCY=4 cargo run -p worker
```

Migrations are applied automatically on startup by both the API and the
worker (`sqlx::migrate!`).

### Running the tests

```bash
docker compose up -d postgres redis
cargo test --workspace
```

The concurrency test (`crates/common/tests/concurrency.rs`, 100 jobs / 10
simulated workers), the reliability test
(`crates/common/tests/reliability.rs`, retry → retry → dead_letter), and
the recovery test (`crates/common/tests/recovery.rs`, expired lease →
retry/dead_letter) all need a real Postgres instance — mocking them
wouldn't make sense, since what's being tested is how `SKIP LOCKED` and
the SQL-computed backoff/recovery logic behave under real conditions.
Notably, the recovery test does **not** need Redis (see ADR-002):
recovering abandoned jobs runs entirely on `lease_until` in Postgres. If
no database is available, they skip themselves with a warning instead of
breaking the suite. They always run in CI (see `.github/workflows/ci.yml`).

A note on test isolation: these tests share one Postgres instance and are
written to tolerate that (unique job-type markers per run, tolerance for
jobs claimed by a concurrently-running test). If you run the suite right
after manually poking at the system by hand (e.g. the crash-recovery demo
above) and hit a surprising failure, truncate the tables and try again:

```bash
psql "$DATABASE_URL" -c "TRUNCATE jobs, job_attempts, workers RESTART IDENTITY CASCADE;"
```

## Endpoints (Phase 4)

| Method | Route                | Description                          |
|--------|----------------------|---------------------------------------|
| POST   | `/jobs`              | Create a job                          |
| GET    | `/jobs`              | List jobs (`?status=`, `?limit=`)     |
| GET    | `/jobs/:id`          | Get a job                             |
| GET    | `/jobs/:id/attempts` | Get a job's attempt history           |
| DELETE | `/jobs/:id`          | Cancel a job (only if still pending)  |
| GET    | `/stats`             | Job count by status (`queue_depth`)   |
| GET    | `/workers`           | Registered workers and who's alive now |
| GET    | `/health`            | Liveness                              |
| GET    | `/ready`             | Readiness (checks DB connectivity)    |

## Project status

See [`PHASES.md`](./PHASES.md).

## Architecture decisions

See [`docs/adr/`](./docs/adr).

## Guarantees (evolving by phase)

- **At-least-once delivery**: an accepted job is never silently lost, but
  it may run more than once in the face of failures (see ADR-003). If a
  worker completes a job but dies before the completion is confirmed, the
  lease eventually expires and another worker re-runs it. Handlers should
  be idempotent whenever possible.
- **PostgreSQL as source of truth**: any data critical to system
  correctness lives in PostgreSQL, never only in Redis.
- **Concurrency without loss or duplication**: verified with an
  integration test against a real Postgres instance (100 jobs / 10
  concurrent workers), not just "should work" on paper.
- **Retries with backoff, not infinite retries**: a failed job retries
  with exponential backoff + jitter up to `max_attempts`; after that it
  moves to `dead_letter` and stays there — no silent retry loops that
  could paper over a bug. Verified with an integration test (retry →
  retry → dead_letter) against a real Postgres instance.
- **Recovery from a crashed worker**: if a worker dies mid-job (crash,
  OOM, kill -9), another worker detects the expired lease and recovers it
  using the same retry/DLQ policy. Verified both with an integration test
  (`crates/common/tests/recovery.rs`) and with a real manual test: killing
  a worker with `kill -9` mid-way through a 25-second job and confirming
  another one picks it up and completes it.
