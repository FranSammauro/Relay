# Distributed Job Queue

[Versión en español](./README_ES.md)

Distributed, persistent, fault-tolerant job processing system written in
Rust. Conceptually inspired by Celery/BullMQ/Sidekiq, built from scratch to
explore real concurrency and distributed systems problems.

> This project is developed in phases. See [`PHASES.md`](./PHASES.md) for
> current status and the checklist for each phase.

## Architecture (Phase 3)

```
Client --POST /jobs--> API (axum) --> PostgreSQL (source of truth)
                                            ^
                                            |
                                    Worker 1, Worker 2, Worker N
                                    (each with its own pool of
                                     concurrent tasks)
```

- **PostgreSQL** persists the full state of every job and its attempt
  history (see `migrations/`). It's the single source of truth — if a
  worker or the API go down, no job is lost.
- **API (axum)** exposes HTTP endpoints to create/query/cancel jobs, check
  their execution history, and see the queue's aggregate state.
- **Worker** polls the `jobs` table using
  `SELECT ... FOR UPDATE SKIP LOCKED`, which allows multiple workers to
  compete for work without blocking each other or claiming the same job
  twice. Each worker runs several jobs at once, bounded by a semaphore
  (`CONCURRENCY`). Each execution runs under a hard timeout
  (`timeout_seconds`, per job) and, if it fails or hangs, is retried with
  exponential backoff + jitter until `max_attempts` is exhausted, at which
  point the job moves to `dead_letter`.

Redis (ephemeral coordination, rate limiting, fast leases) is introduced
starting in Phase 4, once worker heartbeats and crash recovery are
implemented — it isn't needed for the persistence core or for retries.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

This brings up PostgreSQL, the API on `:8080`, and 3 workers (configurable
with `docker compose up --build --scale worker=N`).

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
```

### Running locally without Docker

```bash
# requires a running Postgres instance (see docker-compose.yml for credentials)
cargo run -p api
CONCURRENCY=4 cargo run -p worker
```

Migrations are applied automatically on startup by both the API and the
worker (`sqlx::migrate!`).

### Running the tests

```bash
docker compose up -d postgres
cargo test --workspace
```

The concurrency test (`crates/common/tests/concurrency.rs`, 100 jobs / 10
simulated workers) and the reliability test
(`crates/common/tests/reliability.rs`, retry → retry → dead_letter) both
need a real Postgres instance — mocking them wouldn't make sense, since
what's being tested is how `SKIP LOCKED` and the SQL-computed backoff
behave under real conditions. If no database is available, they skip
themselves with a warning instead of breaking the suite. They always run
in CI (see `.github/workflows/ci.yml`).

## Endpoints (Phase 3)

| Method | Route                | Description                          |
|--------|----------------------|---------------------------------------|
| POST   | `/jobs`              | Create a job                          |
| GET    | `/jobs`              | List jobs (`?status=`, `?limit=`)     |
| GET    | `/jobs/:id`          | Get a job                             |
| GET    | `/jobs/:id/attempts` | Get a job's attempt history           |
| DELETE | `/jobs/:id`          | Cancel a job (only if still pending)  |
| GET    | `/stats`             | Job count by status (`queue_depth`)   |
| GET    | `/health`            | Liveness                              |
| GET    | `/ready`             | Readiness (checks DB connectivity)    |

## Project status

See [`PHASES.md`](./PHASES.md).

## Architecture decisions

See [`docs/adr/`](./docs/adr).

## Guarantees (evolving by phase)

- **At-least-once delivery**: an accepted job is never silently lost, but
  it may run more than once in the face of failures (see ADR-003, added in
  Phase 4). Handlers should be idempotent whenever possible.
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
