# Distributed Job Queue

[Versión en español](./README_ES.md)

Distributed, persistent, fault-tolerant job processing system written in
Rust. Conceptually inspired by Celery/BullMQ/Sidekiq, built from scratch to
explore real concurrency and distributed systems problems.

> This project is developed in phases. See [`PHASES.md`](./PHASES.md) for
> current status and the checklist for each phase.

## Architecture (Phase 6)

```
Client --POST /jobs--> API (axum) --> PostgreSQL (source of truth)
          |                                  ^
          |                                  |
     Dashboard (/)                   Worker 1, Worker 2, Worker N
     Metrics (/metrics)              (concurrency + heartbeat +
     queue-cli (CLI)                  reaper + scheduler leader
                                       candidate, each one)
                                            |
                                            v
                                    Redis (heartbeats, TTL)
```

- **PostgreSQL** persists the full state of every job, its attempt
  history, its leases, and the cron schedules (see `migrations/`). It's
  the single source of truth — if a worker, the API, or Redis go down, no
  job is lost. It's also the source of the metrics: there are no counters
  accumulated in any process's memory (see below).
- **API (axum)** exposes HTTP endpoints to create/query/cancel jobs, check
  their execution history, the queue's aggregate state, which workers are
  currently alive, manage cron schedules, a web dashboard (`/`), and
  Prometheus-format metrics (`/metrics`).
- **Worker** polls the `jobs` table using
  `SELECT ... FOR UPDATE SKIP LOCKED`, which allows multiple workers to
  compete for work without blocking each other or claiming the same job
  twice. Each worker runs several jobs at once (semaphore, `CONCURRENCY`),
  and also runs three background tasks of its own:
  - **Heartbeat**: beats every `HEARTBEAT_INTERVAL_MS` into Redis with a
    TTL, so `GET /workers` knows who's still alive (see ADR-002).
  - **Reaper**: periodically checks for `running` jobs whose lease expired
    (the worker holding them died without saying so) and recovers them
    using the same retry/backoff/DLQ policy as a normal failure (see
    ADR-003 and ADR-004). Runs on **every** worker at once, with no single
    coordinator — kill any of them with `kill -9` mid-job and another one
    recovers it on its own.
  - **Cron scheduler**: competes for a Postgres advisory lock (see
    ADR-006); whoever gets it is the only one scanning and firing due
    `cron_schedules`, creating the corresponding real job.
- **`queue-cli`**: command-line binary that talks directly to Postgres
  (not the API) to operate the system — useful even if the API is down.

Redis is used exclusively for ephemeral coordination (heartbeats). If it
goes down, visibility into who's alive is lost, but recovering abandoned
jobs and the cron scheduler keep working exactly the same, since both run
entirely on PostgreSQL and never depend on Redis — this is explicitly
tested in `crates/common/tests/recovery.rs` and
`crates/common/tests/scheduling.rs`, neither of which spins up Redis.

Both the API and the worker handle SIGTERM/Ctrl+C gracefully: the API
finishes in-flight requests before shutting down, and the worker stops
claiming new jobs and waits for the ones already running to finish (up to
a maximum, `SHUTDOWN_GRACE_SECONDS`) before exiting.

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

curl localhost:8080/metrics
# => Prometheus-format metrics

open http://localhost:8080/ in a browser
# => live dashboard with queue_depth, workers, and recent jobs

curl -X POST localhost:8080/cron \
  -H "Content-Type: application/json" \
  -d '{"name": "daily-cleanup", "cron_expr": "0 4 * * *", "type": "cleanup"}'
# => creates the schedule, computes next_run_at automatically

curl localhost:8080/cron
# => list of cron schedules with their next run
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

### Try a cron schedule

```bash
curl -X POST localhost:8080/cron \
  -H "Content-Type: application/json" \
  -d '{"name": "every-minute", "cron_expr": "* * * * *", "type": "noop"}'

# wait a minute plus one scheduler cycle (~10s by default) and watch it
# fire on its own
curl localhost:8080/cron/<id>
# => last_run_at is now set, next_run_at advanced to the next minute
```

### Try a graceful shutdown

```bash
# send a slow job and note which worker claims it
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "sleep", "payload": {"seconds": 8}, "timeout_seconds": 30}'

# send it SIGTERM (docker stop does this automatically)
docker kill --signal=TERM <worker_container>
# or locally: kill -TERM <pid>

# the worker stops claiming new work, waits for the 8-second job to
# finish completely, and only then shuts down -- watch its logs for the
# sequence: worker_draining -> worker_drained -> worker_stopped
```

### Using the CLI

`queue-cli` talks directly to Postgres (not the API) — it works even if
the API is down:

```bash
cargo run -p queue-cli -- jobs list --status dead_letter
cargo run -p queue-cli -- jobs attempts <id>
cargo run -p queue-cli -- stats
cargo run -p queue-cli -- cron create --name daily-report --expr "0 6 * * *" --type generate_report
cargo run -p queue-cli -- bench --jobs 1000 --type noop
cargo run -p queue-cli -- --help
```

### Benchmarks and performance

`queue-cli bench` measures submission, queue, and execution latency with
real data (not estimated), and requires at least one worker running
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

Migrations are applied automatically on startup by both the API and the
worker (`sqlx::migrate!`).

### Running the tests

```bash
docker compose up -d postgres redis
cargo test --workspace
```

The concurrency test (`crates/common/tests/concurrency.rs`, 100 jobs / 10
simulated workers), the reliability test
(`crates/common/tests/reliability.rs`, retry → retry → dead_letter), the
recovery test (`crates/common/tests/recovery.rs`, expired lease →
retry/dead_letter), and the scheduling test
(`crates/common/tests/scheduling.rs`, cron firing + leadership) all need a
real Postgres instance — mocking them wouldn't make sense, since what's
being tested is how `SKIP LOCKED`, the SQL-computed backoff/recovery
logic, and the Postgres advisory lock behave under real conditions.
Notably, neither the recovery nor the scheduling test needs Redis (see
ADR-002 and ADR-006): both recovering abandoned jobs and the cron
scheduler run entirely on Postgres. If no database is available, they
skip themselves with a warning instead of breaking the suite. They always
run in CI (see `.github/workflows/ci.yml`).

A note on test isolation: these tests share one Postgres instance and are
written to tolerate that (unique job-type/name markers per run, tolerance
for jobs claimed by a concurrently-running test). If you run the suite
right after manually poking at the system by hand (e.g. the demos above)
and hit a surprising failure, truncate the tables and try again:

```bash
psql "$DATABASE_URL" -c "TRUNCATE jobs, job_attempts, workers, cron_schedules RESTART IDENTITY CASCADE;"
```

## Endpoints (Phase 6)

| Method | Route                | Description                          |
|--------|----------------------|---------------------------------------|
| GET    | `/`                  | Web dashboard (live HTML)             |
| POST   | `/jobs`              | Create a job                          |
| GET    | `/jobs`              | List jobs (`?status=`, `?limit=`)     |
| GET    | `/jobs/:id`          | Get a job                             |
| GET    | `/jobs/:id/attempts` | Get a job's attempt history           |
| DELETE | `/jobs/:id`          | Cancel a job (only if still pending)  |
| GET    | `/stats`             | Job count by status (`queue_depth`)   |
| GET    | `/metrics`           | Prometheus-format metrics             |
| GET    | `/workers`           | Registered workers and who's alive now |
| POST   | `/cron`              | Create a cron schedule                |
| GET    | `/cron`              | List cron schedules                   |
| GET    | `/cron/:id`          | Get a cron schedule                   |
| DELETE | `/cron/:id`          | Delete a cron schedule                |
| GET    | `/health`            | Liveness                              |
| GET    | `/ready`             | Readiness (checks DB connectivity)    |

To operate without going through HTTP, see `queue-cli` above.

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
  correctness lives in PostgreSQL, never only in Redis. The `/metrics`
  data too: it's computed on the fly, never accumulated in process
  memory, so a restart never loses any of it.
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
- **No duplicate cron firings**: a cron schedule only gets fired by the
  current leader (Postgres advisory lock, see ADR-006), and the job it
  creates carries a derived `idempotency_key` as an extra safety net — a
  double firing of the same time slot doesn't create two jobs. Verified
  with an integration test and with a real manual test: a schedule created
  via the API fired on its own, unattended, with correct timing.
- **No in-flight job gets cut off by a deploy**: SIGTERM makes the worker
  stop claiming new work and wait (up to a maximum,
  `SHUTDOWN_GRACE_SECONDS`) for jobs already in progress to finish before
  exiting. Verified with a real manual test: SIGTERM sent mid-way through
  an 8-second job, the worker waited the full 8 seconds before shutting
  down.
