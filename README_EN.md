# Distributed Job Queue

[Versión en español](./README_ES.md)

Distributed, persistent, fault-tolerant job processing system written in Rust.
Conceptually inspired by Celery/BullMQ/Sidekiq, built from scratch to explore
real-world concurrency and distributed systems problems.

> This project is developed in phases. See [`PHASES.md`](./PHASES.md) for
> current status and phase checklists.

## Architecture (Phase 6)

```
Client --POST /jobs--> API (axum) --> PostgreSQL (source of truth)
      |                                  ^
      |                                  |
Dashboard (/)                    Worker 1, Worker 2, Worker N
Metrics (/metrics)               (concurrency + heartbeat +
queue-cli (CLI)                  reaper + scheduler leader
                                        candidate, each one)
                                             |
                                             v
                                     Redis (heartbeats, TTL)
```

- **PostgreSQL** persists the full state of every job, its attempt history,
  its leases, and cron schedules (see `migrations/`). It is the single
  source of truth — if a worker, the API, or Redis crashes, no job is lost.
  It is also the source of metrics: no counters accumulated in any process
  memory (see below).
- **API (axum)** exposes HTTP endpoints to create/query/cancel jobs, view
  execution history, aggregated queue state, which workers are alive right
  now, manage cron schedules, a web dashboard (`/`), and Prometheus metrics
  (`/metrics`). **Authentication**: API key in `Authorization: Bearer <key>`
  header. **Authorization** by role (see below). **Rate limiting** per key
  (sliding window 60s, limits by role, fail-open if Redis down).
- **Worker** polls the `jobs` table using
  `SELECT ... FOR UPDATE SKIP LOCKED`, allowing multiple workers to compete
  for work without blocking or duplicating the claim of the same job. Each
  worker runs several jobs concurrently (semaphore, `CONCURRENCY`), and also
  runs three background tasks:
  - **Heartbeat**: beats every `HEARTBEAT_INTERVAL_MS` in Redis with TTL,
    so `GET /workers` knows who is still alive (see ADR-002).
  - **Reaper**: periodically checks for `running` jobs whose lease expired
    (the worker that had them died without notice) and recovers them applying
    the same retry/backoff/DLQ policy as a normal failure (see ADR-003 and
    ADR-004). Runs in **all** workers simultaneously, no single coordinator
    — kill any with `kill -9` mid-job and another recovers it automatically.
  - **Cron scheduler**: competes for a Postgres advisory lock (see
    ADR-006); the one that gets it is the only one that scans and fires
    due `cron_schedules`, creating the corresponding real job.
- **`queue-cli`**: command-line binary that talks directly to Postgres (not
  the API) to operate the system — works even if the API is down.

Redis is used exclusively for ephemeral coordination (heartbeats). If it
goes down, visibility of who is alive is lost, but abandoned-job recovery
and the cron scheduler keep working the same, because they run entirely on
PostgreSQL and never depend on Redis — explicitly tested in
`crates/common/tests/recovery.rs` and `crates/common/tests/scheduling.rs`,
which never bring up Redis.

Both the API and the worker handle SIGTERM/Ctrl+C gracefully: the API
drains in-flight requests before closing, and the worker stops claiming new
jobs and waits for those already running to finish (with a max deadline,
`SHUTDOWN_GRACE_SECONDS`) before exiting.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

This brings up PostgreSQL, Redis, the API on `:8080`, and 3 workers
(configurable with `docker compose up --build --scale worker=N`).

### Try the flow (with API key)

```bash
# 1. Create a producer key (CLI uses Postgres directly)
KEY=$(cargo run -p queue-cli -- api-key create --name "demo" --role producer 2>&1 | grep -A1 "key (shown" | tail -1 | xargs)
echo "API Key: $KEY"

# 2. Submit a job
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $KEY" \
  -d '{"type": "resize_image", "payload": {"width": 1920, "height": 1080}}'

# => {"id": "...", "status": "pending"}

# 3. Query (same header on all requests)
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id>
# => status goes pending -> running -> completed

curl -H "Authorization: Bearer $KEY" localhost:8080/stats
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id>/attempts
curl -H "Authorization: Bearer $KEY" localhost:8080/workers
curl -H "Authorization: Bearer $KEY" localhost:8080/metrics

# 4. Dashboard web: open http://localhost:8080/ in browser,
#    paste API key when prompted (saved in localStorage)

# 5. Cron schedules (requires admin role)
ADMIN_KEY=$(cargo run -p queue-cli -- api-key create --name "admin" --role admin 2>&1 | grep -A1 "key (shown" | tail -1 | xargs)
curl -X POST localhost:8080/cron \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -d '{"name": "daily-cleanup", "cron_expr": "0 4 * * *", "type": "cleanup"}'

curl -H "Authorization: Bearer $ADMIN_KEY" localhost:8080/cron
```

### Try worker crash recovery

```bash
# artificially slow job, to have time to kill the worker that picks it up
KEY=$(cargo run -p queue-cli -- api-key create --name "demo" --role producer 2>&1 | grep -A1 "key (shown" | tail -1 | xargs)
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $KEY" \
  -d '{"type": "sleep", "payload": {"seconds": 25}, "timeout_seconds": 30}'

# see which worker claimed it
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id> | grep worker_id

# kill it brutally
docker kill <container_of_worker_that_had_it>

# wait lease_until + a reaper cycle (~15s default) and watch another
# worker pick it up and re-run it
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id>/attempts
```

### Try a cron schedule

```bash
ADMIN_KEY=$(cargo run -p queue-cli -- api-key create --name "admin" --role admin 2>&1 | grep -A1 "key (shown" | tail -1 | xargs)
curl -X POST localhost:8080/cron \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -d '{"name": "every-minute", "cron_expr": "* * * * *", "type": "noop"}'

# wait a minute and a scheduler cycle (~10s default) and watch it fire
curl -H "Authorization: Bearer $ADMIN_KEY" localhost:8080/cron/<id>
# => last_run_at now has a value, next_run_at advanced to next minute
```

### Try graceful shutdown

```bash
# submit a slow job and see which worker picked it up
KEY=$(cargo run -p queue-cli -- api-key create --name "demo" --role producer 2>&1 | grep -A1 "key (shown" | tail -1 | xargs)
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $KEY" \
  -d '{"type": "sleep", "payload": {"seconds": 8}, "timeout_seconds": 30}'

# send SIGTERM (docker stop does this automatically)
docker kill --signal=TERM <worker_container>
# or locally: kill -TERM <pid>

# the worker stops claiming new work, waits for the 8s job to finish
# completely, and only then exits -- check its logs for the sequence:
# worker_draining -> worker_drained -> worker_stopped
```

### Use the CLI

`queue-cli` talks directly to Postgres (not the API) — works even if the
API is down:

```bash
# Jobs
cargo run -p queue-cli -- jobs list --status dead_letter
cargo run -p queue-cli -- jobs attempts <id>
cargo run -p queue-cli -- stats

# Cron
cargo run -p queue-cli -- cron create --name daily-report --expr "0 6 * * *" --type generate_report

# API Keys (Phase 8)
cargo run -p queue-cli -- api-key create --name "my-service" --role producer
cargo run -p queue-cli -- api-key list
cargo run -p queue-cli -- api-key revoke <prefix>

# Benchmarks
cargo run -p queue-cli -- bench --jobs 1000 --type noop
cargo run -p queue-cli -- --help
```

### Benchmarks and performance

`queue-cli bench` measures submission, queue, and execution latency with
real data (not estimates), and requires at least one worker running against
the same database. See [`docs/performance.md`](./docs/performance.md) for
the full report, with reproducible methodology and a real finding about
an index under sustained backlog.

### Run locally without Docker

```bash
# requires a running Postgres and Redis (see docker-compose.yml for credentials)
cargo run -p api
CONCURRENCY=4 cargo run -p worker
```

Migrations apply automatically on both API and worker startup
(`sqlx::migrate!`).

### Running tests

```bash
docker compose up -d postgres
cargo test --workspace
```

The concurrency test (`crates/common/tests/concurrency.rs`, 100 jobs /
10 simulated workers), reliability test (`crates/common/tests/reliability.rs`,
retry → retry → dead_letter), recovery test
(`crates/common/tests/recovery.rs`, lease expired → retry/dead_letter) and
scheduling test (`crates/common/tests/scheduling.rs`, cron firing +
leadership) need a real Postgres — no point mocking them, because what's
being tested is `SKIP LOCKED` behavior, SQL-calculated backoff/recovery,
and Postgres advisory lock, all under real conditions. Notably, neither the
recovery test nor the scheduling test need Redis (see ADR-002 and ADR-006):
both abandoned-job recovery and the cron scheduler run entirely on Postgres.
If no database is available, tests skip themselves with a notice instead of
breaking the suite. In CI they always run (see
`.github/workflows/ci.yml`).

Isolation note: these tests share a single Postgres instance and are written
to tolerate it (unique `job_type`/`name` per run, tolerance to jobs claimed
by another concurrently running test). If you run the suite right after
manual system testing (e.g. the demos above) and hit a flaky failure,
truncate the tables and try again:

```bash
psql "$DATABASE_URL" -c "TRUNCATE jobs, job_attempts, workers, cron_schedules RESTART IDENTITY CASCADE;"
```

## Endpoints (Phase 8)

| Method | Path                | Description                          | Auth / Role                   |
|--------|---------------------|---------------------------------------|------------------------------|
| GET    | `/`                 | Dashboard web (HTML, live)            | Public (JS prompts for key)  |
| POST   | `/jobs`             | Create a job                          | producer, admin              |
| GET    | `/jobs`             | List jobs (`?status=`, `?limit=`)     | producer, worker, admin      |
| GET    | `/jobs/:id`         | Get a job                             | producer, worker, admin      |
| GET    | `/jobs/:id/attempts`| Attempt history for a job             | producer, worker, admin      |
| DELETE | `/jobs/:id`         | Cancel a job (only if pending)        | producer, admin              |
| GET    | `/stats`            | Job count by state (`queue_depth`)    | producer, worker, admin      |
| GET    | `/metrics`          | Metrics in Prometheus format          | producer, worker, admin      |
| GET    | `/workers`          | Registered workers and liveness       | producer, worker, admin      |
| POST   | `/cron`             | Create a cron schedule                | admin                        |
| GET    | `/cron`             | List cron schedules                   | admin                        |
| GET    | `/cron/:id`         | Get a cron schedule                   | admin                        |
| DELETE | `/cron/:id`         | Delete a cron schedule                | admin                        |
| GET    | `/health`           | Liveness                              | Public                       |
| GET    | `/ready`            | Readiness (checks DB connection)      | Public                       |

For CLI-based operation, see `queue-cli` above.

## Project Status

See [`PHASES.md`](./PHASES.md).

## Architecture Decisions

See [`docs/adr/`](./docs/adr).

## Guarantees (evolving by phase)

- **At-least-once delivery**: an accepted job is not silently lost,
  but may execute more than once on failures (see ADR-003, added in
  Phase 4). Handlers should be idempotent when possible.
- **PostgreSQL as source of truth**: any data critical to system
  correctness lives in PostgreSQL, never only in Redis. Metrics from
  `/metrics` too: calculated on the fly, not accumulated in process
  memory, so a restart loses no data.
- **Concurrency without loss or duplication**: verified with an
  integration test against real Postgres (100 jobs / 10 concurrent
  workers), not just "should work" on paper.
- **Retries with backoff, not infinite retries**: a failed job retries
  with exponential backoff + jitter until `max_attempts`; then goes to
  `dead_letter` and stays there — no silent retry loops that can mask
  a bug. Verified with integration test (retry → retry → dead_letter)
  against real Postgres.
- **Recovery on worker crash**: if a worker dies mid-job (crash, OOM,
  kill -9), another worker detects it via lease expiry and recovers it
  applying the same retry/DLQ policy. Verified both with integration test
  (`crates/common/tests/recovery.rs`) and a real manual test: kill a
  worker with `kill -9` mid-way through a 25-second job and confirm
  another picks it up and completes it.
- **Cron without duplicates**: a cron schedule is only fired by the
  current leader (Postgres advisory lock, see ADR-006), and the job it
  creates carries an `idempotency_key` derived as extra safety — a double
  firing of the same schedule does not create two jobs. Verified with
  integration test and a real manual test: a schedule created via API
  fired autonomously with correct timing.
- **No in-flight job cut in half by a deploy**: SIGTERM makes the worker
  stop claiming new work and wait (with a max deadline,
  `SHUTDOWN_GRACE_SECONDS`) for in-flight jobs to finish before exiting.
  Verified with a real manual test: SIGTERM mid-way through an 8-second
  job, the worker waited the full 8 seconds before closing.