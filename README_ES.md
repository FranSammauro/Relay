# Distributed Job Queue

[English version](./README_EN.md)

Sistema distribuido de procesamiento de jobs, persistente y tolerante a
fallos, escrito en Rust. Inspirado conceptualmente en Celery/BullMQ/Sidekiq,
diseñado desde cero para explorar problemas reales de concurrencia y
sistemas distribuidos.

> Este proyecto se desarrolla por fases. Ver [`PHASES.md`](./PHASES.md) para
> el estado actual y el checklist de cada fase.

## Arquitectura (Fase 5)

```
Client --POST /jobs--> API (axum) --> PostgreSQL (fuente de verdad)
                                            ^
                                            |
                                    Worker 1, Worker 2, Worker N
                                    (concurrencia + heartbeat +
                                     reaper + candidato a líder
                                     del scheduler, cada uno)
                                            |
                                            v
                                    Redis (heartbeats, TTL)
```

- **PostgreSQL** persiste el estado completo de cada job, su historial de
  intentos, sus leases y los cron schedules (ver `migrations/`). Es la
  única fuente de verdad — si un worker, la API o Redis se caen, ningún
  job se pierde.
- **API (axum)** expone endpoints HTTP para crear/consultar/cancelar jobs,
  ver su historial de ejecución, el estado agregado de la cola, qué
  workers están vivos ahora mismo, y gestionar cron schedules.
- **Worker** hace polling de la tabla `jobs` usando
  `SELECT ... FOR UPDATE SKIP LOCKED`, lo que permite que múltiples
  workers compitan por trabajo sin bloquearse ni duplicar el claim de un
  mismo job. Cada worker corre varios jobs en simultáneo (semáforo,
  `CONCURRENCY`), y además corre en background tres tareas propias:
  - **Heartbeat**: late cada `HEARTBEAT_INTERVAL_MS` en Redis con TTL, para
    que `GET /workers` sepa quién sigue vivo (ver ADR-002).
  - **Reaper**: revisa periódicamente si hay jobs `running` cuyo lease
    venció (el worker que los tenía murió sin avisar) y los recupera
    aplicando la misma política de retry/backoff/DLQ que un fallo
    normal (ver ADR-003 y ADR-004). Corre en **todos** los workers a la
    vez, sin coordinador único — matá cualquiera con `kill -9` a mitad de
    un job y otro lo recupera solo.
  - **Scheduler de cron**: compite por un advisory lock de Postgres (ver
    ADR-006); el que lo consigue es el único que escanea y dispara los
    `cron_schedules` vencidos, creando el job real correspondiente.

Redis se usa exclusivamente para coordinación efímera (heartbeats). Si se
cae, se pierde visibilidad de quién está vivo, pero la recuperación de
jobs abandonados y el scheduler de cron siguen funcionando igual, porque
corren enteramente sobre PostgreSQL y nunca dependen de Redis — está
probado explícitamente en `crates/common/tests/recovery.rs` y
`crates/common/tests/scheduling.rs`, que no levantan Redis en ningún
momento.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

Esto levanta PostgreSQL, Redis, la API en `:8080` y 3 workers (configurable
con `docker compose up --build --scale worker=N`).

### Probar el flujo

```bash
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "resize_image", "payload": {"width": 1920, "height": 1080}}'

# => {"id": "...", "status": "pending"}

curl localhost:8080/jobs/<id>
# => status pasa de pending -> running -> completed

curl localhost:8080/stats
# => {"by_status": {"pending": 0, "completed": 1, ...}}

curl localhost:8080/jobs/<id>/attempts
# => historial completo de cada intento, con worker, error y resultado

curl localhost:8080/workers
# => quién está registrado y quién está vivo ahora mismo

curl -X POST localhost:8080/cron \
  -H "Content-Type: application/json" \
  -d '{"name": "cleanup-diario", "cron_expr": "0 4 * * *", "type": "cleanup"}'
# => crea el schedule, calcula next_run_at automáticamente

curl localhost:8080/cron
# => lista de cron schedules con su próxima corrida
```

### Probar la recuperación de un worker caído

```bash
# job artificialmente lento, para tener tiempo de matar el worker que lo agarra
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "sleep", "payload": {"seconds": 25}, "timeout_seconds": 30}'

# mirá qué worker lo reclamó
curl localhost:8080/jobs/<id> | grep worker_id

# matalo de mala manera
docker kill <container_del_worker_que_lo_tenia>

# esperá lease_until + un ciclo de reaper (~15s por default) y mirá cómo
# otro worker lo recupera y lo vuelve a correr
curl localhost:8080/jobs/<id>/attempts
```

### Probar un cron schedule

```bash
curl -X POST localhost:8080/cron \
  -H "Content-Type: application/json" \
  -d '{"name": "cada-minuto", "cron_expr": "* * * * *", "type": "noop"}'

# esperá un minuto y un ciclo de scheduler (~10s por default) y mirá
# como se disparó solo
curl localhost:8080/cron/<id>
# => last_run_at ya tiene valor, next_run_at avanzó al próximo minuto
```

### Correr localmente sin Docker

```bash
# requiere un Postgres y un Redis corriendo (ver docker-compose.yml para credenciales)
cargo run -p api
CONCURRENCY=4 cargo run -p worker
```

Las migraciones se aplican automáticamente al iniciar tanto la API como el
worker (`sqlx::migrate!`).

### Correr los tests

```bash
docker compose up -d postgres
cargo test --workspace
```

El test de concurrencia (`crates/common/tests/concurrency.rs`, 100 jobs /
10 workers simulados), el de reliability (`crates/common/tests/reliability.rs`,
retry → retry → dead_letter), el de recovery
(`crates/common/tests/recovery.rs`, lease vencido → retry/dead_letter) y el
de scheduling (`crates/common/tests/scheduling.rs`, disparo de cron +
liderazgo) necesitan un Postgres real — no tiene sentido mockearlos, porque
lo que se está probando es el comportamiento de `SKIP LOCKED`, el
backoff/recovery calculados en SQL, y el advisory lock de Postgres, todo
bajo condiciones reales. Notablemente, ni el test de recovery ni el de
scheduling necesitan Redis (ver ADR-002 y ADR-006): tanto la recuperación
de jobs abandonados como el scheduler de cron corren enteramente sobre
Postgres. Si no hay base disponible, los tests se saltan solos con un
aviso en vez de romper la suite. En CI corren siempre (ver
`.github/workflows/ci.yml`).

Nota sobre aislamiento: estos tests comparten una sola instancia de
Postgres y están escritos para tolerarlo (marca de `job_type`/`name` única
por corrida, tolerancia a jobs reclamados por otro test corriendo en
simultáneo). Si corrés la suite justo después de probar el sistema a mano
(por ejemplo las demos de más arriba) y te encontrás con un fallo raro,
truncá las tablas y probá de nuevo:

```bash
psql "$DATABASE_URL" -c "TRUNCATE jobs, job_attempts, workers, cron_schedules RESTART IDENTITY CASCADE;"
```

## Endpoints (Fase 5)

| Método | Ruta                | Descripción                          |
|--------|---------------------|---------------------------------------|
| POST   | `/jobs`             | Crea un job                           |
| GET    | `/jobs`             | Lista jobs (`?status=`, `?limit=`)    |
| GET    | `/jobs/:id`         | Consulta un job                       |
| GET    | `/jobs/:id/attempts`| Historial de intentos de un job       |
| DELETE | `/jobs/:id`         | Cancela un job (solo si está pending) |
| GET    | `/stats`            | Conteo de jobs por estado (`queue_depth`) |
| GET    | `/workers`          | Workers registrados y quién está vivo ahora |
| POST   | `/cron`             | Crea un cron schedule                 |
| GET    | `/cron`             | Lista cron schedules                  |
| GET    | `/cron/:id`         | Consulta un cron schedule             |
| DELETE | `/cron/:id`         | Elimina un cron schedule              |
| GET    | `/health`           | Liveness                              |
| GET    | `/ready`            | Readiness (chequea conexión a DB)     |

## Estado del proyecto

Ver [`PHASES.md`](./PHASES.md).

## Decisiones de arquitectura

Ver [`docs/adr/`](./docs/adr).

## Garantías (evolucionando por fase)

- **At-least-once delivery**: un job aceptado no se pierde silenciosamente,
  pero puede llegar a ejecutarse más de una vez ante fallos (ver ADR-003,
  que se agrega en Fase 4). Los handlers deberían ser idempotentes cuando
  sea posible.
- **PostgreSQL como fuente de verdad**: cualquier dato crítico para la
  correctitud del sistema vive en PostgreSQL, nunca solo en Redis.
- **Concurrencia sin pérdida ni duplicación**: verificado con un test de
  integración contra Postgres real (100 jobs / 10 workers concurrentes),
  no solo "debería andar" en el papel.
- **Reintentos con backoff, no reintentos infinitos**: un job fallido
  reintenta con backoff exponencial + jitter hasta `max_attempts`; después
  pasa a `dead_letter` y queda ahí — no hay loops de retry silenciosos que
  puedan tapar un bug. Verificado con test de integración (retry → retry
  → dead_letter) contra Postgres real.
- **Recuperación ante caída de worker**: si un worker muere a mitad de un
  job (crash, OOM, kill -9), otro worker lo detecta por lease vencido y lo
  recupera aplicando la misma política de retry/DLQ. Verificado tanto con
  test de integración (`crates/common/tests/recovery.rs`) como con una
  prueba manual real: matar un worker con `kill -9` a mitad de un job de
  25 segundos y confirmar que otro lo retoma y lo completa.
- **Cron sin duplicados**: un cron schedule solo lo dispara el líder
  actual (advisory lock de Postgres, ver ADR-006), y el job que crea lleva
  un `idempotency_key` derivado como red de seguridad extra — un doble
  disparo del mismo horario no crea dos jobs. Verificado con test de
  integración y con una prueba manual real: un schedule creado vía API se
  disparó solo, sin intervención, con el timing correcto.
