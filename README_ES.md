# Distributed Job Queue

[English version](./README_EN.md)

Sistema distribuido de procesamiento de jobs, persistente y tolerante a
fallos, escrito en Rust. Inspirado conceptualmente en Celery/BullMQ/Sidekiq,
diseñado desde cero para explorar problemas reales de concurrencia y
sistemas distribuidos.

> Este proyecto se desarrolla por fases. Ver [`PHASES.md`](./PHASES.md) para
> el estado actual y el checklist de cada fase.

## Arquitectura (Fase 2)

```
Client --POST /jobs--> API (axum) --> PostgreSQL (fuente de verdad)
                                            ^
                                            |
                                    Worker 1, Worker 2, Worker N
                                    (cada uno con su propio pool
                                     de tareas concurrentes)
```

- **PostgreSQL** persiste el estado completo de cada job (ver
  `migrations/`). Es la única fuente de verdad — si un worker o la API se
  caen, ningún job se pierde.
- **API (axum)** expone endpoints HTTP para crear/consultar/cancelar jobs
  y ver el estado agregado de la cola.
- **Worker** hace polling de la tabla `jobs` usando
  `SELECT ... FOR UPDATE SKIP LOCKED`, lo que permite que múltiples
  workers compitan por trabajo sin bloquearse ni duplicar el claim de un
  mismo job. Cada worker corre varios jobs en simultáneo, acotado por un
  semáforo (`CONCURRENCY`, fijo por config desde Fase 2 — nada de
  autoscaling todavía).

Redis (coordinación efímera, rate limiting, leases rápidos) se incorpora a
partir de Fase 4, cuando se implementen heartbeats y recuperación de workers
caídos — no es necesario para el core de persistencia ni para la
concurrencia de Fase 2.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

Esto levanta PostgreSQL, la API en `:8080` y 3 workers (configurable con
`docker compose up --build --scale worker=N`).

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
```

### Correr localmente sin Docker

```bash
# requiere un Postgres corriendo (ver docker-compose.yml para credenciales)
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
10 workers simulados) necesita un Postgres real — no tiene sentido
mockearlo, porque lo que se está probando es el comportamiento de
`SKIP LOCKED` bajo contención real. Si no hay base disponible, el test se
salta solo con un aviso en vez de romper la suite. En CI corre siempre
(ver `.github/workflows/ci.yml`).

## Endpoints (Fase 2)

| Método | Ruta          | Descripción                          |
|--------|---------------|---------------------------------------|
| POST   | `/jobs`       | Crea un job                           |
| GET    | `/jobs`       | Lista jobs (`?status=`, `?limit=`)    |
| GET    | `/jobs/:id`   | Consulta un job                       |
| DELETE | `/jobs/:id`   | Cancela un job (solo si está pending) |
| GET    | `/stats`      | Conteo de jobs por estado (`queue_depth`) |
| GET    | `/health`     | Liveness                              |
| GET    | `/ready`      | Readiness (chequea conexión a DB)     |

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
