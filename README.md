# Distributed Job Queue

Sistema distribuido de procesamiento de jobs, persistente y tolerante a
fallos, escrito en Rust. Inspirado conceptualmente en Celery/BullMQ/Sidekiq,
diseñado desde cero para explorar problemas reales de concurrencia y
sistemas distribuidos.

> Este proyecto se desarrolla por fases. Ver [`PHASES.md`](./PHASES.md) para
> el estado actual y el checklist de cada fase.

## Arquitectura (Fase 1)

```
Client --POST /jobs--> API (axum) --> PostgreSQL (fuente de verdad)
                                            ^
                                            |
                                          Worker  (claim -> execute -> ack)
```

- **PostgreSQL** persiste el estado completo de cada job (ver
  `migrations/0001_init.sql`). Es la única fuente de verdad — si el worker o
  la API se caen, ningún job se pierde.
- **API (axum)** expone endpoints HTTP para crear/consultar/cancelar jobs.
- **Worker** hace polling de la tabla `jobs` usando
  `SELECT ... FOR UPDATE SKIP LOCKED`, lo que permite (desde ya) que
  múltiples workers compitan por trabajo sin bloquearse ni duplicar el
  claim de un mismo job.

Redis (coordinación efímera, rate limiting, leases rápidos) se incorpora a
partir de Fase 4, cuando se implementen heartbeats y recuperación de workers
caídos — no es necesario para el core de persistencia.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

Esto levanta PostgreSQL, la API en `:8080` y un worker.

### Probar el flujo

```bash
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "resize_image", "payload": {"width": 1920, "height": 1080}}'

# => {"id": "...", "status": "pending"}

curl localhost:8080/jobs/<id>
# => status pasa de pending -> running -> completed
```

### Correr localmente sin Docker

```bash
# requiere un Postgres corriendo (ver docker-compose.yml para credenciales)
cargo run -p api
cargo run -p worker
```

Las migraciones se aplican automáticamente al iniciar tanto la API como el
worker (`sqlx::migrate!`).

## Endpoints (Fase 1)

| Método | Ruta          | Descripción                          |
|--------|---------------|---------------------------------------|
| POST   | `/jobs`       | Crea un job                           |
| GET    | `/jobs`       | Lista jobs (`?status=`, `?limit=`)    |
| GET    | `/jobs/:id`   | Consulta un job                       |
| DELETE | `/jobs/:id`   | Cancela un job (solo si está pending) |
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
