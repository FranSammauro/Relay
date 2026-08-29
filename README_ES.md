# Distributed Job Queue

[English version](./README_EN.md)

Sistema distribuido de procesamiento de jobs, persistente y tolerante a
fallos, escrito en Rust. Inspirado conceptualmente en Celery/BullMQ/Sidekiq,
diseñado desde cero para explorar problemas reales de concurrencia y
sistemas distribuidos.

> Este proyecto se desarrolla por fases. Ver [`PHASES.md`](./PHASES.md) para
> el estado actual y el checklist de cada fase.

## Arquitectura (Fase 6)

```
Client --POST /jobs--> API (axum) --> PostgreSQL (fuente de verdad)
          |                                  ^
          |                                  |
     Dashboard (/)                   Worker 1, Worker 2, Worker N
     Métricas (/metrics)             (concurrencia + heartbeat +
     queue-cli (CLI)                  reaper + candidato a líder
                                       del scheduler, cada uno)
                                            |
                                            v
                                    Redis (heartbeats, TTL)
```

- **PostgreSQL** persiste el estado completo de cada job, su historial de
  intentos, sus leases y los cron schedules (ver `migrations/`). Es la
  única fuente de verdad — si un worker, la API o Redis se caen, ningún
  job se pierde. También es la fuente de las métricas: no hay contadores
  acumulados en memoria de ningún proceso (ver más abajo).
- **API (axum)** expone endpoints HTTP para crear/consultar/cancelar jobs,
  ver su historial de ejecución, el estado agregado de la cola, qué
  workers están vivos ahora mismo, gestionar cron schedules, un dashboard
  web (`/`) y métricas en formato Prometheus (`/metrics`).
  **Autenticación**: API key en header `Authorization: Bearer <key>`.
  **Autorización** por rol (ver más abajo). **Rate limiting** por key
  (ventana deslizante 60s, límites por rol, fail-open si Redis caído).
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
- **`queue-cli`**: binario de línea de comandos que habla directo con
  Postgres (no con la API) para operar el sistema — útil incluso si la API
  está caída.

Redis se usa exclusivamente para coordinación efímera (heartbeats). Si se
cae, se pierde visibilidad de quién está vivo, pero la recuperación de
jobs abandonados y el scheduler de cron siguen funcionando igual, porque
corren enteramente sobre PostgreSQL y nunca dependen de Redis — está
probado explícitamente en `crates/common/tests/recovery.rs` y
`crates/common/tests/scheduling.rs`, que no levantan Redis en ningún
momento.

Tanto la API como el worker manejan SIGTERM/Ctrl+C de forma prolija: la
API termina las requests en vuelo antes de cerrar, y el worker deja de
reclamar jobs nuevos y espera a que los que ya están corriendo terminen
(con un plazo máximo, `SHUTDOWN_GRACE_SECONDS`) antes de salir.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

Esto levanta PostgreSQL, Redis, la API en `:8080` y 3 workers (configurable
con `docker compose up --build --scale worker=N`).

## Autenticación y autorización (Fase 8)

La API protege los endpoints operativos con **API keys**:

- Formato: `dq_<prefijo_8_chars>_<secreto_32_bytes_base64url>` (ej.
  `dq_a1b2c3d4_xYz123...`).
- La key completa se muestra **una sola vez** al crearla; la base guarda
  únicamente el prefijo (en claro) y el hash SHA-256 de la key completa.
- Verificación en tiempo constante; revocación inmediata (`revoked_at`).
- Roles y permisos:

| Rol       | Endpoints permitidos                                                               |
|-----------|------------------------------------------------------------------------------------|
| producer  | `POST /jobs`, `DELETE /jobs/:id`, GET lectura (`/jobs`, `/jobs/:id`, `/stats`, `/metrics`, `/workers`) |
| worker    | Solo lectura: `GET /jobs`, `/jobs/:id`, `/jobs/:id/attempts`, `/stats`, `/metrics`, `/workers`         |
| admin     | Todo lo anterior + gestión de cron (`GET/POST /cron`, `GET/DELETE /cron/:id`)   |
| público   | `/`, `/health`, `/ready` (sin key)                                                |

- El dashboard web (`/`) es HTML público, pero su JavaScript pide la key y
  la guarda en `localStorage`; los fetches internos incluyen el header
  `Authorization: Bearer <key>`.

### Crear una API key

```bash
# La CLI habla directo con Postgres (no con la API)
cargo run -p queue-cli -- api-key create --name "mi-servicio" --role producer
# => creada: mi-servicio (id ...)
# => clave (se muestra una sola vez, no se puede recuperar):
#    dq_xyz12345_abcdefghijklmnopqrstuvwxyz1234567890ABCD
```

La key generada se guarda en `.env` del cliente o en su gestor de secretos.

### Variables de entorno (rate limiting)

| Variable                          | Default | Descripción                    |
|-----------------------------------|---------|--------------------------------|
| `RATE_LIMIT_PRODUCER_PER_MINUTE`  | 300     | Límite para rol producer (0 = sin límite) |
| `RATE_LIMIT_WORKER_PER_MINUTE`    | 300     | Límite para rol worker |
| `RATE_LIMIT_ADMIN_PER_MINUTE`     | 0       | Sin límite para admin |

Si se supera el límite, la API devuelve `429 Too Many Requests` con header
`Retry-After` (segundos al próximo window). Si Redis no está disponible,
el rate limiting se desactiva temporalmente (fail-open) — nunca 500.

### Probar la recuperación de un worker caído

```bash
# job artificialmente lento, para tener tiempo de matar el worker que lo agarra
KEY=$(cargo run -p queue-cli -- api-key create --name "demo" --role producer 2>&1 | grep -A1 "clave (se muestra" | tail -1 | xargs)
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $KEY" \
  -d '{"type": "sleep", "payload": {"seconds": 25}, "timeout_seconds": 30}'

# mirá qué worker lo reclamó
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id> | grep worker_id

# matalo de mala manera
docker kill <container_del_worker_que_lo_tenia>

# esperá lease_until + un ciclo de reaper (~15s por default) y mirá cómo
# otro worker lo recupera y lo vuelve a correr
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id>/attempts
```

### Probar un cron schedule

```bash
ADMIN_KEY=$(cargo run -p queue-cli -- api-key create --name "admin" --role admin 2>&1 | grep -A1 "clave (se muestra" | tail -1 | xargs)
curl -X POST localhost:8080/cron \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -d '{"name": "cada-minuto", "cron_expr": "* * * * *", "type": "noop"}'

# esperá un minuto y un ciclo de scheduler (~10s por default) y mirá
# como se disparó solo
curl -H "Authorization: Bearer $ADMIN_KEY" localhost:8080/cron/<id>
# => last_run_at ya tiene valor, next_run_at avanzó al próximo minuto
```

### Probar el shutdown gracioso

```bash
# mandá un job lento y fijate qué worker lo agarró
KEY=$(cargo run -p queue-cli -- api-key create --name "demo" --role producer 2>&1 | grep -A1 "clave (se muestra" | tail -1 | xargs)
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $KEY" \
  -d '{"type": "sleep", "payload": {"seconds": 8}, "timeout_seconds": 30}'

# mandale SIGTERM (docker stop lo hace automáticamente)
docker kill --signal=TERM <container_del_worker>
# o en local: kill -TERM <pid>

# el worker deja de reclamar trabajo nuevo, espera a que el job de 8s
# termine completo, y recién ahí cierra -- mirá sus logs para ver la
# secuencia: worker_draining -> worker_drained -> worker_stopped
```

### Usar la CLI

`queue-cli` habla directo con Postgres (no con la API) — funciona aunque
la API esté caída:

```bash
# Jobs
cargo run -p queue-cli -- jobs list --status dead_letter
cargo run -p queue-cli -- jobs attempts <id>
cargo run -p queue-cli -- stats

# Cron
cargo run -p queue-cli -- cron create --name reporte-diario --expr "0 6 * * *" --type generate_report

# API Keys (Fase 8)
cargo run -p queue-cli -- api-key create --name "mi-servicio" --role producer
cargo run -p queue-cli -- api-key list
cargo run -p queue-cli -- api-key revoke <prefijo>

# Benchmarks
cargo run -p queue-cli -- bench --jobs 1000 --type noop
cargo run -p queue-cli -- --help
```

### Benchmarks y rendimiento

`queue-cli bench` mide latencia de envío, cola y ejecución con datos
reales (no estimados), y requiere al menos un worker corriendo contra la
misma base. Ver [`docs/performance.md`](./docs/performance.md) para el
informe completo, con metodología reproducible y un hallazgo real sobre
un índice bajo backlog sostenido.

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

## Endpoints (Fase 8)

| Método | Ruta                | Descripción                          | Auth / Rol                   |
|--------|---------------------|---------------------------------------|------------------------------|
| GET    | `/`                 | Dashboard web (HTML, live)            | Público (JS pide key)        |
| POST   | `/jobs`             | Crea un job                           | producer, admin              |
| GET    | `/jobs`             | Lista jobs (`?status=`, `?limit=`)    | producer, worker, admin      |
| GET    | `/jobs/:id`         | Consulta un job                       | producer, worker, admin      |
| GET    | `/jobs/:id/attempts`| Historial de intentos de un job       | producer, worker, admin      |
| DELETE | `/jobs/:id`         | Cancela un job (solo si está pending) | producer, admin              |
| GET    | `/stats`            | Conteo de jobs por estado             | producer, worker, admin      |
| GET    | `/metrics`          | Métricas en formato Prometheus        | producer, worker, admin      |
| GET    | `/workers`          | Workers registrados y vivos           | producer, worker, admin      |
| POST   | `/cron`             | Crea un cron schedule                 | admin                        |
| GET    | `/cron`             | Lista cron schedules                  | admin                        |
| GET    | `/cron/:id`         | Consulta un cron schedule             | admin                        |
| DELETE | `/cron/:id`         | Elimina un cron schedule              | admin                        |
| GET    | `/health`           | Liveness                              | Público                      |
| GET    | `/ready`            | Readiness (chequea conexión a DB)     | Público                      |

Para operar sin pasar por HTTP, ver `queue-cli` más arriba.

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
  correctitud del sistema vive en PostgreSQL, nunca solo en Redis. Las
  métricas de `/metrics` también: se calculan al vuelo, no se acumulan en
  memoria de proceso, así que un reinicio no pierde ningún dato.
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
- **Ningún job en vuelo se corta a la mitad por un deploy**: SIGTERM hace
  que el worker deje de reclamar trabajo nuevo y espere (con un plazo
  máximo, `SHUTDOWN_GRACE_SECONDS`) a que los jobs en curso terminen antes
  de salir. Verificado con una prueba manual real: SIGTERM a mitad de un
  job de 8 segundos, el worker esperó los 8 segundos completos antes de
  cerrar.
