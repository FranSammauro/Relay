# Relay

[English version](./README_EN.md)

Relay es un sistema distribuido de procesamiento de jobs, persistente y
tolerante a fallos, escrito en Rust. Inspirado conceptualmente en Celery/BullMQ/Sidekiq,
diseñado desde cero para explorar problemas reales de concurrencia y
sistemas distribuidos.

> Este proyecto se desarrolla por fases. Ver [`PHASES.md`](./PHASES.md) para
> el estado actual y el checklist de cada fase.

## Arquitectura

```
Client --POST /jobs--> API (axum) --> PostgreSQL (fuente de verdad)
          |                                  ^
          |                                  |
     Dashboard (/)                   Worker 1, Worker 2, Worker N
     Métricas (/metrics)             (concurrencia + heartbeat +
     relay-cli (CLI)                  reaper + candidato a líder
                                       del scheduler, cada uno)
                                            |
                                            v
                                    Redis (heartbeats, TTL,
                                     contadores de rate limit)
```

- **PostgreSQL** persiste el estado completo de cada job, su historial de
  intentos, sus leases, los cron schedules y las API keys emitidas (ver
  `migrations/`). Es la única fuente de verdad: si un worker, la API o
  Redis se caen, ningún job se pierde. También es la fuente de las
  métricas expuestas en `/metrics`, ya que no hay contadores acumulados
  en memoria de ningún proceso (ver más abajo).
- **API (axum)** expone endpoints HTTP para crear, consultar y cancelar
  jobs, ver su historial de ejecución, el estado agregado de la cola, qué
  workers están vivos en un momento dado, gestionar cron schedules, un
  dashboard web y métricas en formato Prometheus. Todos los endpoints
  operativos requieren autenticación mediante una API key enviada en el
  encabezado `Authorization`, y cada solicitud autenticada se resuelve
  contra una tabla de permisos por rol antes de llegar al handler
  correspondiente. Además, cada API key tiene un límite de solicitudes
  por minuto, aplicado mediante un contador de ventana fija sobre Redis.
  El detalle completo de autenticación, autorización y límite de tasa se
  describe en la sección correspondiente más abajo.
- **Worker** hace polling de la tabla `jobs` usando
  `SELECT ... FOR UPDATE SKIP LOCKED`, lo que permite que múltiples
  workers compitan por trabajo sin bloquearse ni duplicar el claim de un
  mismo job. Cada worker corre varios jobs en simultáneo (semáforo,
  `CONCURRENCY`), y además corre en background tres tareas propias:
  - **Heartbeat**: late cada `HEARTBEAT_INTERVAL_MS` en Redis con TTL, para
    que `GET /workers` sepa quién sigue vivo (ver ADR-002).
  - **Reaper**: revisa periódicamente si hay jobs `running` cuyo lease
    venció (el worker que los tenía murió sin avisar) y los recupera
    aplicando la misma política de retry, backoff y dead-letter que un
    fallo normal (ver ADR-003 y ADR-004). Corre en todos los workers a la
    vez, sin coordinador único: si se mata cualquiera con `kill -9` a
    mitad de un job, otro lo recupera solo.
  - **Scheduler de cron**: compite por un advisory lock de PostgreSQL (ver
    ADR-006); el que lo consigue es el único que escanea y dispara los
    `cron_schedules` vencidos, creando el job real correspondiente.
- **`relay-cli`**: binario de línea de comandos que habla directo con
  PostgreSQL, no con la API, para operar el sistema. Es útil incluso si la
  API está caída, y es también el único medio para crear, listar y
  revocar API keys (ver más abajo, en ADR-007, el motivo de esa decisión).

Redis se usa exclusivamente para coordinación efímera: heartbeats de
worker y contadores de rate limiting. Si se cae, se pierde temporalmente
la visibilidad de quién está vivo y el límite de tasa deja de aplicarse,
pero la recuperación de jobs abandonados y el scheduler de cron siguen
funcionando igual, porque corren enteramente sobre PostgreSQL y nunca
dependen de Redis. Esto está probado explícitamente en
`crates/common/tests/recovery.rs` y `crates/common/tests/scheduling.rs`,
que no levantan Redis en ningún momento.

Tanto la API como el worker manejan SIGTERM y Ctrl+C de forma ordenada: la
API termina las solicitudes en curso antes de cerrar, y el worker deja de
reclamar jobs nuevos y espera a que los que ya están corriendo terminen,
con un plazo máximo definido por `SHUTDOWN_GRACE_SECONDS`, antes de
salir.

## Inicio rápido

```bash
cp .env.example .env
docker compose up --build
```

Esto levanta PostgreSQL, Redis, la API en `:8080` y 3 workers (configurable
con `docker compose up --build --scale worker=N`).

## Autenticación y autorización

La API protege sus endpoints operativos mediante API keys. Cada key se
genera con el formato `dq_<prefijo_de_8_caracteres>_<secreto_de_32_bytes_en_base64url>`,
por ejemplo `dq_a1b2c3d4_xYz123...`. La key completa se muestra una única
vez, en el momento de crearla: la base de datos solo conserva el prefijo,
en texto plano, y el hash SHA-256 de la key completa. La verificación de
una key entrante se realiza en tiempo constante, para no filtrar
información por temporización, y la revocación de una key es inmediata en
cuanto se marca su columna `revoked_at`. El razonamiento completo detrás
de estas decisiones está documentado en [ADR-007](./docs/adr/ADR-007-api-keys.md).

Existen tres roles, cada uno con un conjunto de endpoints permitidos:

| Rol      | Endpoints permitidos |
|----------|-----------------------|
| producer | `POST /jobs`, `DELETE /jobs/:id`, y lectura general (`/jobs`, `/jobs/:id`, `/stats`, `/metrics`, `/workers`) |
| worker   | Solo lectura: `GET /jobs`, `/jobs/:id`, `/jobs/:id/attempts`, `/stats`, `/metrics`, `/workers` |
| admin    | Todo lo anterior, además de la gestión de cron schedules (`GET`/`POST /cron`, `GET`/`DELETE /cron/:id`) |
| público  | `/`, `/health`, `/ready`, sin necesidad de key |

El dashboard web, servido en la ruta raíz, es en sí mismo público, pero su
JavaScript solicita una API key al abrirlo, la guarda en `localStorage`
del navegador, y la incluye en el encabezado `Authorization` de cada
solicitud que hace hacia la API.

### Crear una API key

Las API keys se gestionan exclusivamente a través de `relay-cli`, que
opera directo contra PostgreSQL y no requiere una key propia para
funcionar:

```bash
cargo run -p relay-cli -- api-key create --name "mi-servicio" --role producer
# => creada: mi-servicio (id ...)
# => clave (se muestra una sola vez, no se puede recuperar):
#    dq_xyz12345_abcdefghijklmnopqrstuvwxyz1234567890ABCD
```

La key generada debe guardarse de inmediato en el archivo `.env` del
cliente que la va a usar, o en el gestor de secretos correspondiente, ya
que no hay forma de volver a consultarla después de este momento.

### Límite de tasa

Cada API key tiene un límite de solicitudes por minuto, configurable
mediante variables de entorno:

| Variable                         | Valor por defecto | Descripción |
|-----------------------------------|--------------------|--------------|
| `RATE_LIMIT_PRODUCER_PER_MINUTE`  | 300                | Límite para el rol producer (0 significa sin límite) |
| `RATE_LIMIT_WORKER_PER_MINUTE`    | 300                | Límite para el rol worker |
| `RATE_LIMIT_ADMIN_PER_MINUTE`     | 0                  | El rol admin no tiene límite por defecto |

Cuando una key supera su límite, la API responde con `429 Too Many
Requests` e incluye un encabezado `Retry-After` con la cantidad de
segundos que restan hasta la próxima ventana. Si Redis no está disponible
en el momento de evaluar el límite, la solicitud se deja pasar y se
registra una advertencia en los logs: el límite de tasa se desactiva
temporalmente en lugar de convertirse en un motivo de error 500 para toda
la API. El razonamiento detrás de esta decisión está documentado en
[ADR-008](./docs/adr/ADR-008-rate-limiting.md).

### Probar la recuperación de un worker caído

```bash
# job artificialmente lento, para tener tiempo de finalizar el worker que lo tome
KEY=$(cargo run -p relay-cli -- api-key create --name "demo" --role producer 2>&1 | grep -A1 "clave (se muestra" | tail -1 | xargs)
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $KEY" \
  -d '{"type": "sleep", "payload": {"seconds": 25}, "timeout_seconds": 30}'

# consultar qué worker lo reclamó
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id> | grep worker_id

# finalizar ese worker de forma abrupta
docker kill <container_del_worker_que_lo_tenia>

# esperar a que venza lease_until más un ciclo de reaper (15 segundos por
# defecto) y observar cómo otro worker lo recupera y lo ejecuta nuevamente
curl -H "Authorization: Bearer $KEY" localhost:8080/jobs/<id>/attempts
```

### Probar un cron schedule

```bash
ADMIN_KEY=$(cargo run -p relay-cli -- api-key create --name "admin" --role admin 2>&1 | grep -A1 "clave (se muestra" | tail -1 | xargs)
curl -X POST localhost:8080/cron \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -d '{"name": "cada-minuto", "cron_expr": "* * * * *", "type": "noop"}'

# esperar un minuto más un ciclo de scheduler (10 segundos por defecto)
# y observar que se disparó solo
curl -H "Authorization: Bearer $ADMIN_KEY" localhost:8080/cron/<id>
# => last_run_at ya tiene valor, next_run_at avanzó al próximo minuto
```

### Probar el apagado ordenado

```bash
# enviar un job lento y verificar qué worker lo tomó
KEY=$(cargo run -p relay-cli -- api-key create --name "demo" --role producer 2>&1 | grep -A1 "clave (se muestra" | tail -1 | xargs)
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $KEY" \
  -d '{"type": "sleep", "payload": {"seconds": 8}, "timeout_seconds": 30}'

# enviarle la señal SIGTERM (docker stop la envía automáticamente)
docker kill --signal=TERM <container_del_worker>
# en un entorno local: kill -TERM <pid>

# el worker deja de reclamar trabajo nuevo, espera a que el job de 8
# segundos termine por completo, y recién entonces cierra. Revisar sus
# logs para ver la secuencia completa: worker_draining, worker_drained,
# worker_stopped.
```

### Usar la CLI

`relay-cli` habla directo con Postgres, no con la API, por lo que sigue
funcionando aunque la API esté caída:

```bash
# Jobs
cargo run -p relay-cli -- jobs list --status dead_letter
cargo run -p relay-cli -- jobs attempts <id>
cargo run -p relay-cli -- stats

# Cron
cargo run -p relay-cli -- cron create --name reporte-diario --expr "0 6 * * *" --type generate_report

# API keys
cargo run -p relay-cli -- api-key create --name "mi-servicio" --role producer
cargo run -p relay-cli -- api-key list
cargo run -p relay-cli -- api-key revoke <prefijo>

# Benchmarks
cargo run -p relay-cli -- bench --jobs 1000 --type noop
cargo run -p relay-cli -- --help
```

### Benchmarks y rendimiento

`relay-cli bench` mide latencia de envío, cola y ejecución con datos
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

El test de concurrencia (`crates/common/tests/concurrency.rs`, con cien
jobs y diez workers simulados), el de reliability
(`crates/common/tests/reliability.rs`, que sigue la secuencia de retry,
retry y dead-letter), el de recovery (`crates/common/tests/recovery.rs`,
lease vencido que deriva en retry o dead-letter) y el de scheduling
(`crates/common/tests/scheduling.rs`, disparo de cron y liderazgo)
necesitan todos un Postgres real. No tiene sentido mockearlos, porque lo
que se está probando es el comportamiento real de `SKIP LOCKED`, la
lógica de backoff y recuperación calculada en SQL, y el advisory lock de
Postgres, todo bajo condiciones reales. Notablemente, ni el test de
recovery ni el de scheduling necesitan Redis (ver ADR-002 y ADR-006): tanto la recuperación
de jobs abandonados como el scheduler de cron corren enteramente sobre
Postgres. Si no hay base disponible, los tests se saltan solos con un
aviso en vez de romper la suite. En CI corren siempre (ver
`.github/workflows/ci.yml`).

Nota sobre aislamiento: estos tests comparten una sola instancia de
PostgreSQL y están escritos para tolerarlo, mediante una marca de
`job_type` o `name` única por corrida y tolerancia a jobs reclamados por
otro test que se ejecute en simultáneo. Si la suite se ejecuta justo
después de probar el sistema a mano, por ejemplo siguiendo alguna de las
demostraciones anteriores, y se produce un fallo inesperado, conviene
truncar las tablas y ejecutar la suite nuevamente:

```bash
psql "$DATABASE_URL" -c "TRUNCATE jobs, job_attempts, workers, cron_schedules RESTART IDENTITY CASCADE;"
```

## Endpoints

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

Para operar sin pasar por HTTP, ver `relay-cli` más arriba.

## Estado del proyecto

Ver [`PHASES.md`](./PHASES.md).

## Decisiones de arquitectura

Ver [`docs/adr/`](./docs/adr).

## Garantías del sistema

- **Entrega al menos una vez.** Un job aceptado no se pierde
  silenciosamente, aunque puede llegar a ejecutarse más de una vez ante
  ciertos fallos (ver ADR-003). Los handlers deberían diseñarse para ser
  idempotentes siempre que sea posible.
- **PostgreSQL como fuente de verdad.** Cualquier dato crítico para la
  correctitud del sistema vive en PostgreSQL, nunca únicamente en Redis.
  Esto incluye las métricas expuestas en `/metrics`, que se calculan en
  el momento de la consulta y no se acumulan en memoria de proceso, de
  modo que un reinicio no pierde ningún dato.
- **Concurrencia sin pérdida ni duplicación.** Verificado con un test de
  integración contra PostgreSQL real, con cien jobs y diez workers
  ejecutándose de forma concurrente, no solo como una expectativa teórica
  del diseño.
- **Reintentos con backoff, no reintentos infinitos.** Un job fallido
  reintenta con backoff exponencial y variación aleatoria hasta agotar
  `max_attempts`; a partir de ahí pasa a `dead_letter` y permanece en ese
  estado, sin ciclos de reintento silenciosos que puedan ocultar un
  problema real. Verificado con un test de integración que sigue la
  secuencia completa de reintentos hasta llegar a dead-letter.
- **Recuperación ante la caída de un worker.** Si un worker finaliza de
  forma abrupta a mitad de la ejecución de un job, ya sea por una falla,
  quedarse sin memoria, o una terminación forzada del proceso, otro
  worker detecta el lease vencido y recupera el job aplicando la misma
  política de reintentos y dead-letter que un fallo reportado
  normalmente. Verificado tanto con un test de integración
  (`crates/common/tests/recovery.rs`) como con una prueba manual: forzar
  la finalización de un worker a mitad de un job de veinticinco segundos
  y confirmar que otro worker lo retoma y lo completa.
- **Disparo de cron sin duplicados.** Un cron schedule solo lo dispara el
  líder vigente, determinado mediante un advisory lock de PostgreSQL (ver
  ADR-006), y el job que se crea lleva además un identificador de
  idempotencia derivado como capa adicional de seguridad: un doble
  disparo del mismo horario no genera dos jobs. Verificado con un test de
  integración y con una prueba manual: un schedule creado a través de la
  API se disparó sin intervención manual, con el horario correcto.
- **Ningún job en curso se interrumpe por un despliegue.** La señal
  SIGTERM hace que el worker deje de reclamar trabajo nuevo y espere,
  hasta un plazo máximo configurado mediante `SHUTDOWN_GRACE_SECONDS`, a
  que los jobs en curso terminen antes de finalizar el proceso. Verificado
  con una prueba manual: se envió SIGTERM a mitad de un job de ocho
  segundos, y el worker esperó los ocho segundos completos antes de
  cerrar.
- **Autenticación y autorización verificadas de extremo a extremo.** Cada
  endpoint operativo exige una API key válida y un rol autorizado para esa
  ruta específica; una key inexistente, revocada, o con un rol
  insuficiente recibe siempre una respuesta rechazada. Verificado con
  tests de integración que cubren los tres roles contra la tabla completa
  de rutas de la aplicación.
