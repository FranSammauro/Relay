# Fases del proyecto

## ✅ Fase 1 — Core Queue
- [x] Modelo de Job en PostgreSQL (`migrations/0001_init.sql`)
- [x] API HTTP (`POST/GET/DELETE /jobs`, `/health`, `/ready`)
- [x] Worker con loop de ejecución (claim -> execute -> ack)
- [x] Claim seguro para concurrencia (`FOR UPDATE SKIP LOCKED`) desde el día 1
- [x] Estados básicos: pending, running, completed, failed, cancelled
- [x] Docker + docker-compose (postgres, api, worker)
- [x] Logs estructurados (JSON) de eventos clave
- [x] ADR-001, ADR-005

## ✅ Fase 2 — Concurrency
- [x] Múltiples workers concurrentes (`docker-compose.yml` levanta 3 réplicas por default, escalable con `--scale worker=N`)
- [x] Límite de tareas concurrentes por worker (semáforo, `CONCURRENCY` env var)
- [x] Registro de workers (tabla `workers` — solo registro, sin heartbeat; eso es Fase 4)
- [x] Test de concurrencia (100 jobs / 10 workers, 0 perdidos, 0 duplicados) — `crates/common/tests/concurrency.rs`
- [x] `queue_depth` observable (`GET /stats`, conteo por estado — versión JSON previa al Prometheus real de Fase 6)
- [x] CI mínimo (GitHub Actions + Postgres real) para que el test de concurrencia corra en cada push, no solo en la laptop de uno

## ✅ Fase 3 — Reliability
- [x] Retries con `max_attempts` (ya modelado desde Fase 1, ahora aplicado de verdad)
- [x] Exponential backoff + jitter (calculado en SQL: `min(2s·2^(attempts-1), 300s) + random()·2s`)
- [x] Dead-letter queue (`status = 'dead_letter'`, terminal, no reclamable)
- [x] Timeouts por job (`timeout_seconds`, default 30s, `tokio::time::timeout` alrededor del handler)
- [x] Tabla `job_attempts` (historial completo por intento: worker, inicio, fin, resultado, error)
- [x] `GET /jobs/:id/attempts` para consultar el historial
- [x] Tests de integración contra Postgres real: retry → retry → dead_letter, y attempt exitoso

## ✅ Fase 4 — Distributed Failure Recovery
- [x] Heartbeats de worker (Redis, TTL = 3x `HEARTBEAT_INTERVAL_MS`, ver ADR-002)
- [x] Worker leases (`lease_until = claim_time + timeout_seconds + 30s`, fijo al claim, ver ADR-003)
- [x] Detección de workers muertos (`GET /workers`, cruza registro de Postgres con liveness de Redis)
- [x] Recuperación de jobs abandonados (reaper descentralizado en cada worker, ver ADR-004)
- [x] Redis para coordinación efímera (`common::Heartbeats`) — fuera del camino crítico de correctitud
- [x] ADR-002, ADR-003, ADR-004
- [x] Tests de integración: recovery vía Postgres puro (sin Redis) — retry y dead_letter tras lease vencido
- [x] Smoke test manual real: `kill -9` a un worker a mitad de un job de 25s, otro worker lo recupera y completa (verificado end-to-end, no solo en tests automatizados)

## ✅ Fase 5 — Scheduling
- [x] Delayed jobs (`scheduled_at`) — ya funcionaba desde Fase 1, validado explícitamente con test de integración
- [x] Cron jobs (tabla `cron_schedules`, parser de cron propio sin dependencias externas en `common::cron`, 11 tests unitarios)
- [x] Leader election / lease para scheduler distribuido (advisory lock de sesión de Postgres, ver ADR-006 — sin Redis, sin TTL que mantener)
- [x] Tests de integración contra Postgres real: disparo de cron con avance de `next_run_at`, idempotencia ante doble disparo, y exclusión mutua del liderazgo
- [x] Smoke test manual real: cron schedule creado vía API, disparado solo por el scheduler, job creado y completado — verificado end-to-end por HTTP

## ✅ Fase 6 — Operational Features
- [x] Métricas Prometheus (`GET /metrics`, calculadas al vuelo desde Postgres — sin contadores en memoria de proceso, `job_attempts` como `_total`, percentiles de duración vía `percentile_cont` de SQL)
- [x] Dashboard web (`GET /`, HTML+JS estático sin build step ni dependencias nuevas, polling cada 4s sobre los endpoints existentes)
- [x] CLI (`queue-cli`, nuevo binario del workspace, habla directo con Postgres via `common::Storage`, parsing de argumentos a mano sin `clap`)
- [x] Graceful shutdown (SIGTERM/Ctrl+C compartido en `common::shutdown`; API drena requests en vuelo, worker deja de reclamar y espera a que terminen los jobs en curso con un plazo de `SHUTDOWN_GRACE_SECONDS` antes de salir, y limpia su heartbeat de Redis al bajar)
- [x] Validado manualmente de punta a punta: SIGTERM real a mitad de un job de 8s — el worker esperó a que terminara completo antes de cerrar

## ⬜ Fase 7 — Performance
- [ ] Benchmarks reproducibles (submission/queue/execution latency)
- [ ] Profiling y optimización de índices
- [ ] Análisis de cuellos de botella

## ⬜ Fase 8 — Production Polish
- [ ] Autenticación (API keys) y autorización (producer/worker/admin)
- [ ] Rate limiting
- [ ] CI extendido (build + push de imágenes Docker, releases) — el compile+test básico ya corre desde Fase 2
- [ ] Tests de integración y de fallos
- [ ] Threat model
- [ ] Documentación final y ADRs restantes
