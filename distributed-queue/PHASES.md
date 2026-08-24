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

## ⬜ Fase 4 — Distributed Failure Recovery
- [ ] Heartbeats de worker
- [ ] Worker leases (`lease_until`)
- [ ] Detección de workers muertos
- [ ] Recuperación de jobs abandonados
- [ ] Redis para coordinación efímera
- [ ] ADR-002, ADR-003, ADR-004

## ⬜ Fase 5 — Scheduling
- [ ] Delayed jobs (`scheduled_at`)
- [ ] Cron jobs
- [ ] Leader election / lease para scheduler distribuido

## ⬜ Fase 6 — Operational Features
- [ ] Métricas Prometheus
- [ ] Dashboard web
- [ ] CLI
- [ ] Graceful shutdown (SIGTERM)

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
