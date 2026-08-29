# ADR-008: Rate Limiting por API Key (Sliding Window en Redis)

## Estado
Aceptado (Fase 8)

## Contexto
La API necesita protegerse de abusos accidentales o maliciosos (p.ej. bucles
infinitos de reintentos, spamming de jobs). Requisitos:

- Límite configurable por rol (producer/worker/admin).
- Ventana de tiempo simple (sin token bucket complejo).
- Fallback **fail-open**: si Redis no está disponible, **dejar pasar** la
  request y loguear warning. Nunca 500 por rate limiting; coherente con
  ADR-002 (Redis = coordinación efímera).
- Header `Retry-After` en 429 con segundos al próximo window.

## Decisión
1. **Algoritmo**: Sliding window counter con ventanas fijas de 60 segundos.
   - Clave Redis: `ratelimit:{key_id}:{window_epoch}` donde
     `window_epoch = now_unix / 60`.
   - `INCR` atómico. Si el valor devuelto es 1 → `EXPIRE key 60` (solo en
     creación). El contador viejo expira solo.
   - Límite por rol (configurable por env):
     - `RATE_LIMIT_PRODUCER_PER_MINUTE` (default 300).
     - `RATE_LIMIT_WORKER_PER_MINUTE` (default 300).
     - `RATE_LIMIT_ADMIN_PER_MINUTE` (default 0 = sin límite, saltea Redis).
2. **Decisión**:
   - Dentro del límite → `Allowed`.
   - Excedido → `Denied { retry_after_secs }` (segundos al fin del window
     actual: `60 - (now % 60)`, mínimo 1).
   - Error Redis → `Allowed` + `tracing::warn` (fail-open).
3. **Integración axum**:
   - Corre **por dentro** del middleware de auth (necesita `AuthContext.key_id` y `role`).
   - Rutas públicas (`/health`, `/ready`, `/`) no tienen `AuthContext` →
     `rate_limit_middleware` devuelve `Allowed` sin tocar Redis.
   - Respuesta 429 incluye `Retry-After: <secs>` y body JSON
     `{"error":"rate limit exceeded","retry_after_secs":N}`.

## Alternativas consideradas
- **Token bucket / leaky bucket**: más suave en bordes de ventana, pero
  requiere estado persistente más complejo (último timestamp + tokens).
  Ventana fija + counter es determinista, simple y el error de borde
  (ráfaga en límite de ventana) es aceptable para este caso de uso.
- **Rate limiting en aplicación (memoria local)**: no funciona con
  múltiples réplicas de la API (Fase 7 despliega 3 workers, la API escala
  igual). Redis centraliza el contador.
- **Rate limiting en Postgres**: añade carga a la BD de verdad (ADR-001).
  Redis está diseñado para contadores efímeros de alta frecuencia.
- **Fail-closed (rechazar si Redis cae)**: rompe disponibilidad.
  Preferimos "temporalmente sin rate limiting" sobre "toda la API caída".
  El peor caso real es un pico de tráfico sin límite hasta que Redis
  recupera; mitigado por timeouts y circuit breakers en clientes.

## Consecuencias
- Límite por defecto 300 req/min por key (5 req/seg) — razonable para
  productores automatizados. Ajustable por env sin redeploy.
- Admin (rol interno, key operativa) sin límite: operaciones de
  mantenimiento no deben bloquearse.
- Tests de integración limpian contadores entre tests (`DEL ratelimit:{key_id}:*`).
- Métrica `retry_after_secs` expuesta al cliente para backoff educado.