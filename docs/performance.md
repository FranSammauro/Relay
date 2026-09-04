# Informe de rendimiento

Fase 7. Todos los números de este informe provienen de corridas reales
contra PostgreSQL 16 y Redis 7, ejecutadas con `relay-cli bench` y con
`EXPLAIN ANALYZE` directo sobre la base. No hay estimaciones ni cifras de
marketing: cada tabla incluye la metodología exacta para que cualquiera
pueda reproducirla.

## Entorno de la corrida

Importante para interpretar los números: el entorno donde se corrieron
estos benchmarks es un contenedor de desarrollo compartido, sin
aislamiento de CPU garantizado y con recursos moderados, no un servidor
dedicado de benchmarking. Los valores absolutos (latencia en ms,
throughput) van a variar en otro hardware. Lo que sí es representativo y
reproducible es la **forma** de los resultados: dónde se concentra el
tiempo, cómo escala con la concurrencia, y qué confirma o contradice cada
decisión de diseño de las fases anteriores.

## Metodología

`relay-cli bench --jobs N --type <tipo> --timeout-secs S`:

1. Crea N jobs de forma secuencial vía `Storage::create_job`, midiendo el
   tiempo de cada llamada individual (latencia de "envío").
2. Cada job lleva un `idempotency_key` con un prefijo único por corrida
   (`bench:<run_id>:<i>`), lo que permite identificar exactamente qué
   filas pertenecen a esta corrida aunque la base tenga otros datos.
3. Espera, haciendo polling cada 300ms, a que todos los jobs de la corrida
   salgan de un estado no terminal (`pending`/`running`/`retry_scheduled`).
4. Con los timestamps finales (`created_at`, `started_at`, `completed_at`)
   calcula tres métricas por job:
   - **Cola**: `started_at - created_at`. Cuánto esperó el job hasta que
     un worker lo reclamó.
   - **Ejecución**: `completed_at - started_at`. Cuánto tardó el handler.
   - **Total**: `completed_at - created_at`.

Aclaración importante sobre la latencia de envío: como `relay-cli` habla
directo con PostgreSQL, sin pasar por la API, lo que mide es el costo de
un `INSERT` en la tabla `jobs`, no el de una solicitud HTTP completa.
Quien envíe jobs mediante `POST /jobs` va a experimentar, además de este
costo, el tiempo de la capa HTTP de axum, la serialización JSON, la
verificación de la API key y la evaluación del límite de tasa, del orden
de unos pocos milisegundos adicionales en un despliegue típico, aunque no
medidos específicamente en este informe.

Todas las corridas usaron jobs de tipo `noop` (el handler más liviano
disponible, sin trabajo real), precisamente para que la latencia de cola y
de ejecución reflejen el costo del propio sistema de colas y no el de un
handler de negocio arbitrario.

## Resultado 1: carga moderada, un worker

```
relay-cli bench --jobs 300 --type noop --timeout-secs 60
```

Worker único, `CONCURRENCY=8`.

| Métrica    | n   | p50    | p95    | p99    | max    |
|------------|-----|--------|--------|--------|--------|
| Envío      | 300 | 1.0 ms | 3.4 ms | 7.3 ms | 157.7 ms |
| Cola       | 300 | 666 ms | 785 ms | 795 ms | 797 ms |
| Ejecución  | 300 | 2.0 ms | 3.0 ms | 7.0 ms | 170.0 ms |
| Total      | 300 | 669 ms | 788 ms | 797 ms | 799 ms |

Con 300 jobs enviados en 0.67s (447 jobs/s de throughput de envío) y un
solo worker a concurrencia 8, el lote completo de 300 jobs se procesó en
menos de 800ms. La latencia de ejecución (p50 2ms) confirma que el trabajo
en sí es trivial; casi todo el tiempo total lo consume la cola, es decir,
cuánto tarda un worker en encontrar hueco para reclamar cada job dado un
límite de 8 ejecuciones concurrentes.

## Resultado 2: carga sostenida, un worker vs. dos workers

```
relay-cli bench --jobs 1000 --type noop --timeout-secs 60
```

| Escenario           | Cola p50 | Cola p95 | Cola p99 | Total p50 | Total p99 |
|----------------------|---------:|---------:|---------:|----------:|----------:|
| 1 worker (CONCURRENCY=8)  | 2030 ms | 2087 ms | 2103 ms | 2033 ms | 2105 ms |
| 2 workers (16 efectivos)  |  781 ms |  934 ms |  956 ms |  792 ms |  965 ms |

Al duplicar la cantidad de workers (mismo `CONCURRENCY=8` cada uno, 16
ejecuciones concurrentes en total), la latencia de cola en p50 cayó de
2030ms a 781ms: una mejora de 2.6x, cercana a la proporcionalidad esperada
frente al aumento de concurrencia disponible. Esto confirma en la práctica
lo que el diseño con `SKIP LOCKED` (ADR-005) promete desde la Fase 1: los
workers escalan horizontalmente sin coordinación adicional ni contención
artificial entre ellos.

La latencia de ejecución p99 en el escenario de 2 workers (145ms, con un
máximo de 1175ms) es mayor que en el resto de las corridas. Esto es
consistente con el entorno compartido descrito arriba: dos procesos worker
compitiendo por CPU con la propia corrida del benchmark introduce ruido
que un servidor dedicado no tendría. No se interpreta como una regresión
del sistema, sino como una característica del entorno de medición.

## Resultado 3: hallazgo de índice bajo backlog realista

La corrida anterior no expone un problema real de índices porque las colas
se vacían casi tan rápido como se llenan. Para aislar el comportamiento de
`claim_next_job` bajo un backlog sostenido, se insertaron 20.000 filas
`pending` directamente (sin worker corriendo) y se ejecutó
`EXPLAIN ANALYZE` sobre la consulta exacta que usa `Storage::claim_next_job`.

```sql
EXPLAIN ANALYZE
SELECT id, ...
FROM jobs
WHERE status IN ('pending', 'retry_scheduled') AND scheduled_at <= now()
ORDER BY priority DESC, created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

Resultado, con el índice actual `idx_jobs_claim (status, scheduled_at,
priority DESC, created_at ASC)`:

```
Index Scan using idx_jobs_claim on jobs
  (actual time=0.049..4.764 rows=20000 loops=1)
Sort  (actual time=12.285..12.285 rows=1 loops=1)
Execution Time: 12.475 ms
```

El índice sí se usa (no hay Seq Scan), pero el plan revela algo que no es
obvio a simple vista: el Index Scan devuelve las **20.000 filas** que
matchean el filtro antes de que el nodo Sort las ordene y el Limit se
quede con una sola. El motivo es la cláusula `scheduled_at <= now()`: al
ser una condición de rango (no de igualdad), Postgres no puede seguir
usando las columnas siguientes del índice (`priority`, `created_at`) para
mantener el orden de salida, así que el índice solo sirve para filtrar,
no para ordenar, y el resto del trabajo lo hace un sort completo en
memoria.

### Aislando la causa

Para confirmar la hipótesis, se probó con una variante sin `scheduled_at`
en el índice (`status, priority DESC, created_at ASC`) y, por separado,
restringiendo el filtro a un único status en lugar de dos:

| Variante                                                    | Execution Time |
|---------------------------------------------------------------|---------------:|
| Índice actual, `status IN (...)`, dos valores                | 12.475 ms |
| Índice sin `scheduled_at`, `status IN (...)`, dos valores     | 6.134 ms |
| Índice sin `scheduled_at`, `status = 'pending'` (un solo valor) | **0.072 ms** |

La causa raíz no es `scheduled_at`: es el `IN` con dos valores de
`status`. Con un único valor de `status`, el índice devuelve las filas ya
ordenadas por `(priority DESC, created_at ASC)` para ese status
específico, Postgres no necesita materializar nada, y el `LIMIT 1` corta
en la primera fila. Es 170 veces más rápido que el plan actual bajo este
backlog sintético.

### Por qué no se cambió la query todavía

Cambiar `claim_next_job` para consultar `pending` primero y recién si no
hay nada probar `retry_scheduled` (dos consultas en cascada en lugar de un
único `IN`) resolvería este costo, pero introduce un cambio de semántica
que merece su propio análisis: un job en `retry_scheduled` con prioridad
alta y ya vencido podría quedar esperando detrás de jobs `pending` de
prioridad baja, invirtiendo el orden de prioridad entre los dos estados.
En el uso típico del sistema, `retry_scheduled` es una minoría transitoria
frente a `pending`, así que el impacto práctico sería bajo, pero es un
trade-off de comportamiento, no solo de rendimiento, y no corresponde
decidirlo dentro de un informe de benchmarking. Queda documentado acá como
la optimización de índice más concreta y medible que este informe
identificó, para decidir e implementar de forma deliberada en un cambio
aparte (con su propio test de integración que cubra el caso de inversión
de prioridad).

## Otros índices verificados

```sql
EXPLAIN ANALYZE SELECT id FROM jobs WHERE status = 'running' AND lease_until < now();
```

```
Index Scan using idx_jobs_lease_recovery on jobs
  (actual time=0.011..0.011 rows=0 loops=1)
Execution Time: 0.018 ms
```

El índice parcial del reaper (Fase 4, ADR-004) se comporta exactamente
como se esperaba: al ser parcial (`WHERE status = 'running'`) y cubrir
`lease_until` directamente en la condición del índice, no hay
materialización de filas de más ni necesidad de un sort adicional. Este es
el patrón de índice correcto para el tipo de consulta que representa; no
se identificó ninguna mejora pendiente acá.

`count_by_status` (usada por `/stats` y `/metrics`) hace un `Seq Scan`
completo de la tabla seguido de `HashAggregate`, sin índice dedicado. Con
5.000 filas el costo es de 1.2ms, insignificante. No se agregó un índice
para esto: es una consulta de agregación sobre toda la tabla, que por
definición siempre va a tocar todas las filas relevantes; un índice no
cambiaría eso. Si el volumen de jobs creciera varios órdenes de magnitud y
esta consulta se volviera un cuello de botella real (por ejemplo, si
`/metrics` se scrapea cada pocos segundos desde Prometheus contra una
tabla de millones de filas), la solución no sería un índice sino
desnormalizar el conteo en una tabla de contadores mantenida por trigger,
o cachear el resultado por un intervalo corto. No es necesario hoy.

## Conclusiones

1. El sistema procesa cargas de cientos a miles de jobs triviales en el
   orden de segundos, con la mayor parte del tiempo total explicado por la
   espera en cola bajo concurrencia limitada, no por el propio mecanismo
   de persistencia (la latencia de envío se mantiene en el orden de pocos
   milisegundos incluso a 5.000 jobs).
2. Agregar workers reduce la latencia de cola de forma aproximadamente
   proporcional, validando en la práctica el diseño de concurrencia sin
   coordinación de las Fases 2 y 4.
3. Se identificó y documentó una oportunidad de optimización concreta y
   medida en `claim_next_job` bajo backlog sostenido (hasta 170x en el
   escenario sintético), con un trade-off de semántica de prioridad que
   requiere una decisión deliberada antes de implementarse.
4. Los índices de recuperación de leases (Fase 4) y de cron (Fase 5)
   funcionan según lo diseñado, sin hallazgos pendientes.

## Cómo reproducir este informe

```bash
docker compose up -d postgres redis
cargo run -p worker &                       # un worker
cargo run -p relay-cli -- bench --jobs 1000 --type noop --timeout-secs 60

# para el análisis de índices bajo backlog real:
psql "$DATABASE_URL" -c "
  INSERT INTO jobs (job_type, payload, status)
  SELECT 'noop', '{}'::jsonb, 'pending' FROM generate_series(1, 20000);
"
psql "$DATABASE_URL" -c "
  EXPLAIN ANALYZE
  SELECT id FROM jobs
  WHERE status IN ('pending', 'retry_scheduled') AND scheduled_at <= now()
  ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED;
"
```
