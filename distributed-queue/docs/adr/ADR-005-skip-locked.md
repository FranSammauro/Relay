# ADR-005: `FOR UPDATE SKIP LOCKED` para el claim de jobs

## Estado
Aceptado (Fase 1)

## Contexto
Cuando existan múltiples workers haciendo polling concurrente sobre la
misma tabla `jobs`, dos o más de ellos podrían intentar reclamar el mismo
job pendiente al mismo tiempo. Sin un mecanismo de exclusión, esto llevaría
a ejecución duplicada innecesaria o a condiciones de carrera sobre el
estado del job.

## Decisión
El claim de un job se implementa como una transacción que hace:

```sql
SELECT ...
FROM jobs
WHERE status = 'pending' AND scheduled_at <= now()
ORDER BY priority DESC, created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1
```

`SKIP LOCKED` hace que, si otra transacción ya tiene bloqueada una fila
candidata, esta consulta simplemente la ignore y pase a la siguiente, en
vez de esperar a que se libere el lock. Esto evita que los workers se
serialicen entre sí esperando sobre el mismo registro.

## Consecuencias
- El claim es seguro para N workers concurrentes sin necesidad de un lock
  externo (Redis, etcd, etc.).
- La query no garantiza estricto orden global bajo alta contención (un
  worker podría saltarse temporalmente una fila bloqueada por otro), lo
  cual es aceptable: el objetivo es prioridad aproximada, no un orden FIFO
  perfecto.
- El comportamiento bajo alta concurrencia (lock contention, throughput de
  claim) se valida con tests dedicados en Fase 2 y se mide en Fase 7.
