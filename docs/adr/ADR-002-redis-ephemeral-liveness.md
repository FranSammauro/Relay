# ADR-002: Redis solo para liveness efímera, nunca para estado de jobs

## Estado
Aceptado (Fase 4)

## Contexto
Desde Fase 4 el sistema necesita responder "¿qué workers están vivos ahora
mismo?" para observabilidad (`GET /workers`). Esta pregunta es distinta de
"¿qué workers se registraron alguna vez?" (eso ya lo contesta la tabla
`workers` en Postgres desde Fase 2) y distinta también de "¿este job
concreto sigue teniendo un dueño activo?" (eso lo contesta el lease de la
Fase 4, ver ADR-003, que no depende de esto en absoluto).

La señal de "estoy vivo" de un worker es, por naturaleza, efímera: importa
el dato de los últimos segundos, y el dato viejo no solo no sirve sino que
activamente confunde ("¿este worker está colgado o ya nadie lo actualiza
hace una hora?").

## Decisión
El heartbeat de cada worker se escribe en Redis como una clave con TTL
(`worker:heartbeat:{id}`, ver `common::heartbeats`), no en una columna de
Postgres. Cada worker la refresca cada `HEARTBEAT_INTERVAL_MS`; si deja de
hacerlo, la clave expira sola.

Regla general (ya establecida en ADR-001): si perder Redis implica perder
información crítica sobre un job, esa información va en Postgres. El
heartbeat no califica: perderlo degrada observabilidad, no correctitud.

## Consecuencias
- Si Redis se cae, `GET /workers` deja de poder distinguir vivos de
  muertos (todos aparecen como no vivos, que es el fallo seguro: es
  preferible subestimar quién está vivo antes que sobreestimarlo). Sin
  embargo, la recuperación de jobs abandonados (el reaper, ADR-004) sigue
  funcionando exactamente igual, porque corre enteramente sobre
  `lease_until` en Postgres y nunca toca Redis. Esto está probado
  explícitamente: `tests/recovery.rs` no levanta Redis en ningún momento.
- No hace falta un cron ni un job de limpieza para heartbeats viejos, ya
  que el TTL de Redis hace ese trabajo sin costo adicional.
- El rate limiting de la Fase 8 (ver ADR-008) terminó usando esta misma
  instancia de Redis, confirmando la previsión original de que serviría
  para más que heartbeats. El liderazgo del scheduler de cron, en cambio,
  se resolvió con un advisory lock de PostgreSQL (ver ADR-006) en lugar
  de Redis, porque esa decisión concreta tenía requisitos de correctitud
  que encajaban mejor con la regla general de este mismo ADR.
