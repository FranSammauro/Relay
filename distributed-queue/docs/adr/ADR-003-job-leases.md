# ADR-003: Leases de job fijos al claim, no renovables (MVP)

## Estado
Aceptado (Fase 4)

## Contexto
`timeout_seconds` (Fase 3) mata un job que se cuelga *mientras el proceso
del worker sigue vivo* — es `tokio::time::timeout` alrededor del handler,
así que si el worker entero desaparece (`kill -9`, OOM, el nodo se cae),
no hay ningún timeout in-process que se dispare: el job queda `running`
para siempre, con nadie que lo termine ni lo reporte como fallido.

Se necesita una segunda señal, externa al proceso que ejecuta el job, que
diga "este job debería haber terminado a esta hora; si no pasó nada
para entonces, asumamos que el worker murió".

## Decisión
Cada vez que `claim_next_job` le asigna un job a un worker, fija
`lease_until = now() + (timeout_seconds + LEASE_GRACE_SECONDS)`, con
`LEASE_GRACE_SECONDS = 30` fijo (no configurable por ahora). El lease se
fija una única vez al momento del claim — **no se renueva** mientras el
job corre.

Esto es una decisión de scope explícita, no un descuido: un lease
renovable necesitaría que cada job en ejecución mande su propio heartbeat
periódico (heartbeat *por job*, no por worker), lo cual es una pieza de
infraestructura entera aparte. Para el tipo de jobs de este sistema
(acotados por `timeout_seconds`, que ya de por sí es un techo razonable),
un lease fijo con margen es suficiente. Si en el futuro hace falta soportar
jobs de duración muy variable o desconocida de antemano, un lease
renovable es el próximo paso — documentado acá, no implementado a presión.

El reaper (ADR-004) es quien actúa sobre un lease vencido: recupera el job
aplicando la misma política de retry/backoff/DLQ que un fallo reportado
normalmente (`Storage::transition_after_failure`, compartida entre ambos
caminos).

## Consecuencias
- **At-least-once delivery, no exactly-once.** Si un worker termina un job
  con éxito pero muere antes de que el `UPDATE ... SET status =
  'completed'` llegue a confirmarse, el lease eventualmente vence y otro
  worker vuelve a ejecutar el mismo job. Los handlers deberían ser
  idempotentes cuando sea posible — esto ya estaba anticipado en el README
  desde Fase 1.
- El costo de un worker caído no es instantáneo: el job abandonado queda
  "en el limbo" (status `running`, sin dueño real) hasta por
  `timeout_seconds + 30s`, que es el margen que la Fase 8 (rate limiting /
  producción) debería tener en cuenta al definir SLAs.
- `timeout_seconds` sigue siendo la primera línea de defensa (rápida,
  in-process); el lease es la red de seguridad para cuando esa primera
  línea ni siquiera llega a correr.
