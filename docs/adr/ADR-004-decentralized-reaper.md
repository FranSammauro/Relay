# ADR-004: Reaper descentralizado (cada worker recupera, no hay coordinador)

## Estado
Aceptado (Fase 4)

## Contexto
Alguien tiene que revisar periódicamente si hay jobs con el lease vencido
y recuperarlos (ver ADR-003). Las opciones típicas son: (a) un proceso
coordinador dedicado que hace esto y nada más, o (b) que cada worker lo
haga por su cuenta, todos corriendo el mismo chequeo en paralelo.

## Decisión
Cada worker ejecuta su propio reaper en una tarea de fondo, con un
intervalo configurable mediante `REAPER_INTERVAL_MS` (quince segundos por
defecto), sin coordinación entre ellos más allá de lo que ya provee
PostgreSQL. No existe un rol de coordinador separado.

La seguridad ante múltiples reapers corriendo a la vez la da
`SELECT ... FOR UPDATE SKIP LOCKED` dentro de la misma transacción que
hace la transición de estado (`Storage::reap_expired_leases`): si dos
workers agarran el lease vencido del mismo job al mismo tiempo, uno se
queda con la fila bloqueada hasta terminar de transicionarla, el otro
simplemente no la ve (`SKIP LOCKED`) y sigue de largo. Nunca hay doble
recovery del mismo job.

## Consecuencias
- **Sin punto único de falla para la recuperación.** Un coordinador
  dedicado que se cae deja de recuperar jobs abandonados hasta que
  alguien lo reinicie, lo cual es, literalmente, el mismo problema que se
  está tratando de resolver, aplicado ahora al propio mecanismo de
  solución. Con el reaper descentralizado, mientras quede un solo worker
  activo, la recuperación sigue funcionando.
- **Costo aceptado: polling redundante.** Con N workers ejecutando todos
  la misma consulta `SELECT ... WHERE status = 'running' AND lease_until
  < now()` cada 15 segundos, se realizan N menos uno consultas de más
  que, en el caso habitual, no encuentran nada (el índice parcial
  `idx_jobs_lease_recovery` mantiene esas consultas baratas). A la escala
  de este sistema, ese costo es despreciable frente a la simplicidad de
  no tener que elegir ni mantener un líder.
- Si el número de workers creciera considerablemente, del orden de
  cientos, este patrón empezaría a generar una carga de polling no
  despreciable sobre Postgres, y ahí sí valdría la pena reconsiderar un
  coordinador único con su propio lease. La Fase 5 terminó necesitando un
  mecanismo de liderazgo único para el scheduler de cron, pero resuelto
  de una forma distinta a este reaper: un advisory lock de sesión de
  PostgreSQL en lugar de un patrón descentralizado (ver ADR-006), porque
  ese caso sí requería que un único proceso actuara, a diferencia de este
  reaper, donde la seguridad ante ejecuciones concurrentes ya está dada
  por `SKIP LOCKED`. No es un problema que este proyecto haya necesitado
  resolver para el reaper en sí.
