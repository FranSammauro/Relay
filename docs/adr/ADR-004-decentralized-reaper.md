# ADR-004: Reaper descentralizado (cada worker recupera, no hay coordinador)

## Estado
Aceptado (Fase 4)

## Contexto
Alguien tiene que revisar periódicamente si hay jobs con el lease vencido
y recuperarlos (ver ADR-003). Las opciones típicas son: (a) un proceso
coordinador dedicado que hace esto y nada más, o (b) que cada worker lo
haga por su cuenta, todos corriendo el mismo chequeo en paralelo.

## Decisión
Cada worker corre su propio reaper en una tarea de fondo (`REAPER_INTERVAL_MS`,
default 15s), sin coordinación entre ellos más allá de lo que ya da
Postgres. No existe un rol de "coordinador" separado.

La seguridad ante múltiples reapers corriendo a la vez la da
`SELECT ... FOR UPDATE SKIP LOCKED` dentro de la misma transacción que
hace la transición de estado (`Storage::reap_expired_leases`): si dos
workers agarran el lease vencido del mismo job al mismo tiempo, uno se
queda con la fila bloqueada hasta terminar de transicionarla, el otro
simplemente no la ve (`SKIP LOCKED`) y sigue de largo. Nunca hay doble
recovery del mismo job.

## Consecuencias
- **Sin punto único de falla para el recovery.** Un coordinador dedicado
  que se cae deja de recuperar jobs abandonados hasta que alguien lo
  reinicie — literalmente el mismo problema que se está tratando de
  resolver, aplicado al propio mecanismo de solución. Con el reaper
  descentralizado, mientras quede un solo worker vivo, la recuperación
  sigue funcionando.
- **Costo aceptado: polling redundante.** Con N workers todos corriendo el
  mismo `SELECT ... WHERE status = 'running' AND lease_until < now()` cada
  15s, hay N-1 queries "de más" que en el caso común no encuentran nada
  (índice parcial `idx_jobs_lease_recovery` para que esas queries sean
  baratas). A la escala de este sistema es un costo despreciable frente a
  la simplicidad de no tener que elegir ni mantener un líder.
- Si el número de workers creciera mucho (cientos), este patrón empezaría
  a generar carga de polling no despreciable en Postgres — ahí sí valdría
  la pena reconsiderar un coordinador único con lease propio (el mismo
  patrón de leader election que Fase 5 va a necesitar para el scheduler
  distribuido). No es el problema de este proyecto todavía.
