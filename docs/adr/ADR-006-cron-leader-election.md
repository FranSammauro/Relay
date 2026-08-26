# ADR-006: Leader election del scheduler de cron vía advisory lock de Postgres

## Estado
Aceptado (Fase 5)

## Contexto
El disparo de cron schedules (`Storage::fire_cron_schedule`) no es
idempotente-seguro de la misma forma que el reaper de Fase 4. El reaper
puede correr en todos los workers a la vez sin coordinación porque cada
job abandonado se procesa dentro de una transacción con
`FOR UPDATE SKIP LOCKED` -- dos reapers compitiendo por el mismo job
simplemente no se pisan, uno gana y el otro sigue de largo.

Con cron no alcanza ese patrón: si diez workers escanean
`cron_schedules` al mismo tiempo y encuentran un schedule vencido, los
diez van a intentar dispararlo. El `idempotency_key` derivado
(`cron:{schedule_id}:{next_run_at}`, ver `fire_cron_schedule`) evita que
eso termine en jobs duplicados -- pero significa que nueve de cada diez
intentos de disparo son trabajo desperdiciado, y a mayor escala (más
schedules, más workers) ese desperdicio crece sin aportar nada. Hace falta
que **un solo proceso a la vez** sea responsable de escanear y disparar.

## Decisión
Un único advisory lock de sesión de PostgreSQL (`pg_try_advisory_lock`)
funciona como el lock de liderazgo. Cada worker, en su propia tarea de
fondo, intenta tomarlo; el que lo consigue corre el loop de "buscar
vencidos y dispararlos"; el resto reintenta cada `SCHEDULER_INTERVAL_MS`
por si el líder actual desaparece.

Se evaluó explícitamente usar Redis para esto (ya está en el sistema desde
Fase 4) con un patrón tipo Redlock. Se descartó por dos razones:

1. **Menos piezas móviles.** Un advisory lock de sesión se libera solo
   cuando la conexión que lo sostiene muere -- sea porque el proceso se
   cayó, perdió la red, o lo cerró a propósito. No hace falta implementar
   TTL, renovación periódica, ni lógica de "¿cuánto tiempo sin
   renovar cuenta como caído?" -- eso ya lo maneja Postgres.
2. **Consistente con ADR-001 y ADR-002.** La regla general del proyecto es
   que todo lo que afecta la correctitud del sistema vive en Postgres;
   Redis es coordinación efímera de bajo riesgo (heartbeats). Decidir
   quién puede escribir en `jobs` es una decisión de correctitud, no de
   observabilidad -- entra del lado de Postgres.

## Consecuencias
- **Sin TTL que ajustar.** A diferencia del lease de jobs (ADR-003, fijo
  con margen porque no se renueva) o del heartbeat de worker (ADR-002, con
  TTL explícito), acá no hay una ventana de "por las dudas": el lock vive
  exactamente tanto como la conexión TCP que lo sostiene. Un límite: la
  detección de caída depende de que el sistema operativo o Postgres noten
  la conexión muerta (keepalive de TCP), lo cual puede tardar más que un
  heartbeat con TTL corto en casos de partición de red silenciosa (la
  conexión "parece" viva pero no lo está). Para el volumen de cron jobs de
  este proyecto, ese margen es aceptable; si hiciera falta apretarlo,
  Postgres soporta configurar `tcp_keepalives_idle` más agresivo.
- **Una conexión "cautiva" por todo el tiempo que dure el liderazgo.** El
  pool de conexiones (`max_connections(10)`) pierde una conexión mientras
  haya un líder activo -- aceptable a esta escala, pero es lo primero a
  revisar si el pool empezara a quedarse corto.
- **Importante para quien toque este código:** liberar el lock requiere
  cerrar la conexión de verdad (`PoolConnection::close().await`), no solo
  soltar el handle de Rust (`drop`). Un pool de conexiones reutiliza la
  sesión TCP subyacente en vez de cerrarla al hacer `drop` -- y como el
  advisory lock es de sesión, sigue vivo mientras esa sesión exista, sin
  importar que el wrapper de Rust ya no tenga referencias. Esto está
  probado explícitamente en
  `crates/common/tests/scheduling.rs::only_one_connection_can_hold_the_scheduler_leadership`,
  que en su primera versión fallaba por esto exacto.
