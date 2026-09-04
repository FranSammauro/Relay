# ADR-003: Leases de job fijos al claim, no renovables (MVP)

## Estado
Aceptado (Fase 4)

## Contexto
`timeout_seconds`, introducido en la Fase 3, finaliza un job que se
cuelga mientras el proceso del worker sigue vivo: es
`tokio::time::timeout` aplicado alrededor del handler. Si el worker
entero desaparece, por ejemplo mediante `kill -9`, quedarse sin memoria,
o la caída del nodo que lo hospeda, no hay ningún timeout dentro del
proceso que se pueda disparar: el job queda en estado `running` de forma
indefinida, sin que nadie lo finalice ni lo reporte como fallido.

Se necesita una segunda señal, externa al proceso que ejecuta el job, que
permita establecer que un job debería haber terminado a cierta hora, y
que si no ocurrió nada para entonces, corresponde asumir que el worker
murió.

## Decisión
Cada vez que `claim_next_job` asigna un job a un worker, fija
`lease_until = now() + (timeout_seconds + LEASE_GRACE_SECONDS)`, con
`LEASE_GRACE_SECONDS` fijo en 30 segundos, sin ser configurable por el
momento. El lease se fija una única vez, en el momento del claim, y no se
renueva mientras el job se ejecuta.

Esta es una decisión de alcance explícita, no un descuido. Un lease
renovable necesitaría que cada job en ejecución emitiera su propio
heartbeat periódico, es decir, un heartbeat por job en lugar de por
worker, lo cual constituye una pieza de infraestructura completa aparte.
Para el tipo de jobs de este sistema, acotados por `timeout_seconds`, que
ya de por sí funciona como un techo razonable, un lease fijo con margen
resulta suficiente. Si en el futuro fuera necesario soportar jobs de
duración muy variable o desconocida de antemano, un lease renovable sería
el paso siguiente; queda documentado acá como una extensión disponible,
no como algo implementado antes de tiempo.

El reaper (ADR-004) es quien actúa sobre un lease vencido: recupera el
job aplicando la misma política de retry, backoff y dead-letter que un
fallo reportado normalmente, mediante la función compartida
`Storage::transition_after_failure`.

## Consecuencias
- **Entrega al menos una vez, no exactamente una vez.** Si un worker
  termina un job con éxito pero muere antes de que el `UPDATE ... SET
  status = 'completed'` llegue a confirmarse, el lease eventualmente
  vence y otro worker vuelve a ejecutar el mismo job. Los handlers
  deberían ser idempotentes siempre que sea posible; esto ya estaba
  anticipado en el README desde la Fase 1.
- El costo de un worker caído no es instantáneo: el job abandonado queda
  en un estado intermedio, con status `running` pero sin un dueño real,
  hasta por `timeout_seconds` más treinta segundos. Este margen es el
  que hay que tener en cuenta al definir cualquier acuerdo de nivel de
  servicio sobre el tiempo máximo de procesamiento de un job.
- `timeout_seconds` sigue siendo la primera línea de defensa, rápida y
  dentro del propio proceso; el lease es la red de seguridad para
  cuando esa primera línea ni siquiera llega a ejecutarse.
