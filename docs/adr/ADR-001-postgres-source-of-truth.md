# ADR-001: PostgreSQL como fuente de verdad

## Estado
Aceptado (Fase 1)

## Contexto
El sistema necesita persistir el estado de cada job de forma durable, de
manera que ni un reinicio de la API, ni la caída de un worker, ni la
eventual introducción de Redis para coordinación efímera puedan hacer
perder información crítica (payload, estado, intentos, errores).

## Decisión
PostgreSQL es la única fuente de verdad para el estado persistente de los
jobs. Cualquier componente de coordinación efímera (Redis, a partir de
Fase 4) solo maneja información que, si se pierde, no compromete la
correctitud del sistema — únicamente su performance o velocidad de
coordinación.

Regla general: *si perder Redis implica perder información crítica sobre
un job, esa información debe persistirse en PostgreSQL.*

## Consecuencias
- El claim de jobs (`FOR UPDATE SKIP LOCKED`) recae sobre PostgreSQL, lo que
  acopla el throughput de claim a la capacidad de la base de datos. Esto se
  mide explícitamente en Fase 7 (benchmarks) para encontrar el techo real.
- Simplifica el modelo mental: no hay que reconciliar dos fuentes de verdad
  divergentes para el estado de un job.
- Redis se introduce más adelante exclusivamente para lo que es
  naturalmente efímero: heartbeats rápidos, rate limiting, leases de
  scheduler.
