# ADR-008: Límite de tasa por API key mediante ventanas fijas en Redis

## Estado
Aceptado (Fase 8)

## Contexto
Con la introducción de autenticación en la Fase 8 (ADR-007), la API queda
en condiciones de identificar quién realiza cada solicitud, lo que abre la
posibilidad de limitar cuántas solicitudes puede hacer cada cliente en un
período de tiempo dado. El objetivo es proteger al sistema de un cliente
que, por error o por un bucle de reintentos mal configurado, envíe una
cantidad de solicitudes muy superior a lo esperado, sin necesidad de que
un operador humano intervenga manualmente.

## Decisión

### Algoritmo

Se implementó un contador de ventana fija de sesenta segundos, en lugar de
un token bucket u otro algoritmo más elaborado. Cada combinación de key y
ventana temporal corresponde a una clave en Redis, calculada como el
identificador de la key seguido del número de ventana actual, entendido
como el resultado de dividir el timestamp Unix presente por sesenta.
Cada solicitud incrementa esa clave de forma atómica; si el valor
devuelto por el incremento es uno, es decir, si esta es la primera
solicitud de esa ventana, se le asigna una expiración de sesenta segundos,
de modo que el contador desaparece solo cuando la ventana termina, sin
necesidad de un proceso de limpieza aparte.

Un token bucket ofrece un comportamiento más uniforme en los bordes de la
ventana, evitando el caso en que una ráfaga de solicitudes justo al final
de una ventana y otra ráfaga justo al comienzo de la siguiente sumen, en
un margen de segundos, casi el doble del límite nominal. Se evaluó esa
alternativa y se descartó porque requiere mantener un estado más rico por
cliente, típicamente el timestamp de la última solicitud junto con la
cantidad de tokens disponibles en ese momento, en lugar de un único
contador con expiración automática. Para el propósito de este sistema,
que es evitar abusos claros más que ofrecer una garantía de tasa exacta,
la imprecisión en los bordes de ventana de un contador fijo es aceptable a
cambio de una implementación considerablemente más simple.

### Límites por rol

El límite se configura de forma independiente para cada rol mediante
variables de entorno, con un valor por defecto de trescientas solicitudes
por minuto para los roles `producer` y `worker`. El rol `admin`, pensado
para operación interna y mantenimiento, no tiene límite por defecto: sus
solicitudes se dejan pasar sin siquiera consultar Redis para ese caso, ya
que limitar las tareas de administración del propio sistema no protege a
nadie y sí puede entorpecer una operación de emergencia.

### Comportamiento ante la falta de disponibilidad de Redis

Si Redis no está disponible en el momento de evaluar el límite, la
decisión es dejar pasar la solicitud y registrar una advertencia en los
logs, en lugar de rechazarla. Esta decisión, denominada habitualmente
fail-open, es consistente con el rol que Redis cumple en el resto del
sistema desde la Fase 4 (ADR-002): coordinación efímera cuya ausencia
degrada observabilidad o, en este caso, protección contra abuso, pero
nunca debe convertirse en un motivo de indisponibilidad de la API. Se
consideró la alternativa de fallar cerrado, rechazando toda solicitud
mientras Redis no esté disponible, y se descartó porque convierte una
dependencia secundaria, pensada para ser prescindible, en un punto único
de falla para toda la API. El costo real de la decisión tomada es que,
durante una caída de Redis, un cliente sin límite podría generar una carga
alta sobre el sistema; ese riesgo se considera preferible a una caída
completa de la API por la indisponibilidad de un componente que no forma
parte de su camino crítico de correctitud.

### Integración con la API

El middleware de límite de tasa se ejecuta después del middleware de
autenticación, ya que necesita conocer la identidad y el rol del cliente
para decidir qué límite aplicar; no tendría sentido evaluarlo antes,
porque no habría todavía ninguna key contra la cual contar. Las rutas
públicas, que no requieren autenticación, no pasan por ningún conteo:
health, readiness y el dashboard quedan completamente al margen del
límite de tasa. Cuando una solicitud excede el límite vigente, la
respuesta es un código 429 que incluye un encabezado Retry-After con la
cantidad de segundos que restan hasta el fin de la ventana actual, de
modo que un cliente bien comportado pueda esperar exactamente ese tiempo
antes de reintentar en lugar de adivinar cuánto esperar.

## Alternativas descartadas

Se consideró mantener el contador en memoria de cada instancia de la API,
sin depender de Redis. Esta alternativa se descartó porque el sistema está
diseñado para correr con varias instancias de la API en paralelo; un
contador que solo viviera en el proceso de una instancia permitiría a un
cliente esquivar el límite real simplemente distribuyendo sus solicitudes
entre distintas instancias, lo cual anula el propósito del mecanismo.
Centralizar el contador en Redis es lo que permite que el límite sea
efectivamente compartido entre todas las instancias de la API que estén
corriendo en un momento dado.

También se consideró implementar el conteo directamente en PostgreSQL, en
lugar de en Redis. Se descartó porque agregaría una carga de escritura
significativa y de alta frecuencia sobre la base de datos que constituye
la fuente de verdad del sistema (ADR-001), para resolver un problema que
por naturaleza es efímero: lo único que importa de un contador de límite
de tasa es su valor durante la ventana de tiempo actual, y ese es
precisamente el tipo de dato para el que Redis ya está presente en la
arquitectura desde la Fase 4.

## Consecuencias

El límite por defecto de trescientas solicitudes por minuto equivale a
cinco solicitudes por segundo en promedio, un margen razonable para un
cliente automatizado que envía trabajo de forma programática, y ajustable
sin necesidad de un nuevo despliegue simplemente cambiando la variable de
entorno correspondiente. Los tests de integración que verifican este
comportamiento limpian los contadores de Redis entre corridas, para que
una ejecución no deje estado que afecte a la siguiente. El valor de
segundos hasta el próximo intento, expuesto en el encabezado Retry-After y
también en el cuerpo de la respuesta 429, le da a cualquier cliente la
información necesaria para implementar un reintento con espera adecuada
en lugar de reintentar de inmediato y agravar la situación.
