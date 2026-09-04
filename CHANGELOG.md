# Registro de cambios

Este archivo documenta los cambios relevantes de cada versión de Relay.
El formato sigue, en líneas generales, la convención de
[Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/), adaptada a
un proyecto que se desarrolló en fases documentadas individualmente en
[`PHASES.md`](./PHASES.md).

## v1.0.0

Primera versión considerada completa del proyecto. Incluye persistencia
en PostgreSQL, coordinación efímera en Redis, recuperación automática
ante caída de workers, scheduling por cron con liderazgo distribuido,
observabilidad operativa, y autenticación con autorización por rol. El
desarrollo se organizó en ocho fases; a continuación se resume el
contenido de cada una.

### Fase 1: cola básica

Modelo de datos de un job en PostgreSQL, API HTTP para crear, consultar y
cancelar jobs, y un worker con un ciclo de ejecución que reclama trabajo,
lo ejecuta y confirma el resultado. El claim de jobs usa
`SELECT ... FOR UPDATE SKIP LOCKED` desde el primer día, lo cual permite
que múltiples workers compitan por trabajo sin bloquearse entre sí. Incluye
también la infraestructura de despliegue local con Docker Compose y logs
estructurados en formato JSON para los eventos relevantes del sistema.

### Fase 2: concurrencia

Cada worker ejecuta varios jobs en paralelo, con un límite configurable
mediante un semáforo. El entorno de desarrollo levanta varias réplicas de
worker por defecto, demostrando que el diseño de la Fase 1 soporta
concurrencia real sin cambios adicionales. Se agregó también un primer
mecanismo de integración continua que ejecuta la compilación y la suite
de tests en cada cambio.

### Fase 3: confiabilidad

Reintentos automáticos con backoff exponencial y variación aleatoria
calculados en SQL, cola de mensajes fallidos (dead-letter queue) para
jobs que agotan sus reintentos, timeouts configurables por job, y una
tabla de historial de intentos que registra cada ejecución de cada job de
forma individual, incluyendo su resultado y el mensaje de error cuando
corresponde.

### Fase 4: recuperación ante fallas distribuidas

Cada worker emite señales de vida periódicas hacia Redis, con expiración
automática si deja de emitirlas. Cada job reclamado recibe además un
lease con vencimiento en PostgreSQL, independiente del mecanismo de
heartbeat. Un proceso de recuperación, ejecutado de forma descentralizada
en todos los workers activos, detecta jobs cuyo lease venció sin haber
sido completados y los recupera aplicando la misma política de
reintentos que un fallo reportado normalmente. Este mecanismo se
verificó tanto con pruebas automatizadas como con una prueba manual real:
terminar un worker de forma abrupta a mitad de la ejecución de un job y
confirmar que otro worker lo recupera y lo completa.

### Fase 5: scheduling

Soporte para jobs programados a futuro y para jobs recurrentes definidos
mediante expresiones cron, con un parser de expresiones cron escrito
específicamente para este proyecto. El disparo de los jobs recurrentes
está coordinado mediante un mecanismo de liderazgo basado en un advisory
lock de PostgreSQL, de modo que un único worker sea responsable de
disparar cada schedule en cada momento, sin necesidad de un componente
coordinador separado.

### Fase 6: funcionalidad operativa

Endpoint de métricas en formato compatible con Prometheus, calculadas en
el momento de la consulta directamente desde PostgreSQL, sin contadores
mantenidos en memoria de proceso. Un panel de control web con
actualización en vivo para observar el estado del sistema. Una
herramienta de línea de comandos para operar el sistema sin pasar por la
API HTTP. Manejo ordenado de la señal de apagado del sistema operativo,
de modo que un worker en proceso de apagarse termine los jobs que tiene
en curso antes de finalizar, en lugar de interrumpirlos.

### Fase 7: rendimiento

Herramienta de benchmarking integrada en la línea de comandos, capaz de
medir la latencia de envío, de espera en cola y de ejecución de un lote
de jobs con datos reales. Se documentó un informe de rendimiento
reproducible, que incluye un hallazgo concreto sobre el comportamiento
de los índices de PostgreSQL bajo una cola de trabajo sostenida, con su
análisis y las alternativas consideradas para resolverlo.

### Fase 8: preparación para producción

Autenticación mediante API keys, con hash de las credenciales y
verificación en tiempo constante para evitar filtrar información por
temporización. Autorización por rol, resuelta contra una tabla explícita
de permisos por endpoint. Límite de tasa de solicitudes por API key,
implementado sobre Redis con una política de apertura ante la falta de
disponibilidad de ese componente, para no convertir una dependencia
secundaria en un punto único de falla de la API. Un flujo de integración
continua extendido, que construye y publica las imágenes de contenedor
del proyecto ante cada nueva etiqueta de versión.

## Notas sobre versiones anteriores a v1.0.0

El proyecto se desarrolló íntegramente antes de esta primera versión
etiquetada; no existieron versiones publicadas previas. El historial
completo de decisiones de diseño, con su contexto y las alternativas
consideradas en cada caso, está documentado en
[`docs/adr/`](./docs/adr).
