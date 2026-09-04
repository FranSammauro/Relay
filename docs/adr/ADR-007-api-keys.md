# ADR-007: Autenticación mediante API keys

## Estado
Aceptado (Fase 8)

## Contexto
La API expone endpoints de operación (envío de jobs, consulta de estado,
gestión de cron schedules) que hasta la Fase 7 podían invocarse sin
ningún tipo de autenticación. Antes de considerar el proyecto listo para
un entorno de producción, resulta necesario identificar quién realiza
cada solicitud y restringir qué puede hacer cada tipo de cliente.

Los clientes de este sistema son servicios y procesos automatizados, no
usuarios finales con sesiones interactivas. No existe, por lo tanto, la
necesidad de manejar flujos de inicio de sesión, contraseñas elegidas por
personas, ni renovación de sesiones mediante cookies. El requisito real es
más simple: credenciales que se puedan emitir, rotar y revocar sin
necesidad de un nuevo despliegue, junto con una distinción clara entre
tres roles de acceso.

## Decisión

### Formato de la key y almacenamiento

Cada API key tiene la forma `dq_<prefijo><secreto>`, donde el prefijo son
ocho caracteres alfanuméricos y el secreto son treinta y dos bytes
aleatorios codificados en base64url. El marcador `dq_` al inicio permite
reconocer visualmente el formato de la key en logs o documentación sin
ambigüedad.

La tabla `api_keys` almacena, por cada key emitida: el prefijo en texto
plano (que sirve como clave de búsqueda directa, sin necesidad de calcular
ningún hash para localizar la fila candidata), el hash SHA-256 de la key
completa, el rol asociado, la fecha de creación, la fecha de revocación
cuando corresponde, y la fecha de último uso. La key completa nunca se
almacena ni se puede reconstruir a partir de lo guardado en la base: se
muestra una única vez, en el momento de la creación, y a partir de ahí
solo existe en poder de quien la recibió.

### Por qué SHA-256 y no Argon2 o bcrypt

Argon2 y bcrypt son funciones de derivación de clave diseñadas para
contraseñas elegidas por personas, que típicamente tienen baja entropía.
El costo computacional deliberado de esas funciones existe para hacer
impracticable un ataque de fuerza bruta contra ese espacio de contraseñas
reducido. Una API key generada aleatoriamente con doscientos cincuenta y
seis bits de entropía no comparte ese problema: un atacante que obtuviera
la tabla completa de hashes no tendría forma práctica de recuperar ninguna
key a partir de ellos, incluso con un hash rápido, porque el espacio de
búsqueda es astronómicamente mayor que el de cualquier diccionario de
contraseñas humanas. Introducir un costo computacional adicional en cada
verificación de autenticación no mejora la seguridad en este escenario y
sí introduce latencia innecesaria en cada solicitud autenticada.

### Verificación y capas de la API

La verificación de una key entrante extrae el prefijo, busca la fila
correspondiente en `api_keys`, y compara el hash de la key completa contra
el hash almacenado mediante una comparación en tiempo constante, para no
filtrar información sobre en qué posición difieren dos hashes a través
del tiempo de respuesta. Tanto una key inexistente como una key revocada
como una key cuyo hash no coincide producen la misma respuesta, un 401,
sin distinción visible desde afuera: quien intenta autenticarse no debe
poder inferir cuál de esos tres casos ocurrió.

Las capas de la API se componen en un orden deliberado: seguimiento de
solicitudes, CORS, autenticación, autorización, límite de tasa, y
finalmente el handler correspondiente. La autenticación se ubica antes
que las demás capas relacionadas con la identidad del cliente porque no
tiene sentido gastar una consulta a Redis para el límite de tasa, ni
evaluar reglas de autorización, sobre una solicitud que ni siquiera trae
una identidad válida. El resultado de la autenticación se guarda en las
extensiones del request para que las capas siguientes lo reutilicen sin
repetir la búsqueda contra la base de datos.

### Autorización por rol

Existen tres roles: `producer`, que puede enviar jobs y consultar su
estado; `worker`, que solo tiene acceso de lectura y observación, pensado
para monitoreo interno; y `admin`, que además puede gestionar cron
schedules y tiene acceso completo al sistema. La decisión de autorización
se resuelve contra una tabla explícita de permisos, indexada por método
HTTP y ruta, que enumera qué roles pueden acceder a cada endpoint.

Esta tabla centralizada fue una decisión deliberada frente a la
alternativa más obvia de aplicar guardas de rol por subrouter, agrupando
las rutas de cada rol bajo su propio router con su propio middleware. Al
probar ese enfoque se encontró un problema concreto en axum 0.7: cuando
dos routers que comparten una misma ruta se combinan (por ejemplo, GET y
POST sobre `/jobs`, típicamente definidos en routers separados según el
rol que los puede usar), axum conserva los middlewares del primer router
registrado para esa ruta y descarta los del segundo. El resultado es que
una guarda de rol puede desaparecer silenciosamente para una ruta
compartida, sin ningún error de compilación ni advertencia en tiempo de
ejecución que lo delate. Una tabla de grants centralizada, evaluada por
un único middleware, evita esa clase de error por completo: cada
combinación de método y ruta tiene un único lugar donde se define quién
puede acceder, y ese lugar es fácil de recorrer con un test que verifique
la tabla completa contra el conjunto de rutas reales de la aplicación.

### Gestión de keys

La creación, listado y revocación de keys se realiza exclusivamente
mediante relay-cli, que habla directo con PostgreSQL, y no a través de un
endpoint HTTP. Esta decisión resuelve un problema de arranque: si la
única forma de crear una API key fuera a través de la propia API, no
existiría una forma de crear la primera key sin ya tener una. Delegar la
gestión de keys a la CLI, que no requiere autenticación HTTP porque opera
directamente contra la base de datos, es coherente además con el patrón
ya establecido en el resto del proyecto, donde la CLI existe precisamente
para operar el sistema sin depender de que la API esté disponible.

## Alternativas descartadas

Se consideró JWT como mecanismo de autenticación. Se descartó porque
introduce complejidad que no tiene contrapartida de valor para este caso
de uso: manejo de claves de firma, expiración de tokens, y un mecanismo de
revocación que, dado que los JWT son por diseño verificables sin consultar
un almacén central, típicamente requiere una lista de revocación aparte o
tiempos de vida muy cortos combinados con tokens de refresco. Una API key
opaca, verificada contra una tabla que se puede actualizar en cualquier
momento, resuelve la revocación de forma directa y es más simple de
razonar para clientes máquina a máquina que no necesitan sesiones de corta
duración.

También se evaluó agregar un secreto de servidor mediante HMAC-SHA256 en
lugar de un hash SHA-256 directo sobre la key completa. Un secreto de
servidor agrega una capa de protección adicional si la tabla api_keys se
filtrara sin que el atacante tuviera también ese secreto, pero introduce a
su vez la necesidad de generar, almacenar y eventualmente rotar ese
secreto de forma segura. Dado que la key ya tiene doscientos cincuenta y
seis bits de entropía propios, y que el hash no permite reconstruir la key
original en ningún caso, se consideró que el costo operativo adicional no
se justificaba para el nivel de riesgo actual del proyecto. Queda anotado
como una mejora disponible si en el futuro el modelo de amenazas lo
requiere.

## Consecuencias

La fecha de último uso de cada key se actualiza de forma asíncrona tras
una verificación exitosa, sin bloquear la respuesta al cliente por esa
escritura. La key completa, una vez creada, no se puede recuperar bajo
ninguna circunstancia; esto está documentado tanto en la salida de la CLI
al momento de crear una key como en este documento, para que quede claro
que perder la key implica revocarla y emitir una nueva. El límite de tasa
(ADR-008) se apoya en la identidad resuelta por esta capa de
autenticación, aplicando límites distintos según el rol de la key. El
esquema es completamente sin estado del lado del servidor más allá de la
tabla api_keys: no hay sesiones ni cookies que mantener, lo cual lo hace
apto para cualquier forma de despliegue horizontal sin configuración
adicional.
