# ADR-007: Autenticación por API Keys

## Estado
Aceptado (Fase 8)

## Contexto
La API expone endpoints de operación (enviar jobs, consultar estado, gestionar
cron) y necesita autenticar a los callers. No hay autenticación por usuario
final — los clientes son servicios/maquinas que envían trabajo. Requisitos:

- Credenciales rotables y revocables sin redeploy.
- La key completa se muestra **una sola vez** en creación (secreto no
  recuperable en BD).
- Autorización por rol simple: `producer` (envía jobs), `worker` (solo
  lectura/monitoreo), `admin` (todo, incluyendo cron).
- Sin dependencias criptográficas pesadas (Argon2, bcrypt): la key tiene
  256 bits de entropía, un hash rápido (SHA-256) es suficiente.
- Bootstrap del primer admin sin "chicken-and-egg": gestión vía CLI directa
  contra Postgres (`queue-cli api-key create --role admin`), no HTTP.

## Decisión
1. **Formato de key**: `dq_<prefijo_8_chars>_<secreto_32_bytes_base64url>`
   - `dq_` marker fijo para identificación visual.
   - Prefijo (8 chars base64url) almacenado en claro → lookup O(1) sin
     hashear la key completa.
   - Secreto (32 bytes = 256 bits) → entropía completa, no hay necesidad de
     KDF lento.
2. **Almacenamiento**: tabla `api_keys` con columnas:
   - `key_prefix` TEXT UNIQUE (índice implícito).
   - `key_hash` TEXT = SHA-256 hex de la key **completa** (incluye `dq_...`).
   - `role` TEXT CHECK ('producer','worker','admin').
   - `revoked_at` TIMESTAMPTZ NULL (soft delete).
   - Índice parcial `idx_api_keys_active` (created_at WHERE revoked_at IS NULL).
3. **Verificación**:
   - Parsear prefijo de la key entrante → `find_api_key_by_prefix`.
   - Si no existe o `revoked_at` IS NOT NULL → 401.
   - `verify_api_key(key_entrante, stored_hash)`: comparación en tiempo
     constante (hex decode + constant-time compare).
   - OK → `AuthContext { key_id, role }` en request extensions.
4. **Capas en axum (orden, outermost → innermost)**:
   - `auth_middleware`: resuelve `AuthContext` y cachea en extensions.
   - `authorize_middleware`: consulta tabla de grants (método + ruta →
     roles permitidos) contra `AuthContext.role`.
   - `rate_limit_middleware`: sliding window counter por `key_id` + rol.
4. **Matriz de permisos (grants)**: ver `crates/api/src/auth.rs:GRANT_TABLE`.
   - Lectura (GET /jobs, /jobs/:id, /stats, /metrics, /workers): todos.
   - Escritura (POST /jobs, DELETE /jobs/:id): producer + admin.
   - Cron (GET/POST /cron, GET/DELETE /cron/:id): solo admin.
   - Público: `/health`, `/ready`, `/` (dashboard HTML).
5. **Gestión de keys**: solo por CLI (`queue-cli api-key create|list|revoke`).
   - Sin endpoints HTTP `/admin/keys` (evita bootstrap problem + coincide
     con patrón CLI→Postgres directo de este proyecto).

## Alternativas consideradas
- **JWT**: añade complejidad (firma, expiración, JWKS, revocación via
  blacklist o short TTL + refresh). API keys opacas son más simples para
  machine-to-machine.
- **HMAC-SHA256 en lugar de SHA-256 plano**: añade secreto de servidor que
  hay que rotar/guardar. SHA-256 directo sobre key de 256 bits es seguro;
  el hash no permite recuperar la key y el prefijo no revela el secreto.
- **Argon2/bcrypt**: justificado para contraseñas elegidas por humanos
  (baja entropía). Aquí el secreto tiene 256 bits aleatorios — KDF lento
  solo añade latencia sin ganancia de seguridad.
- **Guardas de rol por sub-router con `route_layer`**: descarta silenciosamente
  los middlewares del segundo router al mergear rutas compartidas (GET/POST
  en `/jobs`). Se usa una tabla de grants centralizada en un solo middleware
  `authorize_middleware` (ver `crates/api/src/auth.rs`).

## Consecuencias
- `last_used_at` se actualiza "best effort" vía `tokio::spawn` tras
  verificación exitosa (throttle en SQL a 1 escritura/min).
- La key completa **no** se puede recuperar tras la creación. Documentado
  en CLI output y ADR.
- Rate limiting (ADR-008) corre por dentro de auth, usando `key_id`.
- Sin sesión/cookie: stateless, apto para k8s/serverless.