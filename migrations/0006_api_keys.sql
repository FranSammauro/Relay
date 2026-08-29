-- Fase 8: autenticación por API keys.
--
-- Cada key corresponde a un cliente de la API (producer, worker o admin).
-- En la tabla solo vive el prefijo (en claro, para identificar una key en
-- logs o listados sin exponerla) y el hash de la key completa (ver
-- ADR-007 para la decisión de hashing y por qué no Argon2/bcrypt, y por
-- qué la key completa nunca vuelve a poder leerse después de la creación).
CREATE TABLE IF NOT EXISTS api_keys (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    -- primer segmento de la key, en claro. Sirve para que un operador
    -- distinga "cuál key es" sin necesitar el secreto.
    key_prefix      TEXT NOT NULL UNIQUE,
    -- SHA-256 (hex) de la key completa. Con el UNIQUE de key_prefix ya
    -- existe un índice para el lookup por prefijo; no hace falta otro.
    key_hash        TEXT NOT NULL,
    -- 'producer' | 'worker' | 'admin' (ver matriz de roles en README).
    role            TEXT NOT NULL CHECK (role IN ('producer', 'worker', 'admin')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at      TIMESTAMPTZ,      -- NULL = activa
    last_used_at    TIMESTAMPTZ       -- mejor esfuerzo, ver touch_api_key
);

-- Listado/auditoría de keys activas (operación de admin): solo las que
-- todavía importan, en vez de recorrer toda la tabla.
CREATE INDEX IF NOT EXISTS idx_api_keys_active
    ON api_keys (created_at)
    WHERE revoked_at IS NULL;