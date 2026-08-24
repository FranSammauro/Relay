-- Fase 1: modelo central de jobs.
-- PostgreSQL es la fuente de verdad para el estado persistente (ver docs/adr/ADR-001).

CREATE TABLE IF NOT EXISTS jobs (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type           TEXT NOT NULL,
    payload            JSONB NOT NULL DEFAULT '{}'::jsonb,
    status             TEXT NOT NULL DEFAULT 'pending',
    priority           INTEGER NOT NULL DEFAULT 50,
    attempts           INTEGER NOT NULL DEFAULT 0,
    max_attempts       INTEGER NOT NULL DEFAULT 5,

    scheduled_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at         TIMESTAMPTZ,
    completed_at       TIMESTAMPTZ,
    failed_at          TIMESTAMPTZ,

    -- Campos que se usarán a partir de Fase 4 (leases / recovery), ya modelados
    -- desde ahora para no tener que migrar el esquema más adelante.
    worker_id          TEXT,
    lease_until        TIMESTAMPTZ,

    idempotency_key    TEXT UNIQUE,
    last_error         TEXT,

    CONSTRAINT jobs_status_check CHECK (
        status IN ('pending', 'running', 'completed', 'failed', 'retry_scheduled', 'dead_letter', 'cancelled')
    )
);

-- Índice pensado para el patrón de claim de la Fase 2:
-- SELECT ... WHERE status = 'pending' AND scheduled_at <= now()
-- ORDER BY priority DESC, created_at ASC FOR UPDATE SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_jobs_claim
    ON jobs (status, scheduled_at, priority DESC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (job_type);
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency ON jobs (idempotency_key);
