-- Fase 5: scheduling. cron_schedules guarda "plantillas" de job que se
-- disparan solas según una expresión cron -- cada disparo crea una fila
-- normal en `jobs` (no hay un tipo de job especial ni un camino de
-- ejecución distinto; un job creado por cron es indistinguible de uno
-- creado por la API una vez que existe).
CREATE TABLE IF NOT EXISTS cron_schedules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE,
    cron_expr       TEXT NOT NULL,

    -- plantilla del job que se crea en cada disparo.
    job_type        TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority        INTEGER NOT NULL DEFAULT 50,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    timeout_seconds INTEGER NOT NULL DEFAULT 30,

    enabled         BOOLEAN NOT NULL DEFAULT true,
    next_run_at     TIMESTAMPTZ NOT NULL,
    last_run_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- la query del scheduler es "qué está habilitado y ya debería haber
-- corrido" -- índice parcial, mismo criterio que idx_jobs_lease_recovery
-- en Fase 4.
CREATE INDEX IF NOT EXISTS idx_cron_schedules_due
    ON cron_schedules (next_run_at)
    WHERE enabled = true;
