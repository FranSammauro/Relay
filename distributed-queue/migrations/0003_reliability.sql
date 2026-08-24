-- Fase 3: reliability. Dos cosas nuevas:
--
-- 1) timeout_seconds en jobs: cuanto puede correr un job antes de que el
--    worker lo mate y lo cuente como fallo. 30s de default es arbitrario
--    pero razonable para el tipo de handlers de ejemplo que tenemos; el
--    caller puede pisarlo por job.
--
-- 2) job_attempts: historial de cada intento de ejecución. No reemplaza a
--    `jobs` (que sigue siendo la fuente de verdad del estado actual), es
--    el log de "qué pasó cada vez que se intentó". Sin esto, después de
--    3 reintentos fallidos solo te queda el último error en jobs.last_error
--    y listo -- con esta tabla te queda la película completa.
ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS timeout_seconds INTEGER NOT NULL DEFAULT 30;

CREATE TABLE IF NOT EXISTS job_attempts (
    id              BIGSERIAL PRIMARY KEY,
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number  INTEGER NOT NULL,
    worker_id       TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    -- 'completed' | 'failed' | 'timeout'. NULL mientras el intento sigue
    -- abierto (worker todavía corriendo el job).
    status          TEXT,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id ON job_attempts (job_id);
