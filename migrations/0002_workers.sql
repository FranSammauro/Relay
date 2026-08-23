-- Fase 2: registro de workers activos.
--
-- Ojo con esto: esta tabla NO es todavia el mecanismo de heartbeat/lease
-- (eso llega en Fase 4, con Redis en el medio). Por ahora es solo un
-- registro de "quien esta corriendo y con que concurrency arranco",
-- util para observabilidad y para el test de Fase 2. Si un worker muere
-- de mala manera su fila queda huerfana hasta Fase 4 -- es sabido y
-- esta bien, no rompe nada porque claim_next_job no depende de esta tabla.
CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,
    concurrency     INTEGER NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
