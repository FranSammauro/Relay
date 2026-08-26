-- Fase 4: índice para la query del reaper (recuperación de jobs abandonados).
--
-- WHERE status = 'running' AND lease_until < now() es un patrón distinto al
-- de idx_jobs_claim (que arranca por status pero está pensado para
-- pending/retry_scheduled). Como 'running' es un subconjunto chico y
-- transitorio de la tabla, un índice parcial es más barato que uno
-- genérico -- solo indexa las filas que en algún momento importan para
-- esta query.
CREATE INDEX IF NOT EXISTS idx_jobs_lease_recovery
    ON jobs (lease_until)
    WHERE status = 'running';
