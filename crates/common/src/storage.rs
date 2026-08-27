use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::QueueError;
use crate::model::{
    AttemptOutcome, CronSchedule, Job, JobAttempt, JobDurationStats, JobRow, NewCronSchedule,
    NewJob, StatusCount, WorkerInfo,
};

/// Columnas de `jobs` compartidas por casi todas las queries de este módulo.
/// Un solo lugar para tocar si el día de mañana se agrega una columna --
/// ya nos pasó de olvidarnos una en un SELECT y perder una tarde con eso.
const JOB_COLUMNS: &str = r#"id, job_type, payload, status, priority, attempts, max_attempts,
                      scheduled_at, created_at, started_at, completed_at, failed_at,
                      worker_id, lease_until, timeout_seconds, idempotency_key, last_error"#;

/// Cuánto margen le damos al lease de un job por encima de su propio
/// `timeout_seconds`. El timeout ya mata el job in-process si se cuelga;
/// el lease existe para el caso más feo, que el proceso entero del worker
/// desaparezca (kill -9, OOM, nodo caído) antes de que el timeout llegue a
/// dispararse. 30s de margen cubre jitter de scheduling y GC pauses sin
/// hacer que un job legítimamente lento parezca abandonado.
///
/// Nota de scope: el lease se fija una sola vez al hacer claim, no se
/// renueva mientras el job corre. Para jobs individuales esto está bien
/// porque timeout_seconds + este margen ya define un techo razonable. Un
/// lease renovable (heartbeat por-job en vez de por-worker) es la
/// evolución natural si algún día hay jobs de duración muy variable, pero
/// es la típica funcionalidad que se anota en el roadmap en vez de meterla
/// a presión en el MVP.
const LEASE_GRACE_SECONDS: i32 = 30;

/// Storage es la única puerta de entrada a PostgreSQL, que actúa como fuente
/// de verdad para el estado persistente del sistema (ADR-001).
#[derive(Clone)]
pub struct Storage {
    pool: PgPool,
}

impl Storage {
    pub async fn connect(database_url: &str) -> Result<Self, QueueError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), QueueError> {
        sqlx::migrate!("../../migrations")
            .run(&self.pool)
            .await
            .map_err(|e| QueueError::Database(sqlx::Error::Migrate(Box::new(e))))?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Inserta un job nuevo. Si trae idempotency_key y ya existe un job con esa
    /// clave, devuelve el job existente en lugar de crear uno duplicado
    /// (ver sección "Idempotency Keys" del informe).
    pub async fn create_job(&self, new_job: NewJob) -> Result<Job, QueueError> {
        if let Some(key) = &new_job.idempotency_key {
            if let Some(existing) = self.get_job_by_idempotency_key(key).await? {
                return Ok(existing);
            }
        }

        let scheduled_at = new_job.scheduled_at.unwrap_or_else(Utc::now);

        let row = sqlx::query_as::<_, JobRow>(&format!(
            r#"
            INSERT INTO jobs (job_type, payload, priority, max_attempts, timeout_seconds, scheduled_at, idempotency_key)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING {JOB_COLUMNS}
            "#
        ))
        .bind(&new_job.job_type)
        .bind(&new_job.payload)
        .bind(new_job.priority)
        .bind(new_job.max_attempts)
        .bind(new_job.timeout_seconds)
        .bind(scheduled_at)
        .bind(&new_job.idempotency_key)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.into())
    }

    pub async fn get_job(&self, id: Uuid) -> Result<Option<Job>, QueueError> {
        let row = sqlx::query_as::<_, JobRow>(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Into::into))
    }

    async fn get_job_by_idempotency_key(&self, key: &str) -> Result<Option<Job>, QueueError> {
        let row = sqlx::query_as::<_, JobRow>(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE idempotency_key = $1"
        ))
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Into::into))
    }

    /// Lista jobs, opcionalmente filtrados por estado. Orden más reciente primero.
    pub async fn list_jobs(&self, status: Option<&str>, limit: i64) -> Result<Vec<Job>, QueueError> {
        let rows = if let Some(status) = status {
            sqlx::query_as::<_, JobRow>(&format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE status = $1 ORDER BY created_at DESC LIMIT $2"
            ))
            .bind(status)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, JobRow>(&format!(
                "SELECT {JOB_COLUMNS} FROM jobs ORDER BY created_at DESC LIMIT $1"
            ))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Reclama el siguiente job disponible para un worker.
    ///
    /// Usa `FOR UPDATE SKIP LOCKED` para que múltiples workers puedan hacer
    /// polling concurrente sin bloquearse entre sí ni reclamar el mismo job
    /// dos veces (ver ADR-005). Desde Fase 3 también recoge los jobs en
    /// `retry_scheduled` cuyo backoff ya venció -- para el claim, un retry
    /// listo para correr es indistinguible de un job nuevo.
    pub async fn claim_next_job(&self, worker_id: &str) -> Result<Option<Job>, QueueError> {
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query_as::<_, JobRow>(&format!(
            r#"
            SELECT {JOB_COLUMNS}
            FROM jobs
            WHERE status IN ('pending', 'retry_scheduled') AND scheduled_at <= now()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#
        ))
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };

        let updated = sqlx::query_as::<_, JobRow>(&format!(
            r#"
            UPDATE jobs
            SET status = 'running',
                started_at = now(),
                attempts = attempts + 1,
                worker_id = $2,
                lease_until = now() + ((timeout_seconds + $3) * interval '1 second')
            WHERE id = $1
            RETURNING {JOB_COLUMNS}
            "#
        ))
        .bind(row.id)
        .bind(worker_id)
        .bind(LEASE_GRACE_SECONDS)
        .fetch_one(&mut *tx)
        .await?;

        // Abrimos el registro de este intento. Se cierra en record_success /
        // record_failure buscando la fila con finished_at IS NULL -- no hace
        // falta ir pasando el id del attempt de un lado a otro.
        sqlx::query(
            r#"
            INSERT INTO job_attempts (job_id, attempt_number, worker_id, started_at)
            VALUES ($1, $2, $3, now())
            "#,
        )
        .bind(updated.id)
        .bind(updated.attempts)
        .bind(worker_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some(updated.into()))
    }

    pub async fn mark_completed(&self, id: Uuid) -> Result<(), QueueError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"UPDATE jobs SET status = 'completed', completed_at = now(), lease_until = NULL WHERE id = $1"#,
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE job_attempts SET finished_at = now(), status = 'completed'
            WHERE job_id = $1 AND finished_at IS NULL
            "#,
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Núcleo compartido de "este intento falló, ¿y ahora?". Lo usan tanto
    /// `record_failure` (el worker reporta un fallo mientras sigue vivo)
    /// como `reap_expired_leases` (nadie reporta nada porque el worker ya
    /// no está, así que lo inferimos del lease vencido). Misma decisión de
    /// retry-vs-dead_letter en los dos casos -- un job no debería tener dos
    /// políticas de reintento distintas según quién detectó el fallo.
    ///
    /// El cálculo de backoff vive en SQL a propósito: así la decisión
    /// "cuántos intentos van" y "cuándo es el próximo" quedan atómicas
    /// dentro de un solo UPDATE, sin ida y vuelta con Rust en el medio que
    /// pueda pisarse con otro proceso tocando el mismo job.
    ///
    /// Fórmula: delay = min(2s * 2^(attempts-1), 300s) + random(0..2s).
    /// Cap de 5 minutos y jitter de hasta 2s para no generar manada de
    /// reintentos pegados todos al mismo segundo (thundering herd de
    /// bolsillo). Es un valor fijo por ahora -- si hace falta hacerlo
    /// configurable por job, es la próxima vuelta de tuerca, no un MVP.
    async fn transition_after_failure(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        id: Uuid,
        attempt_status: &str,
        error: &str,
    ) -> Result<String, QueueError> {
        sqlx::query(
            r#"
            UPDATE job_attempts SET finished_at = now(), status = $2, error = $3
            WHERE job_id = $1 AND finished_at IS NULL
            "#,
        )
        .bind(id)
        .bind(attempt_status)
        .bind(error)
        .execute(&mut **tx)
        .await?;

        let row: (String,) = sqlx::query_as(
            r#"
            UPDATE jobs
            SET
                status = CASE WHEN attempts >= max_attempts THEN 'dead_letter' ELSE 'retry_scheduled' END,
                last_error = $2,
                failed_at = CASE WHEN attempts >= max_attempts THEN now() ELSE failed_at END,
                worker_id = NULL,
                lease_until = NULL,
                scheduled_at = CASE
                    WHEN attempts >= max_attempts THEN scheduled_at
                    ELSE now() + (LEAST(2 * power(2, attempts - 1), 300) + random() * 2) * interval '1 second'
                END
            WHERE id = $1
            RETURNING status
            "#,
        )
        .bind(id)
        .bind(error)
        .fetch_one(&mut **tx)
        .await?;

        Ok(row.0)
    }

    /// Registra un intento fallido reportado por un worker vivo. Devuelve
    /// el estado resultante (`retry_scheduled` o `dead_letter`) para que el
    /// caller pueda loguear bien lo que pasó.
    pub async fn record_failure(
        &self,
        id: Uuid,
        error: &str,
        outcome: AttemptOutcome,
    ) -> Result<String, QueueError> {
        let mut tx = self.pool.begin().await?;
        let status = Self::transition_after_failure(&mut tx, id, outcome.as_str(), error).await?;
        tx.commit().await?;
        Ok(status)
    }

    /// El reaper: busca jobs cuyo lease venció sin que nadie los haya
    /// marcado como terminados, y los recupera aplicando la misma lógica
    /// de retry/DLQ que un fallo reportado normalmente.
    ///
    /// `FOR UPDATE SKIP LOCKED` dentro de la misma transacción que hace la
    /// transición (no dos transacciones separadas) es lo que hace esto
    /// seguro con múltiples workers corriendo el reaper al mismo tiempo
    /// (ver ADR-004): si dos reapers compiten por el mismo job abandonado,
    /// uno se queda con la fila bloqueada hasta terminar de transicionarla,
    /// el otro simplemente no la ve y sigue de largo. Ninguno de los dos
    /// pisa al otro ni duplica el trabajo de recovery.
    pub async fn reap_expired_leases(&self) -> Result<Vec<(Uuid, String)>, QueueError> {
        let mut tx = self.pool.begin().await?;

        let expired: Vec<(Uuid,)> = sqlx::query_as(
            r#"
            SELECT id FROM jobs
            WHERE status = 'running' AND lease_until < now()
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;

        let mut results = Vec::with_capacity(expired.len());
        for (id,) in expired {
            let status = Self::transition_after_failure(
                &mut tx,
                id,
                "lease_expired",
                "worker lease expired (worker probably crashed or lost connectivity)",
            )
            .await?;
            results.push((id, status));
        }

        tx.commit().await?;
        Ok(results)
    }

    /// Historial completo de intentos de un job, más reciente primero.
    pub async fn list_attempts(&self, job_id: Uuid) -> Result<Vec<JobAttempt>, QueueError> {
        let rows = sqlx::query_as::<_, JobAttempt>(
            r#"
            SELECT id, job_id, attempt_number, worker_id, started_at, finished_at, status, error
            FROM job_attempts
            WHERE job_id = $1
            ORDER BY attempt_number DESC
            "#,
        )
        .bind(job_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn cancel_job(&self, id: Uuid) -> Result<bool, QueueError> {
        let result = sqlx::query(
            r#"UPDATE jobs SET status = 'cancelled' WHERE id = $1 AND status = 'pending'"#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Registra un worker al arrancar. Es upsert porque si reiniciás un
    /// worker con el mismo WORKER_ID (por ejemplo en un redeploy), no tiene
    /// sentido explotar por unique constraint -- simplemente actualiza
    /// concurrency y started_at.
    ///
    /// Esto NO es el mecanismo de liveness (eso es Fase 4 con heartbeats de
    /// verdad). Si el worker se cae de mala manera, la fila queda como
    /// "último dato conocido" nomás.
    pub async fn register_worker(&self, worker_id: &str, concurrency: i32) -> Result<(), QueueError> {
        sqlx::query(
            r#"
            INSERT INTO workers (id, concurrency, started_at)
            VALUES ($1, $2, now())
            ON CONFLICT (id) DO UPDATE
                SET concurrency = EXCLUDED.concurrency,
                    started_at = EXCLUDED.started_at
            "#,
        )
        .bind(worker_id)
        .bind(concurrency)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Cuenta jobs agrupados por estado. Sirve para el `queue_depth`
    /// observable que pide la Fase 2 y como base cruda para las métricas
    /// Prometheus que llegan en Fase 6.
    pub async fn count_by_status(&self) -> Result<Vec<StatusCount>, QueueError> {
        let rows = sqlx::query_as::<_, StatusCount>(
            r#"SELECT status, COUNT(*) as count FROM jobs GROUP BY status"#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Conteo de intentos por resultado (`completed`/`failed`/`timeout`/
    /// `lease_expired`). A diferencia de `count_by_status` (estado actual
    /// de cada job), esto es un contador que solo crece -- sirve como
    /// `_total` de Prometheus en `GET /metrics` sin necesitar acumular
    /// nada en memoria del proceso (ver comentario en `handlers::metrics`).
    pub async fn count_attempts_by_outcome(&self) -> Result<Vec<StatusCount>, QueueError> {
        let rows = sqlx::query_as::<_, StatusCount>(
            r#"
            SELECT status, COUNT(*) as count
            FROM job_attempts
            WHERE status IS NOT NULL
            GROUP BY status
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Percentiles de duración de attempts terminados, agrupados por tipo
    /// de job. `percentile_cont` es una función de agregación estándar de
    /// Postgres -- no hace falta mantener un histograma en memoria en
    /// ningún proceso, la propia base lo calcula al vuelo.
    pub async fn job_duration_percentiles(&self) -> Result<Vec<JobDurationStats>, QueueError> {
        let rows = sqlx::query_as::<_, JobDurationStats>(
            r#"
            SELECT
                j.job_type,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (a.finished_at - a.started_at))) AS p50_seconds,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (a.finished_at - a.started_at))) AS p95_seconds,
                COUNT(*) AS sample_count
            FROM job_attempts a
            JOIN jobs j ON j.id = a.job_id
            WHERE a.finished_at IS NOT NULL
            GROUP BY j.job_type
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Todos los workers que alguna vez se registraron (no solo los vivos
    /// ahora mismo -- para eso está `Heartbeats::list_alive`, que consulta
    /// Redis). La API combina las dos fuentes en `GET /workers`.
    pub async fn list_workers(&self) -> Result<Vec<WorkerInfo>, QueueError> {
        let rows = sqlx::query_as::<_, WorkerInfo>(
            r#"SELECT id, concurrency, started_at FROM workers ORDER BY started_at DESC"#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    // ---- Fase 5: scheduling ----------------------------------------

    /// Crea un cron schedule nuevo. Valida la expresión y calcula el
    /// primer `next_run_at` acá mismo -- quien llama no elige esa fecha,
    /// para que no haya forma de crear un schedule con un `next_run_at`
    /// inconsistente con su propia expresión.
    pub async fn create_cron_schedule(&self, new: NewCronSchedule) -> Result<CronSchedule, QueueError> {
        let expr = crate::cron::CronExpr::parse(&new.cron_expr)
            .map_err(|e| QueueError::InvalidPayload(e.to_string()))?;
        let next_run_at = expr
            .next_after(Utc::now())
            .ok_or_else(|| QueueError::InvalidPayload("la expresión cron nunca matchea ninguna fecha".into()))?;

        let row = sqlx::query_as::<_, CronSchedule>(
            r#"
            INSERT INTO cron_schedules
                (name, cron_expr, job_type, payload, priority, max_attempts, timeout_seconds, next_run_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, name, cron_expr, job_type, payload, priority, max_attempts,
                      timeout_seconds, enabled, next_run_at, last_run_at, created_at
            "#,
        )
        .bind(&new.name)
        .bind(&new.cron_expr)
        .bind(&new.job_type)
        .bind(&new.payload)
        .bind(new.priority)
        .bind(new.max_attempts)
        .bind(new.timeout_seconds)
        .bind(next_run_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    pub async fn list_cron_schedules(&self) -> Result<Vec<CronSchedule>, QueueError> {
        let rows = sqlx::query_as::<_, CronSchedule>(
            r#"
            SELECT id, name, cron_expr, job_type, payload, priority, max_attempts,
                   timeout_seconds, enabled, next_run_at, last_run_at, created_at
            FROM cron_schedules ORDER BY name
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, QueueError> {
        let row = sqlx::query_as::<_, CronSchedule>(
            r#"
            SELECT id, name, cron_expr, job_type, payload, priority, max_attempts,
                   timeout_seconds, enabled, next_run_at, last_run_at, created_at
            FROM cron_schedules WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn delete_cron_schedule(&self, id: Uuid) -> Result<bool, QueueError> {
        let result = sqlx::query("DELETE FROM cron_schedules WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Cron schedules habilitados cuyo `next_run_at` ya pasó. Solo el líder
    /// del scheduler (ver `Storage::try_become_scheduler_leader`) debería
    /// llamar a esto -- no tiene `SKIP LOCKED` porque no está pensado para
    /// que varios procesos lo llamen a la vez, a propósito (ver ADR-006).
    pub async fn due_cron_schedules(&self) -> Result<Vec<CronSchedule>, QueueError> {
        let rows = sqlx::query_as::<_, CronSchedule>(
            r#"
            SELECT id, name, cron_expr, job_type, payload, priority, max_attempts,
                   timeout_seconds, enabled, next_run_at, last_run_at, created_at
            FROM cron_schedules
            WHERE enabled = true AND next_run_at <= now()
            ORDER BY next_run_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Dispara un cron schedule: crea el job real a partir de la plantilla
    /// y avanza `next_run_at` al siguiente horario. Las dos cosas van en la
    /// misma transacción -- si algo falla a mitad de camino, mejor que el
    /// schedule quede exactamente como estaba (y se reintente en el
    /// próximo ciclo del scheduler) a que quede "a medio disparar".
    ///
    /// El job creado lleva un `idempotency_key` derivado del id del
    /// schedule y del `next_run_at` que se estaba cumpliendo. Es una
    /// segunda red de seguridad además de la exclusión mutua del líder: si
    /// por lo que fuera este disparo se ejecuta dos veces para el mismo
    /// horario, `create_job` ya sabe devolver el job existente en vez de
    /// duplicar (Fase 1).
    pub async fn fire_cron_schedule(&self, schedule: &CronSchedule) -> Result<Job, QueueError> {
        let expr = crate::cron::CronExpr::parse(&schedule.cron_expr)
            .map_err(|e| QueueError::InvalidPayload(e.to_string()))?;
        let next_run_at = expr.next_after(schedule.next_run_at).ok_or_else(|| {
            QueueError::InvalidPayload("la expresión cron dejó de tener próximas ocurrencias".into())
        })?;

        let idempotency_key = format!("cron:{}:{}", schedule.id, schedule.next_run_at.to_rfc3339());

        let mut tx = self.pool.begin().await?;

        let job_row = sqlx::query_as::<_, JobRow>(&format!(
            r#"
            INSERT INTO jobs (job_type, payload, priority, max_attempts, timeout_seconds, scheduled_at, idempotency_key)
            VALUES ($1, $2, $3, $4, $5, now(), $6)
            ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
            RETURNING {JOB_COLUMNS}
            "#
        ))
        .bind(&schedule.job_type)
        .bind(&schedule.payload)
        .bind(schedule.priority)
        .bind(schedule.max_attempts)
        .bind(schedule.timeout_seconds)
        .bind(&idempotency_key)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query("UPDATE cron_schedules SET last_run_at = now(), next_run_at = $2 WHERE id = $1")
            .bind(schedule.id)
            .bind(next_run_at)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(job_row.into())
    }

    /// Intenta convertirse en el líder del scheduler de cron usando un
    /// advisory lock de sesión de Postgres (ver ADR-006). Si lo consigue,
    /// devuelve una conexión dedicada que hay que mantener viva mientras
    /// dure el liderazgo -- soltarla (drop) libera el lock automáticamente,
    /// tanto si se hace a propósito como si el proceso muere.
    ///
    /// `pg_try_advisory_lock` es no bloqueante: si otro proceso ya es
    /// líder, devuelve `Ok(None)` al toque en vez de esperar.
    pub async fn try_become_scheduler_leader(
        &self,
    ) -> Result<Option<sqlx::pool::PoolConnection<sqlx::Postgres>>, QueueError> {
        let mut conn = self.pool.acquire().await?;
        let (acquired,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(SCHEDULER_LEADER_LOCK_KEY)
            .fetch_one(&mut *conn)
            .await?;

        if acquired {
            Ok(Some(conn))
        } else {
            Ok(None)
        }
    }
}

/// Key arbitraria pero fija para el advisory lock del líder del scheduler
/// de cron. No tiene que ver con ningún dato real -- es solo un nombre de
/// lock con forma de número, elegido para que no choque por casualidad con
/// otro lock que alguien agregue después (advisory locks comparten un
/// único namespace de enteros por base).
const SCHEDULER_LEADER_LOCK_KEY: i64 = 0x63726f6e_6c6561; // "cronlea" en hex, mnemónico nomás
