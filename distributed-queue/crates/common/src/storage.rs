use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::QueueError;
use crate::model::{AttemptOutcome, Job, JobAttempt, JobRow, NewJob, StatusCount};

/// Columnas de `jobs` compartidas por casi todas las queries de este módulo.
/// Un solo lugar para tocar si el día de mañana se agrega una columna --
/// ya nos pasó de olvidarnos una en un SELECT y perder una tarde con eso.
const JOB_COLUMNS: &str = r#"id, job_type, payload, status, priority, attempts, max_attempts,
                      scheduled_at, created_at, started_at, completed_at, failed_at,
                      worker_id, lease_until, timeout_seconds, idempotency_key, last_error"#;

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
                worker_id = $2
            WHERE id = $1
            RETURNING {JOB_COLUMNS}
            "#
        ))
        .bind(row.id)
        .bind(worker_id)
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

        sqlx::query(r#"UPDATE jobs SET status = 'completed', completed_at = now() WHERE id = $1"#)
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

    /// Registra un intento fallido y decide qué pasa después: si todavía
    /// quedan reintentos, agenda el próximo con backoff exponencial + jitter;
    /// si no, manda el job a `dead_letter`. Devuelve el estado resultante
    /// para que el caller pueda loguear bien lo que pasó.
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
    pub async fn record_failure(
        &self,
        id: Uuid,
        error: &str,
        outcome: AttemptOutcome,
    ) -> Result<String, QueueError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            UPDATE job_attempts SET finished_at = now(), status = $2, error = $3
            WHERE job_id = $1 AND finished_at IS NULL
            "#,
        )
        .bind(id)
        .bind(outcome.as_str())
        .bind(error)
        .execute(&mut *tx)
        .await?;

        let row: (String,) = sqlx::query_as(
            r#"
            UPDATE jobs
            SET
                status = CASE WHEN attempts >= max_attempts THEN 'dead_letter' ELSE 'retry_scheduled' END,
                last_error = $2,
                failed_at = CASE WHEN attempts >= max_attempts THEN now() ELSE failed_at END,
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
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(row.0)
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
}
