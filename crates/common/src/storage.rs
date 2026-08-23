use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::QueueError;
use crate::model::{Job, JobRow, NewJob};

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

        let row = sqlx::query_as::<_, JobRow>(
            r#"
            INSERT INTO jobs (job_type, payload, priority, max_attempts, scheduled_at, idempotency_key)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, job_type, payload, status, priority, attempts, max_attempts,
                      scheduled_at, created_at, started_at, completed_at, failed_at,
                      worker_id, lease_until, idempotency_key, last_error
            "#,
        )
        .bind(&new_job.job_type)
        .bind(&new_job.payload)
        .bind(new_job.priority)
        .bind(new_job.max_attempts)
        .bind(scheduled_at)
        .bind(&new_job.idempotency_key)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.into())
    }

    pub async fn get_job(&self, id: Uuid) -> Result<Option<Job>, QueueError> {
        let row = sqlx::query_as::<_, JobRow>(
            r#"SELECT id, job_type, payload, status, priority, attempts, max_attempts,
                      scheduled_at, created_at, started_at, completed_at, failed_at,
                      worker_id, lease_until, idempotency_key, last_error
               FROM jobs WHERE id = $1"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Into::into))
    }

    async fn get_job_by_idempotency_key(&self, key: &str) -> Result<Option<Job>, QueueError> {
        let row = sqlx::query_as::<_, JobRow>(
            r#"SELECT id, job_type, payload, status, priority, attempts, max_attempts,
                      scheduled_at, created_at, started_at, completed_at, failed_at,
                      worker_id, lease_until, idempotency_key, last_error
               FROM jobs WHERE idempotency_key = $1"#,
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Into::into))
    }

    /// Lista jobs, opcionalmente filtrados por estado. Orden más reciente primero.
    pub async fn list_jobs(&self, status: Option<&str>, limit: i64) -> Result<Vec<Job>, QueueError> {
        let rows = if let Some(status) = status {
            sqlx::query_as::<_, JobRow>(
                r#"SELECT id, job_type, payload, status, priority, attempts, max_attempts,
                          scheduled_at, created_at, started_at, completed_at, failed_at,
                          worker_id, lease_until, idempotency_key, last_error
                   FROM jobs WHERE status = $1 ORDER BY created_at DESC LIMIT $2"#,
            )
            .bind(status)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, JobRow>(
                r#"SELECT id, job_type, payload, status, priority, attempts, max_attempts,
                          scheduled_at, created_at, started_at, completed_at, failed_at,
                          worker_id, lease_until, idempotency_key, last_error
                   FROM jobs ORDER BY created_at DESC LIMIT $1"#,
            )
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
    /// dos veces (ver sección "Consistencia y concurrencia" / ADR-005).
    /// La formalización completa de concurrency limits llega en Fase 2; esta
    /// query ya es segura para múltiples workers desde ahora.
    pub async fn claim_next_job(&self, worker_id: &str) -> Result<Option<Job>, QueueError> {
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query_as::<_, JobRow>(
            r#"
            SELECT id, job_type, payload, status, priority, attempts, max_attempts,
                   scheduled_at, created_at, started_at, completed_at, failed_at,
                   worker_id, lease_until, idempotency_key, last_error
            FROM jobs
            WHERE status = 'pending' AND scheduled_at <= now()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#,
        )
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };

        let updated = sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE jobs
            SET status = 'running',
                started_at = now(),
                attempts = attempts + 1,
                worker_id = $2
            WHERE id = $1
            RETURNING id, job_type, payload, status, priority, attempts, max_attempts,
                      scheduled_at, created_at, started_at, completed_at, failed_at,
                      worker_id, lease_until, idempotency_key, last_error
            "#,
        )
        .bind(row.id)
        .bind(worker_id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some(updated.into()))
    }

    pub async fn mark_completed(&self, id: Uuid) -> Result<(), QueueError> {
        sqlx::query(
            r#"UPDATE jobs SET status = 'completed', completed_at = now() WHERE id = $1"#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Marca el job como fallido. La lógica de reintentos con backoff y DLQ
    /// se implementa en Fase 3; por ahora un fallo es terminal.
    pub async fn mark_failed(&self, id: Uuid, error: &str) -> Result<(), QueueError> {
        sqlx::query(
            r#"UPDATE jobs SET status = 'failed', failed_at = now(), last_error = $2 WHERE id = $1"#,
        )
        .bind(id)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
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
}
