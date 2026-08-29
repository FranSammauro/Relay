use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use crate::api_keys::{ApiKeyRecord, ApiKeyRole, ApiKeySecret, StoredApiKey};
use crate::error::QueueError;
use crate::model::{
    AttemptOutcome, BenchTimestamps, CronSchedule, Job, JobAttempt, JobDurationStats, JobRow,
    NewCronSchedule, NewJob, StatusCount, WorkerInfo,
};

/// Columnas de `jobs` compartidas por casi todas las consultas de este
/// módulo. Centralizarlas en una sola constante evita el riesgo de omitir
/// una columna en algún SELECT al agregar campos nuevos.
const JOB_COLUMNS: &str = r#"id, job_type, payload, status, priority, attempts, max_attempts,
                      scheduled_at, created_at, started_at, completed_at, failed_at,
                      worker_id, lease_until, timeout_seconds, idempotency_key, last_error"#;

/// Margen adicional que se otorga al lease de un job por encima de su
/// propio `timeout_seconds`. El timeout ya finaliza el job dentro del
/// proceso si este se cuelga; el lease cubre el caso en que el proceso
/// completo del worker desaparece (kill -9, OOM, caída del nodo) antes de
/// que el timeout llegue a activarse. Un margen de 30 segundos absorbe
/// variaciones normales de scheduling y pausas de recolección de basura
/// sin que un job legítimamente lento se interprete como abandonado.
///
/// Nota de alcance: el lease se fija una única vez al momento del claim y
/// no se renueva mientras el job se ejecuta. Para jobs individuales esto
/// es suficiente, dado que timeout_seconds más este margen ya definen un
/// límite razonable. Un lease renovable, con heartbeat por job en lugar
/// de por worker, sería la evolución natural si en el futuro existieran
/// jobs de duración muy variable; por ahora queda fuera del alcance
/// definido para el MVP.
const LEASE_GRACE_SECONDS: i32 = 30;

/// Storage es el único punto de acceso a PostgreSQL, que actúa como
/// fuente de verdad para el estado persistente del sistema (ADR-001).
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

    /// Inserta un job nuevo. Si se especifica idempotency_key y ya existe
    /// un job con esa clave, devuelve el job existente en lugar de crear
    /// uno duplicado (ver sección "Idempotency Keys" del informe técnico).
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
    /// Utiliza `FOR UPDATE SKIP LOCKED` para que múltiples workers puedan
    /// realizar polling concurrente sin bloquearse entre sí ni reclamar el
    /// mismo job dos veces (ver ADR-005). Desde la Fase 3 también incluye
    /// los jobs en `retry_scheduled` cuyo backoff ya venció: a efectos del
    /// claim, un reintento listo para ejecutarse es equivalente a un job
    /// nuevo.
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

        // Se abre el registro del intento. Se cierra posteriormente en
        // mark_completed o transition_after_failure, localizando la fila
        // con finished_at IS NULL; no es necesario propagar el id del
        // attempt entre funciones.
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

    /// Núcleo compartido de la transición "este intento falló, cuál es el
    /// siguiente paso". Lo utilizan tanto `record_failure` (el worker
    /// reporta un fallo mientras sigue activo) como `reap_expired_leases`
    /// (no hay reporte porque el worker ya no está disponible, y el fallo
    /// se infiere del lease vencido). Ambos casos aplican la misma
    /// decisión entre reintento y dead_letter: un job no debería tener
    /// políticas de reintento distintas según qué mecanismo detectó el
    /// fallo.
    ///
    /// El cálculo de backoff se realiza en SQL de forma deliberada: así la
    /// decisión de cuántos intentos restan y cuándo corresponde el
    /// próximo quedan dentro de una única transacción UPDATE, sin una
    /// ida y vuelta intermedia hacia Rust que pueda quedar desincronizada
    /// si otro proceso modifica el mismo job.
    ///
    /// Fórmula: delay = min(2s * 2^(attempts-1), 300s) + random(0..2s).
    /// El límite superior de 5 minutos y el jitter de hasta 2 segundos
    /// evitan que múltiples reintentos queden agrupados en el mismo
    /// instante. El valor es fijo por el momento; hacerlo configurable por
    /// job queda fuera del alcance definido para el MVP.
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

    /// Registra un intento fallido reportado por un worker activo.
    /// Devuelve el estado resultante (`retry_scheduled` o `dead_letter`)
    /// para que quien llame pueda registrar correctamente el resultado.
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

    /// El reaper: localiza jobs cuyo lease venció sin que nadie los haya
    /// marcado como finalizados y los recupera aplicando la misma lógica
    /// de reintento o dead-letter que un fallo reportado normalmente.
    ///
    /// El uso de `FOR UPDATE SKIP LOCKED` dentro de la misma transacción
    /// que realiza la transición, en lugar de dos transacciones separadas,
    /// es lo que garantiza la seguridad cuando múltiples workers ejecutan
    /// el reaper simultáneamente (ver ADR-004): si dos instancias del
    /// reaper compiten por el mismo job abandonado, una retiene la fila
    /// bloqueada hasta completar la transición mientras la otra
    /// simplemente la omite y continúa. Ninguna de las dos interfiere con
    /// la otra ni se duplica el trabajo de recuperación.
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

    /// Historial completo de intentos de un job, ordenado del más reciente al más antiguo.
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

    /// Registra un worker al iniciar. Es una operación upsert: si un
    /// worker se reinicia con el mismo WORKER_ID, por ejemplo tras un
    /// redeploy, no corresponde fallar por la restricción de unicidad,
    /// sino actualizar concurrency y started_at.
    ///
    /// Esto no constituye el mecanismo de liveness (ese corresponde a la
    /// Fase 4, con heartbeats reales). Si el worker finaliza de forma
    /// anómala, la fila permanece como el último dato conocido.
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

    /// Cuenta jobs agrupados por estado. Utilizado por el `queue_depth`
    /// observable de la Fase 2 y como base para las métricas Prometheus de
    /// la Fase 6.
    pub async fn count_by_status(&self) -> Result<Vec<StatusCount>, QueueError> {
        let rows = sqlx::query_as::<_, StatusCount>(
            r#"SELECT status, COUNT(*) as count FROM jobs GROUP BY status"#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Conteo de intentos por resultado (`completed`, `failed`, `timeout`,
    /// `lease_expired`). A diferencia de `count_by_status`, que refleja el
    /// estado actual de cada job, este valor es un contador monótono
    /// creciente. Cumple el rol de métrica `_total` de Prometheus en
    /// `GET /metrics` sin necesidad de acumular estado en memoria del
    /// proceso (ver comentario en `handlers::metrics`).
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

    /// Percentiles de duración de attempts finalizados, agrupados por tipo
    /// de job. `percentile_cont` es una función de agregación estándar de
    /// PostgreSQL: no es necesario mantener un histograma en memoria de
    /// ningún proceso, ya que la propia base de datos realiza el cálculo.
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

    /// Timestamps crudos de un lote de jobs identificados por un prefijo
    /// de idempotency_key, para el benchmark de la Fase 7. Se devuelven
    /// los valores sin agregar (a diferencia de job_duration_percentiles)
    /// porque el benchmark necesita calcular percentiles combinando estos
    /// datos con las mediciones de latencia de envío tomadas del lado del
    /// cliente, no solo la duración de ejecución.
    pub async fn bench_timestamps(&self, idempotency_prefix: &str) -> Result<Vec<BenchTimestamps>, QueueError> {
        let rows = sqlx::query_as::<_, BenchTimestamps>(
            r#"
            SELECT id, status, created_at, started_at, completed_at, failed_at
            FROM jobs
            WHERE idempotency_key LIKE $1
            "#,
        )
        .bind(format!("{idempotency_prefix}%"))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Todos los workers que se registraron en algún momento, no
    /// únicamente los que están activos ahora mismo (para eso está
    /// `Heartbeats::list_alive`, que consulta Redis). La API combina
    /// ambas fuentes en `GET /workers`.
    pub async fn list_workers(&self) -> Result<Vec<WorkerInfo>, QueueError> {
        let rows = sqlx::query_as::<_, WorkerInfo>(
            r#"SELECT id, concurrency, started_at FROM workers ORDER BY started_at DESC"#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    // Fase 5: scheduling.

    /// Crea un cron schedule nuevo. Valida la expresión y calcula el
    /// primer `next_run_at` en esta misma función; quien invoca el método
    /// no elige esa fecha directamente, de modo que no es posible crear un
    /// schedule con un `next_run_at` inconsistente con su propia
    /// expresión.
    pub async fn create_cron_schedule(&self, new: NewCronSchedule) -> Result<CronSchedule, QueueError> {
        let expr = crate::cron::CronExpr::parse(&new.cron_expr)
            .map_err(|e| QueueError::InvalidPayload(e.to_string()))?;
        let next_run_at = expr
            .next_after(Utc::now())
            .ok_or_else(|| QueueError::InvalidPayload("la expresión cron no coincide con ninguna fecha".into()))?;

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

    /// Cron schedules habilitados cuyo `next_run_at` ya venció. Solo el
    /// líder del scheduler (ver `Storage::try_become_scheduler_leader`)
    /// debe invocar este método; deliberadamente no utiliza
    /// `SKIP LOCKED`, ya que no está pensado para ser llamado por
    /// múltiples procesos de forma concurrente (ver ADR-006).
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
    /// y avanza `next_run_at` al siguiente horario. Ambas operaciones se
    /// realizan en la misma transacción; si algo falla a mitad de camino,
    /// es preferible que el schedule permanezca exactamente como estaba,
    /// para ser reintentado en el próximo ciclo del scheduler, a que quede
    /// en un estado intermedio.
    ///
    /// El job creado incluye un `idempotency_key` derivado del id del
    /// schedule y del `next_run_at` que se estaba cumpliendo. Esto
    /// constituye una segunda capa de seguridad además de la exclusión
    /// mutua del líder: si por alguna razón este disparo se ejecutara dos
    /// veces para el mismo horario, `create_job` devuelve el job existente
    /// en lugar de crear un duplicado (ver Fase 1).
    pub async fn fire_cron_schedule(&self, schedule: &CronSchedule) -> Result<Job, QueueError> {
        let expr = crate::cron::CronExpr::parse(&schedule.cron_expr)
            .map_err(|e| QueueError::InvalidPayload(e.to_string()))?;
        let next_run_at = expr.next_after(schedule.next_run_at).ok_or_else(|| {
            QueueError::InvalidPayload("la expresión cron ya no tiene próximas ocurrencias".into())
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

    /// Intenta convertirse en líder del scheduler de cron mediante un
    /// advisory lock de sesión de PostgreSQL (ver ADR-006). Si lo obtiene,
    /// devuelve una conexión dedicada que debe mantenerse activa mientras
    /// dure el liderazgo. Liberar esa conexión, ya sea de forma explícita
    /// o porque el proceso finaliza, libera automáticamente el lock.
    ///
    /// `pg_try_advisory_lock` es no bloqueante: si otro proceso ya es
    /// líder, devuelve `Ok(None)` de inmediato en lugar de esperar.
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

    // Fase 8: API keys (ver ADR-007).

    /// Crea una key nueva y la devuelve con su secreto en texto plano.
    /// Este es el ÚNICO momento en que la key completa existe en el
    /// sistema: se imprime al caller y no queda en ningún lugar (en la
    /// base solo viven `key_prefix` y el hash).
    pub async fn create_api_key(&self, name: &str, role: ApiKeyRole) -> Result<(ApiKeyRecord, ApiKeySecret), QueueError> {
        let secret = crate::api_keys::generate();

        let row: (Uuid, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
            r#"
            INSERT INTO api_keys (name, key_prefix, key_hash, role)
            VALUES ($1, $2, $3, $4)
            RETURNING id, created_at
            "#,
        )
        .bind(name)
        .bind(secret.prefix())
        .bind(secret.hash())
        .bind(role.as_str())
        .fetch_one(&self.pool)
        .await?;

        let record = ApiKeyRecord {
            id: row.0,
            name: name.to_string(),
            key_prefix: secret.prefix().to_string(),
            role,
            created_at: row.1,
            revoked_at: None,
            last_used_at: None,
        };

        Ok((record, secret))
    }

    /// Lista todas las keys (activas y revocadas) para auditoría/provisión
    /// de admin. Nunca incluye el hash ni el secreto; solo `key_prefix`.
    pub async fn list_api_keys(&self) -> Result<Vec<ApiKeyRecord>, QueueError> {
        let rows = sqlx::query_as::<_, ApiKeyRecord>(
            r#"
            SELECT id, name, key_prefix, role, created_at, revoked_at, last_used_at
            FROM api_keys
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Busca una key por prefijo para el camino de verificación de la API.
    /// Expone el hash internamente: no debe serializarse ni salir del proceso.
    pub async fn find_api_key_by_prefix(&self, prefix: &str) -> Result<Option<StoredApiKey>, QueueError> {
        let row = sqlx::query_as::<_, StoredApiKey>(
            r#"
            SELECT id, key_prefix, key_hash, role, revoked_at
            FROM api_keys
            WHERE key_prefix = $1
            "#,
        )
        .bind(prefix)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Revoca una key por prefijo. Devuelve `false` si no existía o ya
    /// estaba revocada.
    pub async fn revoke_api_key_by_prefix(&self, prefix: &str) -> Result<bool, QueueError> {
        let result = sqlx::query(
            r#"UPDATE api_keys SET revoked_at = now() WHERE key_prefix = $1 AND revoked_at IS NULL"#,
        )
        .bind(prefix)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Registra `last_used_at` de forma "mejor esfuerzo": el WHERE con la
    /// ventana de un minuto hace que, al 1 rps promedio, se escriba como
    /// máximo una vez por minuto por key, y el caller (el middleware de
    /// auth) puede lanzarlo con `tokio::spawn` sin esperar el resultado.
    pub async fn touch_api_key(&self, id: Uuid) -> Result<(), QueueError> {
        sqlx::query(
            r#"
            UPDATE api_keys
            SET last_used_at = now()
            WHERE id = $1 AND (last_used_at IS NULL OR last_used_at < now() - interval '1 minute')
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// Clave arbitraria pero fija para el advisory lock del líder del
/// scheduler de cron. No corresponde a ningún dato real; es únicamente un
/// identificador numérico elegido para evitar colisiones con otro lock
/// que se agregue en el futuro (los advisory locks comparten un único
/// espacio de enteros por base de datos).
const SCHEDULER_LEADER_LOCK_KEY: i64 = 0x63726f6e_6c6561; // Representación hexadecimal de "cronlea", como valor mnemónico.
