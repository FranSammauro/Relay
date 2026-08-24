use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Estados posibles de un job. Ver sección "Modelo de estados" del informe técnico.
///
/// Transiciones válidas (resumen):
///   pending -> running -> completed
///   pending -> running -> failed -> retry_scheduled -> pending
///   pending -> running -> failed -> dead_letter   (max_attempts agotado)
///   pending -> cancelled
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    RetryScheduled,
    DeadLetter,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::RetryScheduled => "retry_scheduled",
            JobStatus::DeadLetter => "dead_letter",
            JobStatus::Cancelled => "cancelled",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => JobStatus::Pending,
            "running" => JobStatus::Running,
            "completed" => JobStatus::Completed,
            "failed" => JobStatus::Failed,
            "retry_scheduled" => JobStatus::RetryScheduled,
            "dead_letter" => JobStatus::DeadLetter,
            "cancelled" => JobStatus::Cancelled,
            _ => return None,
        })
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Representación completa de un job tal como se persiste en PostgreSQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub status: JobStatus,
    pub priority: i32,
    pub attempts: i32,
    pub max_attempts: i32,

    pub scheduled_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,

    pub worker_id: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,

    pub timeout_seconds: i32,

    pub idempotency_key: Option<String>,
    pub last_error: Option<String>,
}

/// Fila cruda tal como sqlx la devuelve (status como texto).
#[derive(Debug, sqlx::FromRow)]
pub struct JobRow {
    pub id: Uuid,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub priority: i32,
    pub attempts: i32,
    pub max_attempts: i32,
    pub scheduled_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub worker_id: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub timeout_seconds: i32,
    pub idempotency_key: Option<String>,
    pub last_error: Option<String>,
}

impl From<JobRow> for Job {
    fn from(r: JobRow) -> Self {
        Job {
            id: r.id,
            job_type: r.job_type,
            payload: r.payload,
            status: JobStatus::from_str_opt(&r.status).unwrap_or(JobStatus::Pending),
            priority: r.priority,
            attempts: r.attempts,
            max_attempts: r.max_attempts,
            scheduled_at: r.scheduled_at,
            created_at: r.created_at,
            started_at: r.started_at,
            completed_at: r.completed_at,
            failed_at: r.failed_at,
            worker_id: r.worker_id,
            lease_until: r.lease_until,
            timeout_seconds: r.timeout_seconds,
            idempotency_key: r.idempotency_key,
            last_error: r.last_error,
        }
    }
}

/// DTO de entrada para crear un job vía la API.
#[derive(Debug, Clone, Deserialize)]
pub struct NewJob {
    #[serde(rename = "type")]
    pub job_type: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i32,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: i32,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
}

fn default_priority() -> i32 {
    50
}

fn default_max_attempts() -> i32 {
    5
}

fn default_timeout_seconds() -> i32 {
    30
}

/// Resultado de un intento de ejecución, tal como queda en `job_attempts`.
/// `TimedOut` es un caso particular de fallo -- cuenta contra max_attempts
/// igual que cualquier otro error, pero vale la pena distinguirlo en el
/// historial porque diagnosticar "se colgó" es distinto a "tiró excepción".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    Completed,
    Failed,
    TimedOut,
}

impl AttemptOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            AttemptOutcome::Completed => "completed",
            AttemptOutcome::Failed => "failed",
            AttemptOutcome::TimedOut => "timeout",
        }
    }
}

/// Fila de `job_attempts`, para exponer el historial de ejecución de un job.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct JobAttempt {
    pub id: i64,
    pub job_id: Uuid,
    pub attempt_number: i32,
    pub worker_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: Option<String>,
    pub error: Option<String>,
}

/// Fila de la tabla `workers` (Fase 2: solo registro, sin heartbeat todavía).
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct WorkerInfo {
    pub id: String,
    pub concurrency: i32,
    pub started_at: DateTime<Utc>,
}

/// Conteo de jobs agrupados por estado, para el endpoint de stats.
/// No es Prometheus todavía (eso es Fase 6), pero da la misma info en JSON.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}
