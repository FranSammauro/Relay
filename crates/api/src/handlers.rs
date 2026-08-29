use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use common::{NewCronSchedule, NewJob, QueueError};

use crate::state::AppState;

/// Envoltorio de error que traduce QueueError a una respuesta HTTP + JSON.
pub struct ApiError(QueueError);

impl From<QueueError> for ApiError {
    fn from(e: QueueError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self.0 {
            QueueError::NotFound(_) => (StatusCode::NOT_FOUND, self.0.to_string()),
            QueueError::InvalidPayload(_) => (StatusCode::BAD_REQUEST, self.0.to_string()),
            QueueError::InvalidState(_) => (StatusCode::CONFLICT, self.0.to_string()),
            QueueError::Database(_) => {
                tracing::error!(error = %self.0, "database error handling request");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
            }
            QueueError::Redis(_) => {
                // En este contexto, Redis representa coordinación efímera
                // (ver ADR-002), no una fuente de verdad. Si no está
                // disponible, el endpoint que lo requiere no puede
                // responder correctamente, pero esto no constituye un
                // error interno del servicio (500), sino la ausencia de
                // una dependencia externa (503).
                tracing::error!(error = %self.0, "redis error handling request");
                (StatusCode::SERVICE_UNAVAILABLE, "liveness backend unavailable".to_string())
            }
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

pub async fn ready(State(state): State<AppState>) -> Response {
    match sqlx::query("SELECT 1").execute(state.storage.pool()).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({ "status": "ready" }))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "readiness check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "status": "not_ready" })),
            )
                .into_response()
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
    id: Uuid,
    status: String,
}

pub async fn create_job(
    State(state): State<AppState>,
    Json(new_job): Json<NewJob>,
) -> Result<(StatusCode, Json<CreateJobResponse>), ApiError> {
    if new_job.job_type.trim().is_empty() {
        return Err(QueueError::InvalidPayload("`type` is required".into()).into());
    }

    let job = state.storage.create_job(new_job).await?;

    tracing::info!(
        event = "job_submitted",
        job_id = %job.id,
        job_type = %job.job_type,
        "job accepted"
    );

    Ok((
        StatusCode::CREATED,
        Json(CreateJobResponse {
            id: job.id,
            status: job.status.as_str().to_string(),
        }),
    ))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<common::Job>, ApiError> {
    let job = state
        .storage
        .get_job(id)
        .await?
        .ok_or(QueueError::NotFound(id))?;

    Ok(Json(job))
}

/// Fase 3: historial de intentos de un job, con un registro por cada vez
/// que un worker lo tomó. Permite responder por qué un job falló
/// determinada cantidad de veces sin necesidad de revisar los logs de
/// varios workers distintos.
pub async fn get_job_attempts(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<common::JobAttempt>>, ApiError> {
    // Si el job no existe, se devuelve 404 en lugar de una lista vacía de
    // forma silenciosa; resulta más útil al depurar un id incorrecto.
    state.storage.get_job(id).await?.ok_or(QueueError::NotFound(id))?;

    let attempts = state.storage.list_attempts(id).await?;
    Ok(Json(attempts))
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    status: Option<String>,
    limit: Option<i64>,
}

pub async fn list_jobs(
    State(state): State<AppState>,
    Query(q): Query<ListJobsQuery>,
) -> Result<Json<Vec<common::Job>>, ApiError> {
    let jobs = state
        .storage
        .list_jobs(q.status.as_deref(), q.limit.unwrap_or(50))
        .await?;

    Ok(Json(jobs))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let cancelled = state.storage.cancel_job(id).await?;

    if cancelled {
        tracing::info!(event = "job_cancelled", job_id = %id);
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(QueueError::InvalidState(id).into())
    }
}

/// Fase 2: conteo de jobs por estado, la versión cruda de `queue_depth`
/// antes de que exista un endpoint /metrics en formato Prometheus (Fase 6).
/// Sirve tal cual para el test de concurrencia y para mirar el estado del
/// sistema a mano con un curl.
pub async fn stats(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let counts = state.storage.count_by_status().await?;

    let by_status: std::collections::HashMap<String, i64> =
        counts.into_iter().map(|c| (c.status, c.count)).collect();

    Ok(Json(serde_json::json!({ "by_status": by_status })))
}

/// Fase 4: combina dos fuentes con roles distintos (ver ADR-002). El
/// registro en PostgreSQL (`workers`, desde la Fase 2) indica quién
/// existió en algún momento; el heartbeat en Redis indica quién está
/// respondiendo en este instante. Ninguna de las dos fuentes responde por
/// sí sola a la pregunta de quién está activo: la tabla no distingue si un
/// worker finalizó hace una hora, y Redis no conserva historial de
/// heartbeats ya expirados.
pub async fn list_workers(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let registered = state.storage.list_workers().await?;
    let alive: std::collections::HashSet<String> =
        state.heartbeats.list_alive().await?.into_iter().collect();

    let workers: Vec<serde_json::Value> = registered
        .into_iter()
        .map(|w| {
            let is_alive = alive.contains(&w.id);
            serde_json::json!({
                "id": w.id,
                "concurrency": w.concurrency,
                "started_at": w.started_at,
                "alive": is_alive,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "workers": workers })))
}

/// Fase 5: crea un cron schedule. La expresión se valida inmediatamente:
/// una expresión inválida devuelve un 400 en el momento de la creación, en
/// lugar de generar un schedule inconsistente que solo fallaría cuando el
/// scheduler intentara utilizarlo por primera vez.
pub async fn create_cron_schedule(
    State(state): State<AppState>,
    Json(new): Json<NewCronSchedule>,
) -> Result<(StatusCode, Json<common::CronSchedule>), ApiError> {
    if new.name.trim().is_empty() {
        return Err(QueueError::InvalidPayload("`name` is required".into()).into());
    }
    if new.job_type.trim().is_empty() {
        return Err(QueueError::InvalidPayload("`type` is required".into()).into());
    }

    let schedule = state.storage.create_cron_schedule(new).await?;

    tracing::info!(
        event = "cron_schedule_created",
        schedule_id = %schedule.id,
        schedule_name = %schedule.name,
        cron_expr = %schedule.cron_expr,
        next_run_at = %schedule.next_run_at,
        "cron schedule created"
    );

    Ok((StatusCode::CREATED, Json(schedule)))
}

pub async fn list_cron_schedules(
    State(state): State<AppState>,
) -> Result<Json<Vec<common::CronSchedule>>, ApiError> {
    Ok(Json(state.storage.list_cron_schedules().await?))
}

pub async fn get_cron_schedule(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<common::CronSchedule>, ApiError> {
    let schedule = state
        .storage
        .get_cron_schedule(id)
        .await?
        .ok_or(QueueError::NotFound(id))?;
    Ok(Json(schedule))
}

pub async fn delete_cron_schedule(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let deleted = state.storage.delete_cron_schedule(id).await?;
    if deleted {
        tracing::info!(event = "cron_schedule_deleted", schedule_id = %id);
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(QueueError::NotFound(id).into())
    }
}

/// Fase 6: métricas en formato de exposición de Prometheus.
///
/// De forma deliberada, no existen contadores acumulados en memoria de
/// proceso en ningún componente, ni en la API ni en el worker. Todo se
/// calcula en el momento de la consulta directamente sobre PostgreSQL, que
/// ya constituye la fuente de verdad del resto del sistema (ADR-001):
/// `job_attempts` cumple el rol del contador monótono (`_total`), y los
/// percentiles de duración se obtienen mediante una agregación SQL
/// (`percentile_cont`) en lugar de un histograma mantenido manualmente.
/// Como consecuencia, reiniciar la API o cualquier worker no implica la
/// pérdida de ninguna métrica, ya que todas están persistidas desde su
/// origen y no se acumulan por separado.
pub async fn metrics(State(state): State<AppState>) -> Result<String, ApiError> {
    let mut out = String::new();

    let by_status = state.storage.count_by_status().await?;
    out.push_str("# HELP queue_depth Number of jobs currently in each status\n");
    out.push_str("# TYPE queue_depth gauge\n");
    for row in &by_status {
        out.push_str(&format!("queue_depth{{status=\"{}\"}} {}\n", row.status, row.count));
    }

    let by_outcome = state.storage.count_attempts_by_outcome().await?;
    out.push_str("# HELP job_attempts_total Total job attempts by outcome\n");
    out.push_str("# TYPE job_attempts_total counter\n");
    for row in &by_outcome {
        out.push_str(&format!("job_attempts_total{{outcome=\"{}\"}} {}\n", row.status, row.count));
    }

    let durations = state.storage.job_duration_percentiles().await?;
    out.push_str("# HELP job_duration_seconds Job execution duration percentiles by job type\n");
    out.push_str("# TYPE job_duration_seconds gauge\n");
    for row in &durations {
        if let Some(p50) = row.p50_seconds {
            out.push_str(&format!(
                "job_duration_seconds{{job_type=\"{}\",quantile=\"0.5\"}} {p50:.4}\n",
                row.job_type
            ));
        }
        if let Some(p95) = row.p95_seconds {
            out.push_str(&format!(
                "job_duration_seconds{{job_type=\"{}\",quantile=\"0.95\"}} {p95:.4}\n",
                row.job_type
            ));
        }
    }
    out.push_str("# HELP job_duration_samples_total Sample count backing job_duration_seconds\n");
    out.push_str("# TYPE job_duration_samples_total counter\n");
    for row in &durations {
        out.push_str(&format!(
            "job_duration_samples_total{{job_type=\"{}\"}} {}\n",
            row.job_type, row.sample_count
        ));
    }

    let registered = state.storage.list_workers().await?;
    let alive_count = state.heartbeats.list_alive().await?.len();
    out.push_str("# HELP workers_registered Workers that have registered at some point\n");
    out.push_str("# TYPE workers_registered gauge\n");
    out.push_str(&format!("workers_registered {}\n", registered.len()));
    out.push_str("# HELP workers_alive Workers currently sending heartbeats\n");
    out.push_str("# TYPE workers_alive gauge\n");
    out.push_str(&format!("workers_alive {alive_count}\n"));

    Ok(out)
}

/// Fase 6: dashboard web mínimo. Consiste en HTML y JavaScript estáticos,
/// sin paso de build ni dependencias nuevas, que realizan polling sobre
/// los endpoints existentes (`/stats`, `/workers`, `/jobs`) desde el
/// navegador. Para un proyecto de este tamaño, introducir un framework de
/// frontend representaría más superficie de mantenimiento que valor real.
pub async fn dashboard() -> impl IntoResponse {
    axum::response::Html(include_str!("../static/dashboard.html"))
}
