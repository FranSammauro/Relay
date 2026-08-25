use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use common::{NewJob, QueueError};

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
                // Redis acá es coordinación efímera (ver ADR-002), no fuente
                // de verdad -- si está caído, el endpoint que lo necesita no
                // puede contestar bien, pero no es un 500 de "algo se rompió
                // adentro", es un 503 de "una dependencia externa no está".
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

/// Fase 3: historial de intentos de un job (uno por cada vez que un worker
/// lo agarró). Útil para responder "¿por qué falló esto tres veces?" sin
/// tener que grepear logs de tres workers distintos.
pub async fn get_job_attempts(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<common::JobAttempt>>, ApiError> {
    // si el job no existe directamente devolvemos 404 en vez de una lista
    // vacía silenciosa -- es más útil para debuggear un id mal copiado.
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

/// Fase 4: junta dos fuentes con roles distintos (ver ADR-002). El registro
/// en Postgres (`workers`, desde Fase 2) dice quién existió alguna vez; el
/// heartbeat en Redis dice quién está respondiendo ahora mismo. Ninguna de
/// las dos por sí sola contesta "¿quién está vivo?" -- la tabla no sabe si
/// un worker murió hace una hora, y Redis no tiene historial de nadie que
/// ya expiró.
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
