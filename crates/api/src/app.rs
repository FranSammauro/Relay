//! Composición del router: rutas públicas por un lado y rutas protegidas
//! (autenticación + autorización + rate limiting) por el otro.
//!
//! Ver `auth` para el detalle de las capas y de por qué la autorización es
//! una capa única con tabla de grants en lugar de guardas por sub-router.

use axum::routing::{delete, get, post};
use axum::{middleware, Router};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::auth;
use crate::handlers;
use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    // Rutas públicas: sin autenticación (health, ready y el dashboard HTML).
    let public = Router::new()
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/", get(handlers::dashboard));

    // Rutas protegidas: todas las operativas. Las capas se aplican con
    // `.layer()`; como el último `.layer` es el más externo, se aplican en
    // orden inverso al deseado:
    //   1. rate limit  (primera en `.layer`, queda la más interna)
    //   2. autorización
    //   3. auth        (última en `.layer`, queda la más externa)
    //
    // Flujo entrante: auth resuelve la key y cachea el AuthContext -> la
    // autorización decide con la tabla de grants -> el rate limit cuenta la
    // key -> handler.
    let protected = protected_routes()
        .layer(middleware::from_fn_with_state(state.clone(), auth::rate_limit_middleware))
        .layer(axum::middleware::from_fn(auth::authorize_middleware))
        .layer(middleware::from_fn_with_state(state.clone(), auth::auth_middleware));

    public
        .merge(protected)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn protected_routes() -> Router<AppState> {
    Router::new()
        .route("/jobs", get(handlers::list_jobs))
        .route("/jobs", post(handlers::create_job))
        .route("/jobs/:id", get(handlers::get_job))
        .route("/jobs/:id", delete(handlers::cancel_job))
        .route("/jobs/:id/attempts", get(handlers::get_job_attempts))
        .route("/stats", get(handlers::stats))
        .route("/metrics", get(handlers::metrics))
        .route("/workers", get(handlers::list_workers))
        .route("/cron", post(handlers::create_cron_schedule))
        .route("/cron", get(handlers::list_cron_schedules))
        .route("/cron/:id", get(handlers::get_cron_schedule))
        .route("/cron/:id", delete(handlers::delete_cron_schedule))
}