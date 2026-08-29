mod handlers;
mod state;

use axum::routing::{delete, get, post};
use axum::Router;
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use common::Storage;
use state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .json()
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://queue:queue@localhost:5432/queue".to_string());

    let storage = Storage::connect(&database_url).await?;
    storage.migrate().await?;
    tracing::info!(event = "migrations_applied", "database ready");

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let heartbeats = common::Heartbeats::connect(&redis_url).await?;

    let state = AppState { storage, heartbeats };

    let app = Router::new()
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/metrics", get(handlers::metrics))
        .route("/", get(handlers::dashboard))
        .route("/jobs", post(handlers::create_job))
        .route("/jobs", get(handlers::list_jobs))
        .route("/jobs/:id", get(handlers::get_job))
        .route("/jobs/:id/attempts", get(handlers::get_job_attempts))
        .route("/jobs/:id", delete(handlers::cancel_job))
        .route("/stats", get(handlers::stats))
        .route("/workers", get(handlers::list_workers))
        .route("/cron", post(handlers::create_cron_schedule))
        .route("/cron", get(handlers::list_cron_schedules))
        .route("/cron/:id", get(handlers::get_cron_schedule))
        .route("/cron/:id", delete(handlers::delete_cron_schedule))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr: SocketAddr = std::env::var("API_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;

    tracing::info!(event = "api_starting", %addr, "starting job queue API");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    // Fase 6: graceful shutdown. Las requests en curso finalizan antes de
    // cerrar el servidor, en lugar de interrumpir la conexión a mitad de
    // una respuesta cuando llega SIGTERM (la señal que Docker o
    // Kubernetes envían al detener el contenedor).
    axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown::signal())
        .await?;
    tracing::info!(event = "api_stopped", "API shut down gracefully");

    Ok(())
}
