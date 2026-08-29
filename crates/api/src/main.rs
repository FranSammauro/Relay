use api::{app, state::AppState};
use common::{RateLimits, Storage};
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

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

    let rate_limiter = common::RateLimiter::connect(&redis_url, RateLimits::from_env()).await?;
    tracing::info!(
        event = "rate_limits_loaded",
        producer = %rate_limiter.limits().producer_per_minute,
        worker = %rate_limiter.limits().worker_per_minute,
        admin = %rate_limiter.limits().admin_per_minute,
        "rate limiting ready"
    );

    let state = AppState { storage, heartbeats, rate_limiter };

    let app = app::router(state);

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