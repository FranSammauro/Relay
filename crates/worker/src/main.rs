mod handlers;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use common::{Job, Storage};

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

    let worker_id = std::env::var("WORKER_ID").unwrap_or_else(|_| format!("worker-{}", Uuid::new_v4()));
    let poll_interval = Duration::from_millis(
        std::env::var("POLL_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500),
    );

    // Cuantos jobs corre este worker en paralelo. Fijo por config en Fase 2 --
    // nada de auto-scaling ni tuning dinámico todavía, eso sería resolver un
    // problema que ni siquiera tenemos planteado bien.
    let concurrency: usize = std::env::var("CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);

    storage.register_worker(&worker_id, concurrency as i32).await?;

    tracing::info!(
        event = "worker_starting",
        worker_id = %worker_id,
        concurrency,
        "starting worker"
    );

    // Fase 2: loop de claim que dispara ejecuciones en paralelo, con un
    // semáforo como único freno de mano. La idea es simple a propósito:
    // pedimos un permiso, reclamamos un job, lo largamos en su propia
    // tarea, y seguimos pidiendo mientras haya permisos libres. El permiso
    // se libera solo cuando el job termina (el `Arc<Semaphore>` clonado
    // dentro de la tarea hace ese trabajo sucio).
    let semaphore = Arc::new(Semaphore::new(concurrency));

    loop {
        // acquire_owned nos deja mover el permiso adentro del spawn sin
        // pelearnos con lifetimes -- si no está disponible, se queda acá
        // esperando, que es exactamente el comportamiento que queremos:
        // no reclamar más trabajo del que podemos correr.
        let permit = semaphore.clone().acquire_owned().await?;

        match storage.claim_next_job(&worker_id).await {
            Ok(Some(job)) => {
                let storage = storage.clone();
                let worker_id = worker_id.clone();
                tokio::spawn(async move {
                    run_job(&storage, &worker_id, job).await;
                    drop(permit);
                });
            }
            Ok(None) => {
                // no había nada para reclamar, devolvemos el permiso y
                // esperamos el poll_interval como en Fase 1.
                drop(permit);
                tokio::time::sleep(poll_interval).await;
            }
            Err(e) => {
                drop(permit);
                tracing::error!(event = "claim_error", error = %e, "failed to claim job, backing off");
                tokio::time::sleep(poll_interval * 2).await;
            }
        }
    }
}

async fn run_job(storage: &Storage, worker_id: &str, job: Job) {
    let started = std::time::Instant::now();

    tracing::info!(
        event = "job_started",
        job_id = %job.id,
        job_type = %job.job_type,
        worker_id = %worker_id,
        attempt = job.attempts,
        "job execution started"
    );

    let result = handlers::execute(&job.job_type, &job.payload).await;
    let duration_ms = started.elapsed().as_millis();

    match result {
        Ok(()) => {
            if let Err(e) = storage.mark_completed(job.id).await {
                tracing::error!(job_id = %job.id, error = %e, "failed to persist job completion");
                return;
            }
            tracing::info!(
                event = "job_completed",
                job_id = %job.id,
                job_type = %job.job_type,
                worker_id = %worker_id,
                duration_ms,
                attempt = job.attempts,
                "job completed"
            );
        }
        Err(err) => {
            if let Err(e) = storage.mark_failed(job.id, &err).await {
                tracing::error!(job_id = %job.id, error = %e, "failed to persist job failure");
                return;
            }
            tracing::warn!(
                event = "job_failed",
                job_id = %job.id,
                job_type = %job.job_type,
                worker_id = %worker_id,
                duration_ms,
                attempt = job.attempts,
                error = %err,
                "job failed (retries/backoff/DLQ arrive in Fase 3)"
            );
        }
    }
}
