mod handlers;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use common::{AttemptOutcome, Heartbeats, Job, Storage};

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

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let heartbeat_interval = Duration::from_millis(
        std::env::var("HEARTBEAT_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000),
    );
    let reaper_interval = Duration::from_millis(
        std::env::var("REAPER_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(15_000),
    );

    let heartbeats = Heartbeats::connect(&redis_url).await?;

    tracing::info!(
        event = "worker_starting",
        worker_id = %worker_id,
        concurrency,
        redis_url = %redis_url,
        "starting worker"
    );

    // Fase 4: heartbeat propio, corriendo en su propia tarea de fondo. Late
    // cada heartbeat_interval con un TTL de 3x ese intervalo en Redis -- si
    // el worker deja de latir (se cuelga, muere, pierde la red), la clave
    // expira sola sin que nadie tenga que barrerla a mano (ver
    // common::heartbeats y ADR-002).
    {
        let heartbeats = heartbeats.clone();
        let worker_id = worker_id.clone();
        tokio::spawn(async move {
            let ttl_seconds = (heartbeat_interval.as_secs() * 3).max(1);
            loop {
                if let Err(e) = heartbeats.beat(&worker_id, concurrency as i32, ttl_seconds).await {
                    tracing::warn!(event = "heartbeat_failed", error = %e, "failed to send heartbeat");
                }
                tokio::time::sleep(heartbeat_interval).await;
            }
        });
    }

    // Fase 4: el reaper. Corre en todos los workers a la vez a propósito
    // (ver ADR-004) -- así la recuperación de jobs abandonados no depende
    // de que un único "coordinador" siga vivo. El costo es un poco de
    // polling redundante entre workers; a esta escala es gratis comparado
    // con el beneficio de no tener un punto único de falla para el recovery.
    {
        let storage = storage.clone();
        tokio::spawn(async move {
            loop {
                match storage.reap_expired_leases().await {
                    Ok(reaped) if !reaped.is_empty() => {
                        for (job_id, status) in &reaped {
                            tracing::warn!(
                                event = "job_lease_expired",
                                job_id = %job_id,
                                result_status = %status,
                                "recovered job with expired lease (worker probably crashed)"
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(event = "reaper_error", error = %e, "failed to reap expired leases");
                    }
                }
                tokio::time::sleep(reaper_interval).await;
            }
        });
    }

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
        timeout_seconds = job.timeout_seconds,
        "job execution started"
    );

    // Fase 3: el handler corre bajo un timeout duro. Si se pasa, lo tratamos
    // como un fallo más (cuenta contra max_attempts igual que una excepción),
    // pero lo distinguimos en job_attempts como 'timeout' porque diagnosticar
    // un job colgado es un problema distinto a uno que explotó rápido.
    let timeout = Duration::from_secs(job.timeout_seconds.max(1) as u64);
    let outcome = tokio::time::timeout(timeout, handlers::execute(&job.job_type, &job.payload)).await;
    let duration_ms = started.elapsed().as_millis();

    match outcome {
        Ok(Ok(())) => {
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
        Ok(Err(err)) => {
            record_failure(storage, worker_id, &job, duration_ms, &err, AttemptOutcome::Failed).await;
        }
        Err(_elapsed) => {
            let err = format!("job timed out after {}s", job.timeout_seconds);
            record_failure(storage, worker_id, &job, duration_ms, &err, AttemptOutcome::TimedOut).await;
        }
    }
}

async fn record_failure(
    storage: &Storage,
    worker_id: &str,
    job: &Job,
    duration_ms: u128,
    err: &str,
    outcome: AttemptOutcome,
) {
    let result_status = match storage.record_failure(job.id, err, outcome).await {
        Ok(status) => status,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "failed to persist job failure");
            return;
        }
    };

    // Un solo evento para ambos desenlaces, distinguido por result_status --
    // así un grep de "job_attempt_failed" te da el cuadro completo sin tener
    // que acordarte de dos nombres de evento distintos.
    tracing::warn!(
        event = "job_attempt_failed",
        job_id = %job.id,
        job_type = %job.job_type,
        worker_id = %worker_id,
        duration_ms,
        attempt = job.attempts,
        max_attempts = job.max_attempts,
        outcome = outcome.as_str(),
        result_status = %result_status,
        error = %err,
        "job attempt failed"
    );
}
