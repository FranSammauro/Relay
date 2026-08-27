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

    // Fase 6: canal de shutdown gracioso. Una sola señal (SIGTERM o
    // Ctrl+C) avisa a todo lo que le importa que hay que empezar a
    // terminar en vez de cortar en seco -- el loop principal deja de
    // reclamar jobs nuevos, y más abajo esperamos a que los que ya están
    // en vuelo terminen antes de salir del proceso.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        common::shutdown::signal().await;
        tracing::info!(event = "shutdown_signal_received", "received shutdown signal, winding down");
        let _ = shutdown_tx.send(true);
    });

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

    // Fase 5: scheduler de cron, con exclusión mutua vía advisory lock de
    // Postgres (ver ADR-006). Cada worker intenta ser el líder; el que lo
    // consigue corre el loop de "disparar lo que esté vencido"; el resto
    // reintenta cada scheduler_interval por si el líder actual se cae.
    let scheduler_interval = Duration::from_millis(
        std::env::var("SCHEDULER_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000),
    );
    {
        let storage = storage.clone();
        let worker_id = worker_id.clone();
        tokio::spawn(async move {
            run_scheduler_loop(storage, &worker_id, scheduler_interval).await;
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
        // no reclamar más trabajo del que podemos correr. El select! de
        // acá afuera es lo único nuevo en Fase 6: si llega la señal de
        // shutdown mientras esperamos un permiso libre, cortamos el loop
        // en vez de seguir pidiendo trabajo nuevo.
        let permit = tokio::select! {
            _ = shutdown_rx.changed() => break,
            permit = semaphore.clone().acquire_owned() => permit?,
        };

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
                // esperamos el poll_interval como en Fase 1 -- salvo que
                // llegue el shutdown mientras tanto, en cuyo caso no tiene
                // sentido seguir esperando para volver a preguntar lo mismo.
                drop(permit);
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            }
            Err(e) => {
                drop(permit);
                tracing::error!(event = "claim_error", error = %e, "failed to claim job, backing off");
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = tokio::time::sleep(poll_interval * 2) => {}
                }
            }
        }
    }

    // Ya no reclamamos jobs nuevos. Lo único que falta es no dejar tirados
    // a medio terminar los que ya estaban en vuelo -- esperamos a que se
    // liberen todos los permisos del semáforo (cada tarea de job los
    // suelta al terminar), con un techo de `SHUTDOWN_GRACE_SECONDS` por si
    // alguno se está colgando. Si el plazo se cumple igual salimos: esos
    // jobs van a quedar con el lease corriendo y el reaper de cualquier
    // otro worker los va a recuperar solo (Fase 4, ADR-004) -- graceful
    // shutdown no es una promesa de esperar para siempre, es evitar el
    // caso común de matar un job a mitad de camino sin necesidad.
    let shutdown_grace = Duration::from_secs(
        std::env::var("SHUTDOWN_GRACE_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30),
    );

    tracing::info!(
        event = "worker_draining",
        worker_id = %worker_id,
        grace_seconds = shutdown_grace.as_secs(),
        "no longer claiming new jobs, waiting for in-flight jobs to finish"
    );

    match tokio::time::timeout(shutdown_grace, semaphore.acquire_many(concurrency as u32)).await {
        Ok(Ok(_permits)) => {
            tracing::info!(event = "worker_drained", worker_id = %worker_id, "all in-flight jobs finished");
        }
        Ok(Err(e)) => {
            tracing::error!(event = "worker_drain_error", worker_id = %worker_id, error = %e, "semaphore closed unexpectedly during drain");
        }
        Err(_elapsed) => {
            tracing::warn!(
                event = "worker_drain_timeout",
                worker_id = %worker_id,
                "shutdown grace period elapsed with jobs still in flight; they'll be recovered via lease expiry"
            );
        }
    }

    if let Err(e) = heartbeats.forget(&worker_id).await {
        tracing::warn!(event = "heartbeat_forget_failed", error = %e, "failed to clear heartbeat on shutdown");
    }

    tracing::info!(event = "worker_stopped", worker_id = %worker_id, "worker shut down gracefully");
    Ok(())
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

/// Intenta ser el líder del scheduler de cron; si lo consigue, corre el
/// loop de disparo hasta perder el lock (la conexión se cae, el proceso se
/// muere, lo que sea). Si no lo consigue, simplemente reintenta más tarde
/// -- esto es polling barato: `pg_try_advisory_lock` es no bloqueante y no
/// pelea con nadie por filas ni índices.
async fn run_scheduler_loop(storage: Storage, worker_id: &str, interval: Duration) {
    loop {
        match storage.try_become_scheduler_leader().await {
            Ok(Some(leader_conn)) => {
                tracing::info!(
                    event = "scheduler_leader_acquired",
                    worker_id = %worker_id,
                    "acquired cron scheduler leadership"
                );
                // mantenemos la conexión viva (el lock es de sesión, se
                // libera solo si soltamos esta conexión puntual) mientras
                // dure el liderazgo. Si algo la tira abajo, salimos del
                // loop interno y volvemos a intentar ser líder más arriba.
                run_as_leader(&storage, worker_id, interval, leader_conn).await;
                tracing::warn!(
                    event = "scheduler_leader_lost",
                    worker_id = %worker_id,
                    "lost cron scheduler leadership, will retry"
                );
            }
            Ok(None) => {
                // otro worker ya es líder, no hay nada para hacer.
            }
            Err(e) => {
                tracing::error!(event = "scheduler_leader_check_failed", error = %e, "failed to check scheduler leadership");
            }
        }
        tokio::time::sleep(interval).await;
    }
}

async fn run_as_leader(
    storage: &Storage,
    worker_id: &str,
    interval: Duration,
    mut leader_conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
) {
    loop {
        // Un SELECT 1 trivial sobre la conexión que sostiene el advisory
        // lock: si el link con Postgres se cortó, esto falla y soltamos el
        // liderazgo en vez de seguir actuando como líder a ciegas.
        if sqlx::query("SELECT 1").execute(&mut *leader_conn).await.is_err() {
            return;
        }

        match storage.due_cron_schedules().await {
            Ok(due) => {
                for schedule in due {
                    match storage.fire_cron_schedule(&schedule).await {
                        Ok(job) => {
                            tracing::info!(
                                event = "cron_fired",
                                schedule_id = %schedule.id,
                                schedule_name = %schedule.name,
                                job_id = %job.id,
                                worker_id = %worker_id,
                                "cron schedule fired"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                event = "cron_fire_error",
                                schedule_id = %schedule.id,
                                schedule_name = %schedule.name,
                                error = %e,
                                "failed to fire cron schedule"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!(event = "cron_scan_error", error = %e, "failed to scan due cron schedules");
            }
        }

        tokio::time::sleep(interval).await;
    }
}
