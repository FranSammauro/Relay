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

    // Cantidad de jobs que este worker ejecuta en paralelo. Es un valor
    // fijo por configuración desde la Fase 2; no existe auto-scaling ni
    // ajuste dinámico, ya que ese problema no está planteado en el
    // alcance actual del proyecto.
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

    // Fase 6: canal de shutdown ordenado. Una única señal (SIGTERM o
    // Ctrl+C) notifica a todas las tareas que corresponde iniciar el
    // cierre en lugar de interrumpir abruptamente. El ciclo principal deja
    // de reclamar jobs nuevos, y a continuación se espera a que los que ya
    // estaban en ejecución finalicen antes de terminar el proceso.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        common::shutdown::signal().await;
        tracing::info!(event = "shutdown_signal_received", "received shutdown signal, winding down");
        let _ = shutdown_tx.send(true);
    });

    // Fase 4: heartbeat propio, ejecutado en su propia tarea de fondo.
    // Se emite cada heartbeat_interval con un TTL igual a tres veces ese
    // intervalo en Redis. Si el worker deja de emitir el heartbeat (se
    // cuelga, finaliza, o pierde conectividad de red), la clave expira
    // automáticamente sin que sea necesario limpiarla manualmente (ver
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

    // Fase 4: el reaper. Se ejecuta simultáneamente en todos los workers
    // de forma deliberada (ver ADR-004), de modo que la recuperación de
    // jobs abandonados no dependa de que un único coordinador permanezca
    // activo. El costo es cierto polling redundante entre workers; a esta
    // escala resulta despreciable frente al beneficio de eliminar un
    // punto único de falla para la recuperación.
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

    // Fase 5: scheduler de cron, con exclusión mutua mediante advisory
    // lock de PostgreSQL (ver ADR-006). Cada worker intenta convertirse en
    // líder; el que lo consigue ejecuta el ciclo de disparo de los
    // schedules vencidos, mientras el resto reintenta cada
    // scheduler_interval por si el líder actual deja de estar disponible.
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

    // Fase 2: ciclo de claim que dispara ejecuciones en paralelo,
    // utilizando un semáforo como único mecanismo de control de
    // concurrencia. El flujo es intencionalmente simple: se solicita un
    // permiso, se reclama un job, se lanza en su propia tarea, y se
    // continúa solicitando mientras haya permisos disponibles. El permiso
    // se libera únicamente cuando el job finaliza (el `Arc<Semaphore>`
    // clonado dentro de la tarea es responsable de esa liberación).
    let semaphore = Arc::new(Semaphore::new(concurrency));

    loop {
        // acquire_owned permite mover el permiso dentro del spawn sin
        // conflictos de lifetimes. Si no hay permisos disponibles, la
        // ejecución permanece a la espera, que es el comportamiento
        // deseado: no se reclama más trabajo del que el worker puede
        // procesar. El select! agregado en la Fase 6 es la única
        // modificación sobre este comportamiento: si llega la señal de
        // shutdown mientras se espera un permiso libre, se interrumpe el
        // ciclo en lugar de continuar solicitando trabajo nuevo.
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
                // No había nada para reclamar; se devuelve el permiso y se
                // espera poll_interval, como en la Fase 1, salvo que
                // llegue la señal de shutdown mientras tanto, en cuyo caso
                // no corresponde seguir esperando para repetir la misma
                // consulta.
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

    // Ya no se reclaman jobs nuevos. Resta evitar que los jobs que ya
    // estaban en ejecución queden interrumpidos a mitad de camino: se
    // espera a que se liberen todos los permisos del semáforo (cada tarea
    // de job libera el suyo al finalizar), con un límite de tiempo
    // definido por `SHUTDOWN_GRACE_SECONDS` por si alguno queda colgado.
    // Si el plazo se cumple, el proceso finaliza de todas formas: esos
    // jobs quedarán con el lease activo y serán recuperados por el reaper
    // de cualquier otro worker (Fase 4, ADR-004). El graceful shutdown no
    // constituye una garantía de espera indefinida, sino un mecanismo para
    // evitar el caso común de interrumpir un job sin necesidad.
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

    // Fase 3: el handler se ejecuta bajo un timeout estricto. Si se
    // excede, se trata como un fallo adicional, que cuenta contra
    // max_attempts igual que cualquier otra excepción, pero se distingue
    // en job_attempts con el valor 'timeout', ya que diagnosticar un job
    // colgado requiere un análisis distinto al de uno que falló de
    // inmediato.
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

    // Se emite un único evento para ambos desenlaces, distinguido por
    // result_status. De esta forma, una búsqueda de "job_attempt_failed"
    // en los logs proporciona el cuadro completo sin necesidad de conocer
    // dos nombres de evento distintos.
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

/// Intenta convertirse en líder del scheduler de cron. Si lo consigue,
/// ejecuta el ciclo de disparo hasta perder el lock, ya sea porque la
/// conexión se interrumpe o porque el proceso finaliza. Si no lo
/// consigue, simplemente reintenta más adelante: se trata de polling de
/// bajo costo, dado que `pg_try_advisory_lock` es no bloqueante y no
/// compite por filas ni índices.
async fn run_scheduler_loop(storage: Storage, worker_id: &str, interval: Duration) {
    loop {
        match storage.try_become_scheduler_leader().await {
            Ok(Some(leader_conn)) => {
                tracing::info!(
                    event = "scheduler_leader_acquired",
                    worker_id = %worker_id,
                    "acquired cron scheduler leadership"
                );
                // Se mantiene la conexión activa mientras dure el
                // liderazgo, ya que el lock es de sesión y se libera
                // únicamente al soltar esta conexión en particular. Si la
                // conexión se interrumpe, se sale del ciclo interno y se
                // reintenta el liderazgo en la iteración superior.
                run_as_leader(&storage, worker_id, interval, leader_conn).await;
                tracing::warn!(
                    event = "scheduler_leader_lost",
                    worker_id = %worker_id,
                    "lost cron scheduler leadership, will retry"
                );
            }
            Ok(None) => {
                // Otro worker ya ostenta el liderazgo; no hay ninguna acción que realizar.
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
        // Se ejecuta un SELECT 1 trivial sobre la conexión que sostiene el
        // advisory lock: si la conexión con PostgreSQL se interrumpió,
        // esta consulta falla, y se libera el liderazgo en lugar de
        // continuar actuando como líder sin una conexión válida.
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
