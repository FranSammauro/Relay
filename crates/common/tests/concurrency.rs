//! Test de concurrencia de Fase 2: 100 jobs, 10 "workers" compitiendo por
//! ellos al mismo tiempo, contra un Postgres real. Esto no se puede simular
//! con un mock -- lo que estamos probando es justamente que SKIP LOCKED se
//! porta bien bajo contención real, así que necesita una base de verdad.
//!
//! Si no hay Postgres disponible (por ejemplo corriendo `cargo test` en la
//! máquina de alguien sin Docker levantado), el test se salta con un aviso
//! en vez de romper toda la suite. En CI corre siempre contra el servicio
//! de postgres de GitHub Actions (ver .github/workflows/ci.yml).

use std::collections::HashSet;
use std::sync::Arc;

use common::{JobStatus, NewJob, Storage};

const TOTAL_JOBS: usize = 100;
const WORKER_COUNT: usize = 10;

#[tokio::test]
async fn hundred_jobs_ten_workers_none_lost_none_duplicated() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://queue:queue@localhost:5432/queue".to_string());

    let storage = match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        Storage::connect(&database_url),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            eprintln!(
                "saltando test de concurrencia: no hay Postgres en {database_url} ({e}). \
                 Levantá `docker compose up -d postgres` y corré `cargo test` de nuevo."
            );
            return;
        }
        Err(_) => {
            eprintln!(
                "saltando test de concurrencia: timeout conectando a {database_url}. \
                 Levantá `docker compose up -d postgres` y corré `cargo test` de nuevo."
            );
            return;
        }
    };

    storage.migrate().await.expect("migraciones deberían aplicar limpio");

    // marca única por corrida para no chocar con datos de otras corridas
    // si alguien apunta esto a una base compartida por error.
    let run_marker = format!("concurrency-test-{}", uuid::Uuid::new_v4());

    for i in 0..TOTAL_JOBS {
        storage
            .create_job(NewJob {
                job_type: run_marker.clone(),
                payload: serde_json::json!({ "n": i }),
                priority: 50,
                max_attempts: 5,
                scheduled_at: None,
                idempotency_key: None,
            })
            .await
            .expect("create_job no debería fallar");
    }

    let storage = Arc::new(storage);
    let mut handles = Vec::with_capacity(WORKER_COUNT);

    for w in 0..WORKER_COUNT {
        let storage = storage.clone();
        let worker_id = format!("test-worker-{w}");

        handles.push(tokio::spawn(async move {
            let mut claimed = Vec::new();
            loop {
                match storage.claim_next_job(&worker_id).await {
                    Ok(Some(job)) => {
                        storage
                            .mark_completed(job.id)
                            .await
                            .expect("mark_completed no debería fallar");
                        claimed.push(job.id);
                    }
                    Ok(None) => break,
                    Err(e) => panic!("claim_next_job falló: {e}"),
                }
            }
            claimed
        }));
    }

    let mut all_claimed = Vec::new();
    for h in handles {
        all_claimed.extend(h.await.expect("la tarea del worker no debería paniquear"));
    }

    // ningún job se claimeó dos veces -- si esto falla, SKIP LOCKED dejó
    // de hacer lo que promete y hay que revisar la query de claim.
    let unique: HashSet<_> = all_claimed.iter().collect();
    assert_eq!(
        unique.len(),
        all_claimed.len(),
        "se detectaron jobs reclamados más de una vez"
    );

    // y ninguno se perdió
    assert_eq!(
        all_claimed.len(),
        TOTAL_JOBS,
        "se reclamaron {} jobs pero se crearon {}",
        all_claimed.len(),
        TOTAL_JOBS
    );

    let jobs = storage
        .list_jobs(Some(JobStatus::Completed.as_str()), (TOTAL_JOBS * 2) as i64)
        .await
        .expect("list_jobs no debería fallar");

    let completed_from_this_run = jobs
        .iter()
        .filter(|j| j.job_type == run_marker)
        .count();

    assert_eq!(
        completed_from_this_run, TOTAL_JOBS,
        "no todos los jobs de esta corrida terminaron completed"
    );
}
