//! Test de concurrencia de Fase 2: 100 jobs, 10 "workers" compitiendo por
//! ellos al mismo tiempo, contra un Postgres real. Esto no se puede simular
//! con un mock -- lo que estamos probando es justamente que SKIP LOCKED se
//! porta bien bajo contención real, así que necesita una base de verdad.
//!
//! Si no hay Postgres disponible (por ejemplo corriendo `cargo test` en la
//! máquina de alguien sin Docker levantado), el test se salta con un aviso
//! en vez de romper toda la suite. En CI corre siempre contra el servicio
//! de postgres de GitHub Actions (ver .github/workflows/ci.yml).
//!
//! Ojo con esto: `claim_next_job` reclama CUALQUIER job pendiente de la
//! tabla, no solo los de este test -- es el comportamiento real y correcto
//! para producción, así que no lo vamos a filtrar acá. Eso significa que si
//! corrés esto contra una base con basura de otra corrida (otro test en
//! paralelo, una prueba manual que dejaste a medio terminar), los workers
//! de este test también van a agarrar esos jobs ajenos. Los completamos
//! igual (para no dejarlos colgados en `running`), pero solo contamos y
//! validamos los que llevan la marca de esta corrida.

use std::collections::HashSet;
use std::sync::Arc;

use common::{JobStatus, NewJob};

mod support;

const TOTAL_JOBS: usize = 100;
const WORKER_COUNT: usize = 10;

#[tokio::test]
async fn hundred_jobs_ten_workers_none_lost_none_duplicated() {
    let Some(storage) = support::connect_or_skip(&support::database_url()).await else {
        return;
    };

    // marca única por corrida para distinguir "mis jobs" de basura ajena
    // que pueda andar dando vueltas en una base compartida/sucia.
    let run_marker = format!("concurrency-test-{}", uuid::Uuid::new_v4());

    for i in 0..TOTAL_JOBS {
        storage
            .create_job(NewJob {
                job_type: run_marker.clone(),
                payload: serde_json::json!({ "n": i }),
                priority: 50,
                max_attempts: 5,
                timeout_seconds: 30,
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
        let run_marker = run_marker.clone();

        handles.push(tokio::spawn(async move {
            let mut claimed = Vec::new();
            loop {
                match storage.claim_next_job(&worker_id).await {
                    Ok(Some(job)) => {
                        let belongs_to_this_run = job.job_type == run_marker;
                        storage
                            .mark_completed(job.id)
                            .await
                            .expect("mark_completed no debería fallar");
                        // jobs ajenos (basura de otra corrida) se completan
                        // igual para no dejarlos colgados, pero no cuentan
                        // para las validaciones de este test.
                        if belongs_to_this_run {
                            claimed.push(job.id);
                        }
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
