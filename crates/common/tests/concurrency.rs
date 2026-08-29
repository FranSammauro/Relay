//! Test de concurrencia de la Fase 2: 100 jobs y 10 workers simulados
//! compitiendo por ellos simultáneamente, contra un PostgreSQL real. No es
//! posible simular esto con un mock, ya que lo que se está verificando es
//! precisamente el comportamiento de SKIP LOCKED bajo contención real, lo
//! cual requiere una base de datos genuina.
//!
//! Si no hay PostgreSQL disponible (por ejemplo, al ejecutar `cargo test`
//! en una máquina sin Docker levantado), el test se omite con un aviso en
//! lugar de interrumpir toda la suite. En CI se ejecuta siempre, contra el
//! servicio de postgres de GitHub Actions (ver .github/workflows/ci.yml).
//!
//! Es importante notar que `claim_next_job` reclama cualquier job
//! pendiente de la tabla, no únicamente los creados por este test. Este es
//! el comportamiento correcto para producción, por lo que no se filtra
//! aquí. Esto implica que, si el test se ejecuta contra una base con datos
//! remanentes de otra corrida (otro test en paralelo, o una prueba manual
//! interrumpida), los workers de este test también reclamarán esos jobs
//! ajenos. Dichos jobs se completan de todas formas, para no dejarlos
//! pendientes en estado `running`, pero solo se cuentan y validan los que
//! corresponden a esta corrida.

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

    // Marca única por corrida para distinguir los jobs propios de datos
    // remanentes de otra ejecución en una base compartida o sin limpiar.
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
                        // Los jobs ajenos (datos remanentes de otra
                        // corrida) se completan de todas formas, para no
                        // dejarlos pendientes, pero no se incluyen en las
                        // validaciones de este test.
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
        all_claimed.extend(h.await.expect("la tarea del worker no debería entrar en pánico"));
    }

    // Ningún job debería haberse reclamado dos veces. Si esta aserción
    // falla, SKIP LOCKED dejó de cumplir su garantía y corresponde revisar
    // la consulta de claim.
    let unique: HashSet<_> = all_claimed.iter().collect();
    assert_eq!(
        unique.len(),
        all_claimed.len(),
        "se detectaron jobs reclamados más de una vez"
    );

    // Tampoco debería haberse perdido ninguno.
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
        "no todos los jobs de esta corrida terminaron en estado completed"
    );
}
