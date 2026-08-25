//! Test de recovery (Fase 4): simula un worker que agarra un job y
//! desaparece sin avisar (kill -9, OOM, lo que sea) -- nunca llama a
//! mark_completed ni a record_failure. El lease vence solo, y el reaper
//! tiene que recuperar el job aplicando la misma política de retry/DLQ
//! que un fallo reportado normalmente.
//!
//! A propósito, este test NO usa Redis -- y esa es la validación más
//! importante de ADR-002: la recuperación de jobs abandonados corre
//! enteramente sobre Postgres. Si Redis estuviera en el camino crítico acá,
//! este test no podría existir sin levantar dos servicios en vez de uno.
//!
//! Dos cosas sobre aislamiento, porque `reap_expired_leases` es
//! deliberadamente global (barre TODA la tabla, no un job puntual -- así
//! tiene que ser en producción, un reaper real no sabe de antemano qué
//! buscar):
//!
//! 1. El harness de test corre las funciones `#[tokio::test]` de este
//!    mismo archivo en paralelo por default. Si los dos tests de acá
//!    tienen un lease vencido al mismo tiempo, una sola llamada a
//!    `reap_expired_leases` de cualquiera de los dos puede terminar
//!    barriendo el job del otro también. Por eso las validaciones buscan
//!    "mi job en algún lugar de la lista", no "la lista tiene exactamente
//!    un elemento y es el mío".
//! 2. Al reclamar, si el mismatch de `claimed.id != job.id` explota, hay
//!    basura de otra corrida en la base (ver mismo comentario en
//!    reliability.rs).

use common::NewJob;

mod support;

#[tokio::test]
async fn expired_lease_gets_reaped_and_retried() {
    let Some(storage) = support::connect_or_skip(&support::database_url()).await else {
        return;
    };

    let job = storage
        .create_job(NewJob {
            job_type: format!("recovery-test-{}", uuid::Uuid::new_v4()),
            payload: serde_json::json!({}),
            priority: 50,
            max_attempts: 5,
            timeout_seconds: 30,
            scheduled_at: None,
            idempotency_key: None,
        })
        .await
        .unwrap();

    // "worker-que-va-a-morir" reclama el job y nunca vuelve a aparecer.
    let claimed = storage
        .claim_next_job("worker-que-va-a-morir")
        .await
        .unwrap()
        .expect("debería poder reclamar el job");
    assert_eq!(claimed.id, job.id, "se reclamó un job distinto al de este test");

    assert_eq!(claimed.status, common::JobStatus::Running);
    assert!(claimed.lease_until.is_some(), "claim_next_job debería fijar un lease");
    assert_eq!(claimed.worker_id.as_deref(), Some("worker-que-va-a-morir"));

    // Sin esto habría que esperar timeout_seconds + 30s reales. Pisamos el
    // lease a mano para simular "ya venció" -- la trampa está acotada a
    // una sola columna, no a la lógica que se está probando.
    sqlx::query("UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = $1")
        .bind(job.id)
        .execute(storage.pool())
        .await
        .unwrap();

    // Otro worker, todavía vivo, corre el reaper y encuentra el cadáver.
    // Puede haber barrido algo más si el otro test de este archivo corrió
    // en simultáneo -- por eso buscamos nuestro job puntual en la lista en
    // vez de asumir que fue lo único que se recuperó (ver comentario del
    // módulo).
    let reaped = wait_until_reaped(&storage, job.id).await;
    assert_eq!(reaped, "retry_scheduled");

    let after = storage.get_job(job.id).await.unwrap().unwrap();
    assert_eq!(after.status, common::JobStatus::RetryScheduled);
    assert_eq!(after.attempts, 1);
    // El job queda "libre" -- nadie lo posee hasta que alguien lo reclame de nuevo.
    assert!(after.worker_id.is_none());
    assert!(after.lease_until.is_none());
    assert!(after.last_error.as_deref().unwrap().contains("lease expired"));

    // El historial de intentos debería reflejar el intento perdido.
    let attempts = storage.list_attempts(job.id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status.as_deref(), Some("lease_expired"));
}

#[tokio::test]
async fn expired_lease_past_max_attempts_goes_to_dead_letter() {
    let Some(storage) = support::connect_or_skip(&support::database_url()).await else {
        return;
    };

    let job = storage
        .create_job(NewJob {
            job_type: format!("recovery-dlq-test-{}", uuid::Uuid::new_v4()),
            payload: serde_json::json!({}),
            priority: 50,
            max_attempts: 1,
            timeout_seconds: 30,
            scheduled_at: None,
            idempotency_key: None,
        })
        .await
        .unwrap();

    let claimed = storage
        .claim_next_job("worker-que-va-a-morir-2")
        .await
        .unwrap()
        .expect("debería poder reclamar el job");
    assert_eq!(claimed.id, job.id, "se reclamó un job distinto al de este test");

    sqlx::query("UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = $1")
        .bind(job.id)
        .execute(storage.pool())
        .await
        .unwrap();

    let reaped = wait_until_reaped(&storage, job.id).await;
    assert_eq!(reaped, "dead_letter");

    let after = storage.get_job(job.id).await.unwrap().unwrap();
    assert_eq!(after.status, common::JobStatus::DeadLetter);
}

/// Dispara el reaper y espera a que nuestro job puntual salga de `running`
/// (lo haya recuperado nuestra propia llamada o la del otro test de este
/// archivo corriendo en simultáneo -- no importa quién, importa el
/// resultado final de nuestro job). Devuelve el status final.
async fn wait_until_reaped(storage: &common::Storage, job_id: uuid::Uuid) -> String {
    for _ in 0..10 {
        let _ = storage.reap_expired_leases().await.unwrap();
        let job = storage.get_job(job_id).await.unwrap().unwrap();
        if job.status != common::JobStatus::Running {
            return job.status.as_str().to_string();
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("el reaper nunca recuperó el job {job_id}");
}
