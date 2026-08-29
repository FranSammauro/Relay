//! Test de recuperación (Fase 4): simula un worker que reclama un job y
//! desaparece sin notificar (kill -9, OOM, o cualquier otra causa), sin
//! llegar a invocar mark_completed ni record_failure. El lease vence por
//! sí solo, y el reaper debe recuperar el job aplicando la misma política
//! de reintento o dead-letter que un fallo reportado normalmente.
//!
//! Este test no utiliza Redis de forma deliberada, y esa omisión
//! constituye la validación más importante de ADR-002: la recuperación de
//! jobs abandonados se ejecuta enteramente sobre PostgreSQL. Si Redis
//! formara parte del camino crítico en este punto, este test no podría
//! existir sin levantar dos servicios en lugar de uno.
//!
//! Corresponden dos aclaraciones sobre aislamiento, dado que
//! `reap_expired_leases` es deliberadamente global (recorre toda la
//! tabla, no un job puntual, ya que así debe comportarse en producción: un
//! reaper real no conoce de antemano qué job buscar):
//!
//! 1. El harness de pruebas ejecuta las funciones `#[tokio::test]` de este
//!    mismo archivo en paralelo por defecto. Si ambos tests de este
//!    archivo tienen un lease vencido al mismo tiempo, una única llamada
//!    a `reap_expired_leases` desde cualquiera de los dos puede recuperar
//!    también el job del otro. Por esta razón, las validaciones verifican
//!    que el job propio se encuentre en algún lugar del resultado, en
//!    lugar de asumir que la lista contiene exactamente un elemento.
//! 2. Si la aserción `claimed.id != job.id` falla al reclamar, existen
//!    datos remanentes de otra corrida en la base (ver el comentario
//!    equivalente en reliability.rs).

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

    // El worker "worker-que-va-a-morir" reclama el job y no vuelve a aparecer.
    let claimed = storage
        .claim_next_job("worker-que-va-a-morir")
        .await
        .unwrap()
        .expect("debería poder reclamar el job");
    assert_eq!(claimed.id, job.id, "se reclamó un job distinto al de este test");

    assert_eq!(claimed.status, common::JobStatus::Running);
    assert!(claimed.lease_until.is_some(), "claim_next_job debería fijar un lease");
    assert_eq!(claimed.worker_id.as_deref(), Some("worker-que-va-a-morir"));

    // Sin esta modificación, sería necesario esperar timeout_seconds más
    // 30 segundos reales. Se adelanta el lease manualmente para simular su
    // vencimiento; esta simplificación se limita a una sola columna y no
    // afecta la lógica que se está verificando.
    sqlx::query("UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = $1")
        .bind(job.id)
        .execute(storage.pool())
        .await
        .unwrap();

    // Otro worker, todavía activo, ejecuta el reaper y encuentra el job
    // abandonado. Es posible que también haya recuperado otro job si el
    // segundo test de este archivo se ejecutó en simultáneo; por eso se
    // busca el job propio dentro de la lista, en lugar de asumir que fue
    // el único recuperado (ver comentario del módulo).
    let reaped = wait_until_reaped(&storage, job.id).await;
    assert_eq!(reaped, "retry_scheduled");

    let after = storage.get_job(job.id).await.unwrap().unwrap();
    assert_eq!(after.status, common::JobStatus::RetryScheduled);
    assert_eq!(after.attempts, 1);
    // El job queda sin dueño: nadie lo posee hasta que sea reclamado nuevamente.
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

/// Ejecuta el reaper y espera a que el job propio salga del estado
/// `running`, sin importar si lo recuperó esta llamada o el otro test de
/// este archivo ejecutándose en simultáneo; lo relevante es el resultado
/// final del job en cuestión. Devuelve el estado final.
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
