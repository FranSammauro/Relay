//! Test de reliability (Fase 3): un job con max_attempts=3 que siempre
//! falla tiene que pasar por dos rondas de retry_scheduled y terminar en
//! dead_letter en el tercer intento -- ni antes, ni después. También
//! valida que job_attempts queda con la película completa.
//!
//! Como en concurrency.rs, esto necesita Postgres real: la lógica de
//! backoff vive en SQL (CASE + random() dentro del UPDATE), así que no
//! hay forma honesta de mockearla.
//!
//! A diferencia de concurrency.rs, este test SÍ asume que es dueño
//! exclusivo de lo que hay para reclamar en el momento de cada claim (por
//! eso valida `claimed.id == job.id` en vez de tolerar jobs ajenos). Si
//! esto falla con un mismatch de id, lo más probable es que haya un job
//! `pending`/`retry_scheduled` viejo dando vueltas en la base de otra
//! corrida interrumpida -- limpiá la base (`docker compose down -v` y
//! volvé a levantar postgres) antes de reintentar.

use common::{AttemptOutcome, NewJob};

mod support;

#[tokio::test]
async fn failing_job_retries_then_dead_letters() {
    let Some(storage) = support::connect_or_skip(&support::database_url()).await else {
        return;
    };

    let worker_id = "test-worker-reliability";

    let job = storage
        .create_job(NewJob {
            job_type: format!("reliability-test-{}", uuid::Uuid::new_v4()),
            payload: serde_json::json!({}),
            priority: 50,
            max_attempts: 3,
            timeout_seconds: 5,
            scheduled_at: None,
            idempotency_key: None,
        })
        .await
        .expect("create_job no debería fallar");

    // Intentos 1 y 2: todavía quedan reintentos, así que va a
    // retry_scheduled con un scheduled_at en el futuro (backoff).
    for attempt in 1..=2 {
        let claimed = storage
            .claim_next_job(worker_id)
            .await
            .expect("claim no debería fallar")
            .unwrap_or_else(|| panic!("debería poder reclamar el job en el intento {attempt}"));
        assert_eq!(
            claimed.id, job.id,
            "se reclamó un job distinto al de este test -- ¿hay basura vieja en la base?"
        );
        assert_eq!(claimed.attempts, attempt);

        let status = storage
            .record_failure(claimed.id, "boom", AttemptOutcome::Failed)
            .await
            .expect("record_failure no debería fallar");
        assert_eq!(status, "retry_scheduled");

        let after = storage
            .get_job(job.id)
            .await
            .unwrap()
            .expect("el job debería seguir existiendo");
        assert!(
            after.scheduled_at > chrono::Utc::now(),
            "el backoff debería agendar el retry en el futuro, no ahora"
        );

        // Sin esto el test tendría que dormir minutos reales esperando el
        // backoff. Pisamos scheduled_at a mano para simular "ya pasó el
        // tiempo de espera" -- es la única parte donde el test hace trampa
        // a propósito, y está bien: lo que se prueba es la transición de
        // estados, no el reloj.
        sqlx::query("UPDATE jobs SET scheduled_at = now() - interval '1 second' WHERE id = $1")
            .bind(job.id)
            .execute(storage.pool())
            .await
            .expect("no debería fallar el fast-forward del backoff");
    }

    // Intento 3: attempts llega a max_attempts, así que esta vez es DLQ.
    let claimed = storage
        .claim_next_job(worker_id)
        .await
        .expect("claim no debería fallar")
        .expect("debería poder reclamar el job en el tercer intento");
    assert_eq!(claimed.id, job.id, "se reclamó un job distinto al de este test");
    assert_eq!(claimed.attempts, 3);

    let status = storage
        .record_failure(claimed.id, "boom final", AttemptOutcome::TimedOut)
        .await
        .expect("record_failure no debería fallar");
    assert_eq!(status, "dead_letter");

    let final_job = storage
        .get_job(job.id)
        .await
        .unwrap()
        .expect("el job debería seguir existiendo");
    assert_eq!(final_job.status, common::JobStatus::DeadLetter);
    assert!(final_job.failed_at.is_some());

    // Un job en dead_letter es terminal: nadie más lo puede reclamar.
    let nothing_to_claim = storage
        .claim_next_job(worker_id)
        .await
        .expect("claim no debería fallar");
    assert!(
        nothing_to_claim.is_none(),
        "un job en dead_letter no debería ser reclamable"
    );

    // job_attempts tiene que tener los 3 intentos, con el error y el
    // outcome correctos en cada uno.
    let attempts = storage
        .list_attempts(job.id)
        .await
        .expect("list_attempts no debería fallar");
    assert_eq!(attempts.len(), 3);

    let last = attempts.iter().find(|a| a.attempt_number == 3).unwrap();
    assert_eq!(last.status.as_deref(), Some("timeout"));
    assert_eq!(last.error.as_deref(), Some("boom final"));

    let first = attempts.iter().find(|a| a.attempt_number == 1).unwrap();
    assert_eq!(first.status.as_deref(), Some("failed"));
}

#[tokio::test]
async fn successful_job_leaves_a_completed_attempt() {
    let Some(storage) = support::connect_or_skip(&support::database_url()).await else {
        return;
    };

    let job = storage
        .create_job(NewJob {
            job_type: format!("reliability-success-{}", uuid::Uuid::new_v4()),
            payload: serde_json::json!({}),
            priority: 50,
            max_attempts: 3,
            timeout_seconds: 5,
            scheduled_at: None,
            idempotency_key: None,
        })
        .await
        .unwrap();

    let claimed = storage
        .claim_next_job("test-worker-reliability-ok")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, job.id, "se reclamó un job distinto al de este test");

    storage.mark_completed(claimed.id).await.unwrap();

    let attempts = storage.list_attempts(job.id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status.as_deref(), Some("completed"));
    assert!(attempts[0].finished_at.is_some());
}
