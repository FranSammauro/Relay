//! Test de scheduling (Fase 5): tres cosas distintas, en el mismo archivo
//! porque las tres viven en `Storage` y comparten el mismo Postgres real.
//!
//! 1. Delayed jobs: en realidad ya funcionaban desde Fase 1
//!    (`NewJob::scheduled_at` + el filtro `scheduled_at <= now()` en
//!    `claim_next_job`) -- esto es una validación explícita de que siguen
//!    andando, no una feature nueva.
//! 2. Disparo de un cron schedule: crea el job de verdad, avanza
//!    `next_run_at`, y usa un `idempotency_key` derivado para no duplicar
//!    si algo lo dispara dos veces.
//! 3. El primitivo de liderazgo (`try_become_scheduler_leader`): dos
//!    conexiones separadas peleando por el mismo advisory lock -- solo una
//!    debería ganar, y soltar esa conexión debería liberar el lock para
//!    que la otra pueda tomarlo.

use common::{Job, NewCronSchedule, NewJob, Storage};

mod support;

/// Reclama hasta encontrar el job puntual que nos interesa, completando de
/// paso cualquier job ajeno que se cruce en el camino (basura de otro test
/// de este archivo corriendo en simultáneo -- ver nota de aislamiento en
/// recovery.rs). Devuelve `None` si no hay nada más para reclamar antes de
/// encontrar el nuestro, que es la señal de "todavía no le toca".
async fn claim_until_ours_or_empty(storage: &Storage, worker_id: &str, job_id: uuid::Uuid) -> Option<Job> {
    for _ in 0..50 {
        match storage.claim_next_job(worker_id).await.unwrap() {
            Some(job) if job.id == job_id => return Some(job),
            Some(other) => {
                storage.mark_completed(other.id).await.unwrap();
            }
            None => return None,
        }
    }
    panic!("claim_until_ours_or_empty: demasiadas vueltas sin encontrar el job {job_id}");
}

#[tokio::test]
async fn delayed_job_is_not_claimable_before_its_time() {
    let Some(storage) = support::connect_or_skip(&support::database_url()).await else {
        return;
    };

    let job_type = format!("delayed-test-{}", uuid::Uuid::new_v4());
    let future = chrono::Utc::now() + chrono::Duration::hours(1);

    let job = storage
        .create_job(NewJob {
            job_type: job_type.clone(),
            payload: serde_json::json!({}),
            priority: 50,
            max_attempts: 5,
            timeout_seconds: 30,
            scheduled_at: Some(future),
            idempotency_key: None,
        })
        .await
        .unwrap();

    assert_eq!(job.status, common::JobStatus::Pending);

    // el worker que "casualmente" reclama justo ahora no debería llevárselo
    // -- si claim_until_ours_or_empty devuelve Some acá, algo está mal.
    let claimed = claim_until_ours_or_empty(&storage, "test-worker-delayed", job.id).await;
    assert!(claimed.is_none(), "un job agendado para el futuro no debería ser reclamable todavía");

    // pisamos scheduled_at al pasado para simular que ya llegó la hora --
    // misma técnica de "fast-forward" que usamos en reliability.rs.
    sqlx::query("UPDATE jobs SET scheduled_at = now() - interval '1 second' WHERE id = $1")
        .bind(job.id)
        .execute(storage.pool())
        .await
        .unwrap();

    let claimed = claim_until_ours_or_empty(&storage, "test-worker-delayed", job.id)
        .await
        .expect("ahora sí debería poder reclamarlo");
    assert_eq!(claimed.id, job.id);
}

#[tokio::test]
async fn cron_schedule_fires_and_advances_next_run() {
    let Some(storage) = support::connect_or_skip(&support::database_url()).await else {
        return;
    };

    let name = format!("cron-test-{}", uuid::Uuid::new_v4());
    let schedule = storage
        .create_cron_schedule(NewCronSchedule {
            name: name.clone(),
            cron_expr: "* * * * *".to_string(), // cada minuto
            job_type: "sleep".to_string(),
            payload: serde_json::json!({ "seconds": 0 }),
            priority: 50,
            max_attempts: 3,
            timeout_seconds: 10,
        })
        .await
        .expect("create_cron_schedule no debería fallar");

    assert!(schedule.enabled);
    assert!(schedule.last_run_at.is_none());
    assert!(
        schedule.next_run_at > chrono::Utc::now(),
        "next_run_at debería calcularse hacia el futuro, no quedar en el pasado"
    );

    let original_next_run = schedule.next_run_at;
    let _ = original_next_run; // documentativo: ver nota más abajo sobre por qué no lo comparamos directo

    // simulamos que ya llegó la hora, sin esperar el minuto real.
    sqlx::query("UPDATE cron_schedules SET next_run_at = now() - interval '1 second' WHERE id = $1")
        .bind(schedule.id)
        .execute(storage.pool())
        .await
        .unwrap();

    let due = storage.due_cron_schedules().await.unwrap();
    assert!(
        due.iter().any(|s| s.id == schedule.id),
        "el schedule debería aparecer como vencido"
    );
    let due_schedule = due.into_iter().find(|s| s.id == schedule.id).unwrap();

    let job = storage.fire_cron_schedule(&due_schedule).await.unwrap();
    assert_eq!(job.job_type, "sleep");
    assert_eq!(job.max_attempts, 3);
    assert_eq!(job.timeout_seconds, 10);

    let after = storage.get_cron_schedule(schedule.id).await.unwrap().unwrap();
    assert!(after.last_run_at.is_some());
    assert!(
        after.next_run_at > due_schedule.next_run_at,
        "next_run_at debería haber avanzado después de disparar"
    );
    // Nota: NO comparamos against original_next_run acá. Con "* * * * *"
    // (cada minuto) y un test que corre en milisegundos, el next_run_at
    // recalculado después de pisar next_run_at a "ahora" puede coincidir
    // por casualidad con el next_run_at original si ambos caen en el mismo
    // minuto -- no es un bug, es nomás una expresión de grano muy fino
    // para esa comparación puntual. Lo que sí importa (y si probamos
    // arriba) es que avanzó respecto al next_run_at que se estaba
    // cumpliendo en este disparo.

    // disparar el MISMO schedule "vencido" de nuevo (ej: dos líderes se
    // solaparon por un instante) no debería crear un segundo job --
    // idempotency_key hace de red de seguridad.
    let job2 = storage.fire_cron_schedule(&due_schedule).await.unwrap();
    assert_eq!(job2.id, job.id, "un doble disparo del mismo horario no debería duplicar el job");
}

#[tokio::test]
async fn only_one_connection_can_hold_the_scheduler_leadership() {
    let Some(storage) = support::connect_or_skip(&support::database_url()).await else {
        return;
    };

    let first = storage
        .try_become_scheduler_leader()
        .await
        .unwrap()
        .expect("la primera conexión debería poder tomar el liderazgo");

    let second = storage.try_become_scheduler_leader().await.unwrap();
    assert!(
        second.is_none(),
        "una segunda conexión no debería poder tomar el liderazgo mientras la primera lo sostiene"
    );

    // soltar la conexión libera el advisory lock de sesión -- es el
    // mecanismo completo de ADR-006: no hace falta un TTL ni un heartbeat
    // de liderazgo, Postgres limpia solo cuando la sesión muere.
    //
    // Ojo con esto: un `drop(first)` normal NO alcanza acá. sqlx pool
    // devuelve la conexión al pool para reusarla en vez de cerrarla de
    // verdad -- y el advisory lock es de sesión, así que sigue vivo
    // mientras la sesión (la conexión TCP real a Postgres) siga viva,
    // aunque el wrapper de Rust ya se haya soltado. `.close()` sí termina
    // la sesión de verdad. En producción esto no es un problema: si un
    // worker se cae de verdad, se cae la conexión TCP con él, y ahí sí
    // Postgres libera el lock solo -- el caso que hay que手动 manejar es
    // justamente el de "bajada prolija" simulado acá.
    first.close().await.unwrap();

    // dar un instante para que Postgres procese el cierre de la conexión.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let third = storage.try_become_scheduler_leader().await.unwrap();
    assert!(
        third.is_some(),
        "tras soltar la primera conexión, otra debería poder tomar el liderazgo"
    );
}
