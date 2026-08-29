//! Test de scheduling (Fase 5): agrupa tres validaciones distintas en el
//! mismo archivo, dado que las tres se ejecutan sobre `Storage` y
//! comparten el mismo PostgreSQL real.
//!
//! 1. Delayed jobs: este mecanismo ya funcionaba desde la Fase 1
//!    (`NewJob::scheduled_at` junto con el filtro `scheduled_at <= now()`
//!    en `claim_next_job`); esta es una validación explícita de que
//!    continúa funcionando, no una funcionalidad nueva.
//! 2. Disparo de un cron schedule: crea el job real, avanza `next_run_at`,
//!    y utiliza un `idempotency_key` derivado para evitar duplicados si el
//!    disparo se ejecuta más de una vez.
//! 3. El primitivo de liderazgo (`try_become_scheduler_leader`): dos
//!    conexiones separadas compitiendo por el mismo advisory lock, donde
//!    solo una debería obtenerlo, y liberar esa conexión debería permitir
//!    que la otra lo adquiera.

use common::{Job, NewCronSchedule, NewJob, Storage};

mod support;

/// Reclama repetidamente hasta encontrar el job puntual de interés,
/// completando de paso cualquier job ajeno que se interponga (datos
/// remanentes de otro test de este archivo ejecutándose en simultáneo; ver
/// la nota de aislamiento en recovery.rs). Devuelve `None` si no queda
/// nada más para reclamar antes de encontrar el job propio, lo cual
/// constituye la señal de que todavía no corresponde reclamarlo.
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
    panic!("claim_until_ours_or_empty: se excedió el número de intentos sin encontrar el job {job_id}");
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

    // Un worker que intentara reclamar en este momento no debería obtener
    // este job. Si claim_until_ours_or_empty devuelve Some en este punto,
    // corresponde investigar el problema.
    let claimed = claim_until_ours_or_empty(&storage, "test-worker-delayed", job.id).await;
    assert!(claimed.is_none(), "un job agendado para el futuro no debería ser reclamable todavía");

    // Se adelanta scheduled_at al pasado para simular que ya llegó la
    // hora, con la misma técnica utilizada en reliability.rs.
    sqlx::query("UPDATE jobs SET scheduled_at = now() - interval '1 second' WHERE id = $1")
        .bind(job.id)
        .execute(storage.pool())
        .await
        .unwrap();

    let claimed = claim_until_ours_or_empty(&storage, "test-worker-delayed", job.id)
        .await
        .expect("en este punto debería poder reclamarlo");
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
            cron_expr: "* * * * *".to_string(), // Cada minuto.
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
    let _ = original_next_run; // Referenciado únicamente para documentar la nota siguiente; no se compara directamente.

    // Se simula que ya llegó la hora del disparo, sin esperar el minuto real.
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
    // Nota: deliberadamente no se compara el resultado contra
    // original_next_run. Con la expresión "* * * * *" (cada minuto) y un
    // test que se ejecuta en el orden de milisegundos, el next_run_at
    // recalculado tras adelantar next_run_at al presente puede coincidir
    // por casualidad con el valor original si ambos caen dentro del mismo
    // minuto. Esto no constituye un error, sino una limitación de
    // resolución para ese tipo de comparación puntual. Lo relevante, y lo
    // que se verifica en la aserción anterior, es que el valor avanzó
    // respecto al next_run_at que se estaba cumpliendo en este disparo.

    // Disparar el mismo schedule "vencido" nuevamente (por ejemplo, si dos
    // líderes se solaparan por un instante) no debería crear un segundo
    // job: idempotency_key actúa como mecanismo de seguridad adicional.
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

    // Liberar la conexión libera el advisory lock de sesión: este es el
    // mecanismo completo descrito en ADR-006, sin necesidad de TTL ni
    // heartbeat de liderazgo, ya que PostgreSQL realiza la limpieza
    // automáticamente cuando la sesión finaliza.
    //
    // Es importante notar que un `drop(first)` convencional no resulta
    // suficiente en este caso. El pool de sqlx devuelve la conexión al
    // pool para su reutilización en lugar de cerrarla efectivamente, y
    // dado que el advisory lock es de sesión, permanece activo mientras la
    // sesión (la conexión TCP real a PostgreSQL) siga viva, aun cuando el
    // wrapper de Rust ya haya sido descartado. El método `.close()` sí
    // finaliza la sesión de forma efectiva. En producción esto no
    // representa un problema: si un worker finaliza de forma anómala, la
    // conexión TCP se interrumpe junto con él, y en ese caso PostgreSQL sí
    // libera el lock automáticamente. El caso que requiere manejo
    // explícito es precisamente el de una baja ordenada, que es lo que
    // este test simula.
    first.close().await.unwrap();

    // Se otorga un breve margen para que PostgreSQL procese el cierre de la conexión.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let third = storage.try_become_scheduler_leader().await.unwrap();
    assert!(
        third.is_some(),
        "tras soltar la primera conexión, otra debería poder tomar el liderazgo"
    );
}
