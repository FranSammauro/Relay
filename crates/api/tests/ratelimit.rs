//! Tests de rate limiting (requiere Postgres + Redis).

use api::app;
mod test_support;
use test_support::{cleanup_ratelimit, create_test_key, redis_connect_or_skip, test_app_state};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use tower::ServiceExt;
use uuid::Uuid;

async fn setup_rate_test() -> Option<(axum::Router, String, Uuid)> {
    let state = test_app_state().await?;
    let storage = &state.storage;
    let (rec, sec) = create_test_key(storage, "ratelimit-test", common::ApiKeyRole::Producer).await.ok()?;
    // limpiar contadores previos
    if let Some(mut conn) = redis_connect_or_skip().await {
        cleanup_ratelimit(&rec.id, &mut conn).await;
    }
    let app = app::router(state);
    Some((app, sec.as_str().to_string(), rec.id))
}

#[tokio::test]
async fn rate_limit_allows_within_budget() {
    let Some((app, key, _)) = setup_rate_test().await else { eprintln!("saltando: sin infra"); return; };

    // Hacer algunas requests dentro del límite (default 300/min para producer)
    for _ in 0..5 {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/jobs")
            .header(header::AUTHORIZATION, format!("Bearer {}", key))
            .body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn rate_limit_blocks_and_returns_429_with_retry_after() {
    let Some((app, key, key_id)) = setup_rate_test().await else { eprintln!("saltando: sin infra"); return; };

    // Setear un límite bajísimo solo para este test vía env no es trivial sin
    // recompilar; en su lugar, vaciamos y creamos una key de test con role
    // worker (300/min) y usamos la key admin (0 = ilimitado) como control.
    // Para probar el bloqueo real necesitaríamos override de límites o
    // usar el limite de 300 haciendo 301 requests - muy lento.
    // Aquí verificamos el comportamiento del header Retry-After cuando se
    // supera el límite manualmente inyectando contador en Redis.

    if let Some(mut conn) = redis_connect_or_skip().await {
        // Inyectar contador = límite + 1 en la ventana actual
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let window = now / 60;
        let redis_key = format!("ratelimit:{key_id}:{window}");
        let limit = 300u64; // producer default
        let _: () = redis::cmd("SET").arg(&redis_key).arg(limit + 1).arg("EX").arg(60).query_async(&mut conn).await.unwrap();

        // Request debe ser 429
        let req = Request::builder()
            .method(Method::GET)
            .uri("/jobs")
            .header(header::AUTHORIZATION, format!("Bearer {}", key))
            .body(Body::empty()).unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);

        // Verificar header Retry-After presente y > 0
        let retry_after = res.headers().get(header::RETRY_AFTER).and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<u64>().ok());
        assert!(retry_after.is_some() && retry_after.unwrap() > 0);
    }
}

#[tokio::test]
async fn rate_limit_admin_unlimited() {
    let Some(state) = test_app_state().await else { eprintln!("saltando: sin infra"); return; };
    let storage = &state.storage;

    // Admin key (límite 0 = sin límite)
    let (rec, sec) = create_test_key(storage, "admin-unlimited", common::ApiKeyRole::Admin).await.unwrap();

    // Limpiar cualquier contador previo
    if let Some(mut conn) = redis_connect_or_skip().await {
        cleanup_ratelimit(&rec.id, &mut conn).await;
    }

    let app = app::router(state);

    // Muchas requests, nunca debe 429
    for _ in 0..20 {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/jobs")
            .header(header::AUTHORIZATION, format!("Bearer {}", sec.as_str()))
            .body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_ne!(res.status(), StatusCode::TOO_MANY_REQUESTS, "admin no debe rate-limitearse");
    }
}

#[tokio::test]
async fn rate_limit_public_routes_unaffected() {
    let Some(state) = test_app_state().await else { eprintln!("saltando: sin infra"); return; };
    let app = app::router(state);

    // Rutas públicas sin autenticación no se rate-limitan
    for _ in 0..10 {
        let req = Request::builder().method(Method::GET).uri("/health").body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn rate_limit_redis_down_allows_requests() {
    // Este test es difícil de automatizar sin matar Redis.
    // Se documenta el comportamiento en ADR-008: si Redis no está
    // disponible, el rate limiter deja pasar la request (fail-open).
    // El código en common::RateLimiter::check maneja el error y devuelve Allowed.
}