//! Tests de autenticación y autorización HTTP (requiere Postgres + Redis).

use api::app;
mod test_support;
use test_support::{create_test_key, test_app_state};
use axum::{body::Body, http::{Method, Request, StatusCode, header}, Router};
use tower::ServiceExt;
use uuid::Uuid;

async fn app_with_keys() -> Option<(Router, Uuid, String, Uuid, String, Uuid, String)> {
    let state = test_app_state().await?;
    let storage = &state.storage;

    let (prod_rec, prod_sec) = create_test_key(storage, "test-producer", common::ApiKeyRole::Producer).await.ok()?;
    let (worker_rec, worker_sec) = create_test_key(storage, "test-worker", common::ApiKeyRole::Worker).await.ok()?;
    let (admin_rec, admin_sec) = create_test_key(storage, "test-admin", common::ApiKeyRole::Admin).await.ok()?;

    let app = app::router(state);
    Some((app, prod_rec.id, prod_sec.as_str().to_string(), worker_rec.id, worker_sec.as_str().to_string(), admin_rec.id, admin_sec.as_str().to_string()))
}

#[tokio::test]
async fn auth_missing_header_returns_401() {
    let Some(state) = test_app_state().await else { eprintln!("saltando: sin infra"); return; };
    let app = app::router(state);

    let req = Request::builder().method(Method::GET).uri("/jobs").body(Body::empty()).unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    // WWW-Authenticate: Bearer
    let auth = res.headers().get(header::WWW_AUTHENTICATE).and_then(|v| v.to_str().ok());
    assert_eq!(auth, Some("Bearer"));
}

#[tokio::test]
async fn auth_invalid_key_returns_401() {
    let Some(state) = test_app_state().await else { eprintln!("saltando: sin infra"); return; };
    let app = app::router(state);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/jobs")
        .header(header::AUTHORIZATION, "Bearer dq_invalidkey_badformat")
        .body(Body::empty()).unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_revoked_key_returns_401() {
    let Some(state) = test_app_state().await else { eprintln!("saltando: sin infra"); return; };
    let storage = &state.storage;

    let (rec, sec) = create_test_key(storage, "to-revoke", common::ApiKeyRole::Producer).await.unwrap();
    storage.revoke_api_key_by_prefix(&rec.key_prefix).await.unwrap();

    let app = app::router(state);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/jobs")
        .header(header::AUTHORIZATION, format!("Bearer {}", sec.as_str()))
        .body(Body::empty()).unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_producer_can_read_and_write() {
    let Some((app, _prod_id, prod_key, _, _, _, _)) = app_with_keys().await else { eprintln!("saltando: sin infra"); return; };

    // GET /jobs (read)
    let req = Request::builder()
        .method(Method::GET)
        .uri("/jobs")
        .header(header::AUTHORIZATION, format!("Bearer {}", prod_key))
        .body(Body::empty()).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "producer GET /jobs");

    // POST /jobs (creación)
    let req = Request::builder()
        .method(Method::POST)
        .uri("/jobs")
        .header(header::AUTHORIZATION, format!("Bearer {}", prod_key))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"type":"test","payload":{}}"#)).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CREATED, "producer POST /jobs");

    // La cancelación (DELETE /jobs/:id) queda cubierta por la matriz de
    // autorización general; no se ejecuta acá para no depender de crear
    // primero un job y conocer su id.

    // GET /stats, /metrics, /workers (read)
    for path in ["/stats", "/metrics", "/workers"] {
        let req = Request::builder()
            .method(Method::GET)
            .uri(path)
            .header(header::AUTHORIZATION, format!("Bearer {}", prod_key))
            .body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "producer {path}");
    }
}

#[tokio::test]
async fn auth_worker_can_read_but_not_write() {
    let Some((app, _, _, _worker_id, worker_key, _, _)) = app_with_keys().await else { eprintln!("saltando: sin infra"); return; };

    // GET /jobs (read) -> OK
    let req = Request::builder()
        .method(Method::GET)
        .uri("/jobs")
        .header(header::AUTHORIZATION, format!("Bearer {}", worker_key))
        .body(Body::empty()).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "worker GET /jobs");

    // POST /jobs (escritura, prohibido para worker) -> 403
    let req = Request::builder()
        .method(Method::POST)
        .uri("/jobs")
        .header(header::AUTHORIZATION, format!("Bearer {}", worker_key))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"type":"test","payload":{}}"#)).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN, "worker POST /jobs forbidden");

    // DELETE /jobs/:id -> 403 (try with fake id)
    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/jobs/00000000-0000-0000-0000-000000000000")
        .header(header::AUTHORIZATION, format!("Bearer {}", worker_key))
        .body(Body::empty()).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    // 403 (forbidden) or 404 (not found). Since the role guard runs before handler, should be 403.
    // But the path /jobs/:id exists -> method DELETE exists -> guard applies -> 403.
    assert_eq!(res.status(), StatusCode::FORBIDDEN, "worker DELETE /jobs forbidden");

    // GET /stats, /metrics, /workers -> OK
    for path in ["/stats", "/metrics", "/workers"] {
        let req = Request::builder()
            .method(Method::GET)
            .uri(path)
            .header(header::AUTHORIZATION, format!("Bearer {}", worker_key))
            .body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "worker {path}");
    }
}

#[tokio::test]
async fn auth_admin_can_access_cron() {
    let Some((app, _, _, _, _, _admin_id, admin_key)) = app_with_keys().await else { eprintln!("saltando: sin infra"); return; };

    // GET /cron -> OK
    let req = Request::builder()
        .method(Method::GET)
        .uri("/cron")
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_key))
        .body(Body::empty()).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "admin GET /cron");

    // POST /cron -> Created
    let req = Request::builder()
        .method(Method::POST)
        .uri("/cron")
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_key))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"name":"t","cron_expr":"* * * * *","type":"t","payload":{}}"#)).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CREATED, "admin POST /cron");
}

#[tokio::test]
async fn auth_producer_cannot_access_cron() {
    let Some((app, _, prod_key, _, _, _, _)) = app_with_keys().await else { eprintln!("saltando: sin infra"); return; };

    let req = Request::builder()
        .method(Method::GET)
        .uri("/cron")
        .header(header::AUTHORIZATION, format!("Bearer {}", prod_key))
        .body(Body::empty()).unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN, "producer /cron forbidden");
}

#[tokio::test]
async fn auth_worker_cannot_access_cron() {
    let Some((app, _, _, _, worker_key, _, _)) = app_with_keys().await else { eprintln!("saltando: sin infra"); return; };

    let req = Request::builder()
        .method(Method::GET)
        .uri("/cron")
        .header(header::AUTHORIZATION, format!("Bearer {}", worker_key))
        .body(Body::empty()).unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN, "worker /cron forbidden");
}

#[tokio::test]
async fn public_routes_no_auth_required() {
    let state = test_app_state().await;
    if state.is_none() { eprintln!("saltando: sin infra"); return; }
    let app = crate::app::router(state.unwrap());

    for path in ["/health", "/ready", "/"] {
        let req = Request::builder().method(Method::GET).uri(path).body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "public {path}");
    }
}

#[tokio::test]
async fn auth_head_requests_treated_as_get() {
    let Some((app, _, prod_key, _, _, _, _)) = app_with_keys().await else { eprintln!("saltando: sin infra"); return; };

    let req = Request::builder()
        .method(Method::HEAD)
        .uri("/jobs")
        .header(header::AUTHORIZATION, format!("Bearer {}", prod_key))
        .body(Body::empty()).unwrap();
    let res = app.oneshot(req).await.unwrap();
    // HEAD should be treated as GET by the authz layer -> OK
    assert_eq!(res.status(), StatusCode::OK);
}