//! Autenticación por API key y autorización por rol (Fase 8, ver ADR-007 y
//! ADR-008).
//!
//! La autenticación es un extractor `FromRequestParts` (resuelve el
//! `AuthContext` a partir de `Authorization: Bearer <key>`), que también se
//! ejecuta como middleware con `from_fn_with_state` para tener acceso a la
//! base. El `AuthContext` resultante se cachea en las extensions del request
//! para que las capas internas (autorización y rate limiting) no repitan el
//! lookup.
//!
//! Orden de capas y por qué:
//!
//! ```text
//! trace -> cors -> auth -> autorización (grant table) -> rate limit -> handler
//! ```
//!
//! - `auth` corre afuera: sin request autenticado no tiene sentido gastar un
//!   INCR de Redis ni aplicar reglas de rol.
//! - la autorización usa el `AuthContext` cacheado.
//! - el rate limiting también: limita por key y por rol.
//!
//! Nota de diseño: no se montan guardas de rol por sub-router con
//! `route_layer`. Al mergear dos routers que comparten una ruta (GET y POST
//! en `/jobs`), axum (0.7) combina los `MethodRouter` con `merge_for_path`,
//! que conserva los middlewares del primer router y descarta los del segundo.
//! Eso haría que un guarda de rol "desaparezca" silenciosamente para rutas
//! compartidas. En lugar de eso hay una sola capa de autorización con una
//! tabla de grants por (método, ruta) que es explícita y testeable.

use axum::extract::{FromRequestParts, Request, State};
use axum::http::header;
use axum::http::{request::Parts, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use uuid::Uuid;

use common::{parse_key, verify_api_key, ApiKeyRole, RateLimitResult};

use crate::state::AppState;

/// Identidad resuelta por la autenticación: qué key fue y con qué rol (ver
/// ADR-007 para la matriz de permisos). Queda disponible para los handlers
/// que la quieran como extractor.
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub key_id: Uuid,
    pub role: ApiKeyRole,
}

/// Una sola resolución por request: si el middleware de auth ya corrió,
/// devuelve el contexto cacheado en las extensions; si no (por ejemplo un
/// handler montado fuera del middleware en una prueba), resuelve completa
/// contra el header y la base.
///
/// Cada caso de rechazo devuelve el mismo `401` salvo el fallo de base, que
/// es un `500`: no se puede ni debe distinguir si una key no existe, está
/// revocada o el hash no coincide.
#[async_trait::async_trait]
impl FromRequestParts<AppState> for AuthContext {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        if let Some(ctx) = parts.extensions.get::<AuthContext>() {
            return Ok(ctx.clone());
        }

        // Header `Authorization: Bearer <key>`.
        let Some(auth_value) = parts.headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) else {
            return Err(unauthorized());
        };
        let Some(key) = auth_value.strip_prefix("Bearer ") else {
            return Err(unauthorized());
        };

        // Prefijo primero: no se hashea la key si el prefijo ni siquiera
        // existe. Las keys malformadas o desconocidas se descartan barato.
        let Some(prefix) = parse_key(key) else {
            return Err(unauthorized());
        };

        let stored = match state.storage.find_api_key_by_prefix(prefix).await {
            Ok(Some(stored)) => stored,
            Ok(None) => return Err(unauthorized()),
            Err(e) => {
                tracing::error!(event = "auth_lookup_error", error = %e, "failed to look up API key");
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "internal error" })),
                )
                    .into_response());
            }
        };

        // Revocada o hash sin coincidencia -> 401, indistinguible de key
        // inexistente (comparación en tiempo constante, ver common::verify).
        if stored.revoked_at.is_some() {
            return Err(unauthorized());
        }
        if !verify_api_key(key, &stored.key_hash) {
            return Err(unauthorized());
        }

        // last_used_at "mejor esfuerzo": no vale la pena que cada request
        // espere este UPDATE (además Storage::touch_api_key autothrottlea a
        // una escritura por minuto).
        let storage = state.storage.clone();
        tokio::spawn(async move {
            let _ = storage.touch_api_key(stored.id).await;
        });

        Ok(AuthContext {
            key_id: stored.id,
            role: stored.role,
        })
    }
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(json!({ "error": "missing or invalid API key" })),
    )
        .into_response()
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({ "error": "API key does not have permission for this resource" })),
    )
        .into_response()
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))).into_response()
}

/// Middleware de autenticación: corre la resolución de
/// `AuthContext::from_request_parts` con el estado de la app y cachea el
/// resultado en las extensions para las capas internas.
pub async fn auth_middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let (mut parts, body) = req.into_parts();
    match AuthContext::from_request_parts(&mut parts, &state).await {
        Ok(ctx) => {
            parts.extensions.insert(ctx);
            let req = Request::from_parts(parts, body);
            next.run(req).await
        }
        Err(resp) => resp,
    }
}

/// Tabla de autorización: por cada (método, ruta) los roles permitidos. Las
/// rutas con `:id` se modelan con `*`, que matchea un segmento cualquiera.
/// Es la matriz de permisos de ADR-007 en forma ejecutable.
const GRANT_TABLE: &[(&str, &str, &[ApiKeyRole])] = &[
    // Jobs: leer, cualquier rol autenticado.
    ("GET", "/jobs", &[ApiKeyRole::Producer, ApiKeyRole::Worker, ApiKeyRole::Admin]),
    ("GET", "/jobs/*", &[ApiKeyRole::Producer, ApiKeyRole::Worker, ApiKeyRole::Admin]),
    ("GET", "/jobs/*/attempts", &[ApiKeyRole::Producer, ApiKeyRole::Worker, ApiKeyRole::Admin]),
    // Jobs: escribir, solo producer y admin.
    ("POST", "/jobs", &[ApiKeyRole::Producer, ApiKeyRole::Admin]),
    ("DELETE", "/jobs/*", &[ApiKeyRole::Producer, ApiKeyRole::Admin]),
    // Operación: lectura para el monitoreo, cualquier rol autenticado.
    ("GET", "/stats", &[ApiKeyRole::Producer, ApiKeyRole::Worker, ApiKeyRole::Admin]),
    ("GET", "/metrics", &[ApiKeyRole::Producer, ApiKeyRole::Worker, ApiKeyRole::Admin]),
    ("GET", "/workers", &[ApiKeyRole::Producer, ApiKeyRole::Worker, ApiKeyRole::Admin]),
    // Cron (configuración operativa): solo admin.
    ("GET", "/cron", &[ApiKeyRole::Admin]),
    ("POST", "/cron", &[ApiKeyRole::Admin]),
    ("GET", "/cron/*", &[ApiKeyRole::Admin]),
    ("DELETE", "/cron/*", &[ApiKeyRole::Admin]),
];

/// Matchea una ruta concreta contra un patrón de la tabla de grants. `*`
/// matchea exactamente un segmento; el número de segmentos debe coincidir.
fn pattern_matches(pattern: &str, segments: &[&str]) -> bool {
    let pattern_segments: Vec<&str> = pattern
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    pattern_segments.len() == segments.len()
        && pattern_segments
            .iter()
            .zip(segments)
            .all(|(pat, seg)| *pat == "*" || *pat == *seg)
}

/// Busca en la tabla de grants los roles permitidos para (método, ruta).
fn grants_for<'a>(method: &axum::http::Method, path: &str) -> Option<&'static [ApiKeyRole]> {
    // Los `HEAD` llegan a los routes `get(* sx)`: tratar como `GET`.
    let method = if method == axum::http::Method::HEAD {
        "GET"
    } else {
        method.as_str()
    };
    let segments: Vec<&str> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    GRANT_TABLE
        .iter()
        .find(|(m, pattern, _)| *m == method && pattern_matches(pattern, &segments))
        .map(|(_, _, roles)| *roles)
}

/// Middleware de autorización: dado el `AuthContext` cacheado por `auth`,
/// decide con la tabla de grants si la key tiene permiso.
///
/// - es un 404 si la ruta no está en la tabla (nunca debería pasar: las
///   rutas están fijas; no filtra información, ya que la ruta no existiría
///   de todos modos);
/// - es un 403 si el rol de la key no está entre los permitidos;
/// - sin `AuthContext` (ruta protegida accedida sin pasar por auth) es un
///   401.
pub async fn authorize_middleware(req: Request, next: Next) -> Response {
    let Some(ctx) = req.extensions().get::<AuthContext>() else {
        return unauthorized();
    };

    match grants_for(req.method(), req.uri().path()) {
        Some(roles) if roles.contains(&ctx.role) => next.run(req).await,
        Some(_) => forbidden(),
        None => not_found(),
    }
}

/// Rate limiting por key (ventana deslizante sobre Redis, ver ADR-008).
/// Corre por dentro de la autorización, que es lo que garantiza que el
/// `AuthContext` ya está resuelto. Si el contexto no está (imposible en una
/// ruta protegida), no se limita.
///
/// Si Redis no está disponible, `RateLimiter::check` deja pasar la request:
/// el peor caso aceptable es "temporalmente sin rate limiting" (coherente
/// con la postura de alta disponibilidad de ADR-002), nunca cortar una
/// request legítima por un dependencia caída.
pub async fn rate_limit_middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let decision = match req.extensions().get::<AuthContext>() {
        Some(ctx) => state.rate_limiter.check(ctx.key_id, ctx.role).await,
        None => RateLimitResult::Allowed,
    };

    match decision {
        RateLimitResult::Allowed => next.run(req).await,
        RateLimitResult::Denied { retry_after_secs } => {
            tracing::warn!(
                event = "rate_limit_exceeded",
                retry_after_secs,
                "request rejected by rate limit"
            );
            (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, retry_after_secs.max(1).to_string())],
                Json(json!({
                    "error": "rate limit exceeded",
                    "retry_after_secs": retry_after_secs,
                })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_match_fixed_paths() {
        let roles = grants_for(&axum::http::Method::GET, "/jobs").unwrap();
        assert!(roles.contains(&ApiKeyRole::Producer));
        assert!(roles.contains(&ApiKeyRole::Worker));
        assert!(roles.contains(&ApiKeyRole::Admin));
    }

    #[test]
    fn grants_match_captured_paths() {
        let id = "83a3726c-4bea-4c24-a449-f87101835ab2";
        assert!(!grants_for(&axum::http::Method::DELETE, &format!("/jobs/{id}")).is_none());
        let roles = grants_for(&axum::http::Method::GET, &format!("/jobs/{id}/attempts")).unwrap();
        assert!(roles.contains(&ApiKeyRole::Worker));
    }

    #[test]
    fn grants_by_method_and_role() {
        assert!(grants_for(&axum::http::Method::POST, "/jobs").unwrap().contains(&ApiKeyRole::Producer));
        assert!(!grants_for(&axum::http::Method::POST, "/jobs").unwrap().contains(&ApiKeyRole::Worker));
        assert!(grants_for(&axum::http::Method::GET, "/cron").unwrap().contains(&ApiKeyRole::Admin));
        assert!(!grants_for(&axum::http::Method::GET, "/cron").unwrap().contains(&ApiKeyRole::Producer));
    }

    #[test]
    fn unknown_paths_have_no_grants() {
        assert!(grants_for(&axum::http::Method::GET, "/no-existe").is_none());
        assert!(grants_for(&axum::http::Method::GET, "/jobs/x/extra").is_none());
        assert!(grants_for(&axum::http::Method::PATCH, "/jobs").is_none());
        assert!(grants_for(&axum::http::Method::HEAD, "/jobs").is_some());
    }
}