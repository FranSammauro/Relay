//! Utilidades compartidas para tests de integración de la API (requiere
//! Postgres y Redis corriendo).
//!
//! Cada archivo de tests de integración (auth.rs, ratelimit.rs) se compila
//! como un binario independiente, y este módulo se incluye por separado en
//! cada uno. Como consecuencia, una función usada solo desde uno de los
//! dos archivos genera una advertencia de código sin uso al compilar el
//! otro binario, aunque el conjunto completo esté en uso. Por eso las
//! funciones de este módulo llevan `#[allow(dead_code)]`: no indica código
//! muerto real, sino una consecuencia esperada del modelo de compilación
//! de los tests de integración en Rust.

use common::{Storage, Heartbeats, RateLimiter, RateLimits};
use std::env;

/// Intenta conectar a Postgres; si no hay `DATABASE_URL` o falla la
/// conexión, devuelve `None` (el test se salta imprimiendo aviso).
#[allow(dead_code)]
pub async fn pg_connect_or_skip() -> Option<Storage> {
    let url = env::var("DATABASE_URL").ok()?;
    match Storage::connect(&url).await {
        Ok(s) => {
            if s.migrate().await.is_ok() {
                Some(s)
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

/// Intenta conectar a Redis; `None` si no hay `REDIS_URL` o falla.
#[allow(dead_code)]
pub async fn redis_connect_or_skip() -> Option<redis::aio::ConnectionManager> {
    let url = env::var("REDIS_URL").ok()?;
    let client = redis::Client::open(url).ok()?;
    client.get_connection_manager().await.ok()
}



/// Limpia las claves de rate limiting para un `key_id` (útil entre tests).
#[allow(dead_code)]
pub async fn cleanup_ratelimit(key_id: &uuid::Uuid, conn: &mut redis::aio::ConnectionManager) {
    let pattern = format!("ratelimit:{key_id}:*");
    let mut cursor = 0u64;
    let mut keys = Vec::new();
    loop {
        let (new_cursor, found): (u64, Vec<String>) = redis::cmd("SCAN")
            .cursor_arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .query_async(conn)
            .await
            .unwrap_or((0, Vec::new()));
        cursor = new_cursor;
        keys.extend(found);
        if cursor == 0 { break; }
    }
    if !keys.is_empty() {
        let _: () = redis::cmd("DEL").arg(&keys).query_async(conn).await.unwrap_or(());
    }
}

/// Helper para crear una API key de test directamente en la DB (bypassa CLI).
pub async fn create_test_key(
    storage: &Storage,
    name: &str,
    role: common::ApiKeyRole,
) -> anyhow::Result<(common::ApiKeyRecord, common::ApiKeySecret)> {
    storage.create_api_key(name, role).await.map_err(anyhow::Error::from)
}

/// Crea un `AppState` completo para tests de la API (requiere ambos servicios).
pub async fn test_app_state() -> Option<api::state::AppState> {
    let storage = pg_connect_or_skip().await?;
    let redis_url = env::var("REDIS_URL").ok()?;
    let heartbeats = Heartbeats::connect(&redis_url).await.ok()?;
    let rate_limiter = RateLimiter::connect(&redis_url, RateLimits::from_env()).await.ok()?;
    Some(api::state::AppState { storage, heartbeats, rate_limiter })
}