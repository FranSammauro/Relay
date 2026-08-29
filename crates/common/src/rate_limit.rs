//! Rate limiting por API key sobre Redis (sliding window counter, ver
//! ADR-008).
//!
//! El contador vive en Redis y no en Postgres de forma deliberada: el rate
//! limiting es coordinación efímera por definición (un contador que solo
//! importa durante una ventana corta de tiempo), así que es coherente con
//! ADR-002. Si Redis se cae, el peor caso aceptable es "temporalmente sin
//! rate limiting" — se deja pasar la request y se loguea el error, igual
//! que con los heartbeats —, nunca cortar una request legítima.

use chrono::Utc;
use redis::AsyncCommands;
use uuid::Uuid;

use crate::api_keys::ApiKeyRole;
use crate::error::QueueError;

/// Duración de la ventana de conteo. Los límites se expresan "por minuto";
/// la imprecisión de bordes de ventana (ráfagas justo en el límite) no
/// justifica la complejidad de un token bucket verdadero (ver ADR-008).
pub const WINDOW_SECONDS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitResult {
    Allowed,
    Denied {
        /// Segundos que faltan para el próximo `window_epoch`, para `Retry-After`.
        retry_after_secs: u64,
    },
}

/// Límites por rol, configurables por env (0 = sin límite).
#[derive(Debug, Clone)]
pub struct RateLimits {
    pub producer_per_minute: u64,
    pub worker_per_minute: u64,
    pub admin_per_minute: u64,
}

impl RateLimits {
    /// Valores por defecto razonables para un proyecto de este porte: 5
    /// requests por segundo por key para producer/worker, y sin límite
    /// para admin (es la propia operación, no un tercero).
    pub fn from_env() -> Self {
        Self {
            producer_per_minute: env_u64("RATE_LIMIT_PRODUCER_PER_MINUTE").unwrap_or(300),
            worker_per_minute: env_u64("RATE_LIMIT_WORKER_PER_MINUTE").unwrap_or(300),
            admin_per_minute: env_u64("RATE_LIMIT_ADMIN_PER_MINUTE").unwrap_or(0),
        }
    }

    pub fn limit_for(&self, role: ApiKeyRole) -> u64 {
        match role {
            ApiKeyRole::Producer => self.producer_per_minute,
            ApiKeyRole::Worker => self.worker_per_minute,
            ApiKeyRole::Admin => self.admin_per_minute,
        }
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

#[derive(Clone)]
pub struct RateLimiter {
    conn: redis::aio::ConnectionManager,
    limits: RateLimits,
}

impl RateLimiter {
    pub async fn connect(redis_url: &str, limits: RateLimits) -> Result<Self, QueueError> {
        let client = redis::Client::open(redis_url)?;
        let conn = client.get_connection_manager().await?;
        Ok(Self { conn, limits })
    }

    pub fn limits(&self) -> &RateLimits {
        &self.limits
    }

    /// Registra un request de la key `key_id` y decide si se permite.
    ///
    /// Patrón: `INCR ratelimit:{key_id}:{window_epoch}`; si el contador
    /// recién se creó (devuelve 1) se setea el TTL de la ventana. El
    /// `window_epoch` cambia solo cada `WINDOW_SECONDS`, así que el
    /// contador viejo se abandona solo al expirar.
    pub async fn check(&self, key_id: Uuid, role: ApiKeyRole) -> RateLimitResult {
        let limit = self.limits.limit_for(role);
        if limit == 0 {
            return RateLimitResult::Allowed;
        }

        let now = Utc::now().timestamp().max(0) as u64;
        let window = now / WINDOW_SECONDS;
        let retry_after_secs = WINDOW_SECONDS - (now % WINDOW_SECONDS);
        let key = format!("ratelimit:{key_id}:{window}");

        let mut conn = self.conn.clone();
        let count: i64 = match conn.incr(&key, 1).await {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!(
                    event = "rate_limit_redis_error",
                    error = %e,
                    "redis unavailable, allowing request without rate limit (ver ADR-008)"
                );
                return RateLimitResult::Allowed;
            }
        };

        if count == 1 {
            if let Err(e) = conn.expire::<_, ()>(&key, WINDOW_SECONDS as i64).await {
                tracing::warn!(
                    event = "rate_limit_ttl_error",
                    error = %e,
                    "failed to set TTL on rate limit counter"
                );
            }
        }

        if (count as u64) > limit {
            RateLimitResult::Denied { retry_after_secs }
        } else {
            RateLimitResult::Allowed
        }
    }
}