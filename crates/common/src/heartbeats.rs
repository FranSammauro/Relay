use crate::error::QueueError;
use futures_util::StreamExt;
use redis::AsyncCommands;

/// Cliente de liveness de workers sobre Redis.
///
/// Deliberadamente separado de `Storage`: esto es coordinación efímera
/// (ADR-002), no fuente de verdad. Si Redis se cae, se pierde visibilidad
/// de "quién está vivo ahora mismo" -- pero la recuperación de jobs
/// abandonados (`Storage::reap_expired_leases`) sigue funcionando igual,
/// porque corre enteramente sobre PostgreSQL y no depende de esto en
/// absoluto (ver ADR-002 y ADR-003).
#[derive(Clone)]
pub struct Heartbeats {
    conn: redis::aio::ConnectionManager,
}

fn heartbeat_key(worker_id: &str) -> String {
    format!("worker:heartbeat:{worker_id}")
}

impl Heartbeats {
    pub async fn connect(redis_url: &str) -> Result<Self, QueueError> {
        let client = redis::Client::open(redis_url)?;
        let conn = client.get_connection_manager().await?;
        Ok(Self { conn })
    }

    /// Refresca el heartbeat de un worker. La clave expira sola después de
    /// `ttl_seconds` -- si el worker se cae y deja de llamar a esto, en
    /// vez de tener que limpiar nada a mano, Redis lo hace por nosotros.
    /// Es la razón principal por la que esto vive en Redis y no en una
    /// columna de `workers` en Postgres: ahí tendríamos que escribir cada
    /// pocos segundos por worker Y encima acordarnos de barrer heartbeats
    /// viejos con un job aparte.
    pub async fn beat(&self, worker_id: &str, concurrency: i32, ttl_seconds: u64) -> Result<(), QueueError> {
        let mut conn = self.conn.clone();
        let value = serde_json::json!({ "concurrency": concurrency }).to_string();
        conn.set_ex::<_, _, ()>(heartbeat_key(worker_id), value, ttl_seconds)
            .await?;
        Ok(())
    }

    /// Fase 6: borra el heartbeat de un worker que se está bajando de
    /// forma prolija. Sin esto, `GET /workers` seguiría mostrándolo como
    /// "vivo" hasta que venza el TTL (hasta 3x `HEARTBEAT_INTERVAL_MS`) aun
    /// cuando el worker ya cerró la conexión de manera ordenada y no hace
    /// falta esperar nada.
    pub async fn forget(&self, worker_id: &str) -> Result<(), QueueError> {
        let mut conn = self.conn.clone();
        conn.del::<_, ()>(heartbeat_key(worker_id)).await?;
        Ok(())
    }

    /// IDs de los workers que laten actualmente. `SCAN` en vez de `KEYS`
    /// para no bloquear Redis con un dataset grande -- acá con unos pocos
    /// workers da lo mismo, pero es el hábito correcto.
    pub async fn list_alive(&self) -> Result<Vec<String>, QueueError> {
        let mut conn = self.conn.clone();
        let mut ids = Vec::new();
        let mut iter: redis::AsyncIter<'_, String> = conn.scan_match("worker:heartbeat:*").await?;
        while let Some(key) = iter.next().await {
            if let Some(id) = key.strip_prefix("worker:heartbeat:") {
                ids.push(id.to_string());
            }
        }
        Ok(ids)
    }
}
