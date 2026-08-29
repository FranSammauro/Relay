use crate::error::QueueError;
use futures_util::StreamExt;
use redis::AsyncCommands;

/// Cliente de liveness de workers sobre Redis.
///
/// Se mantiene deliberadamente separado de `Storage`: esto es coordinación
/// efímera (ADR-002), no fuente de verdad. Si Redis deja de estar
/// disponible, se pierde visibilidad sobre quién está activo en ese
/// momento, pero la recuperación de jobs abandonados
/// (`Storage::reap_expired_leases`) continúa funcionando sin cambios, ya
/// que se ejecuta enteramente sobre PostgreSQL y no depende de este
/// componente (ver ADR-002 y ADR-003).
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

    /// Refresca el heartbeat de un worker. La clave expira automáticamente
    /// transcurridos `ttl_seconds`; si el worker se detiene y deja de
    /// invocar este método, no es necesario limpiar nada manualmente, ya
    /// que Redis se encarga de la expiración. Esta es la razón principal
    /// por la que este mecanismo vive en Redis y no en una columna de
    /// `workers` en PostgreSQL: allí sería necesario escribir cada pocos
    /// segundos por worker y además implementar un proceso independiente
    /// para depurar heartbeats vencidos.
    pub async fn beat(&self, worker_id: &str, concurrency: i32, ttl_seconds: u64) -> Result<(), QueueError> {
        let mut conn = self.conn.clone();
        let value = serde_json::json!({ "concurrency": concurrency }).to_string();
        conn.set_ex::<_, _, ()>(heartbeat_key(worker_id), value, ttl_seconds)
            .await?;
        Ok(())
    }

    /// Fase 6: elimina el heartbeat de un worker que se detiene de forma
    /// ordenada. Sin esta llamada, `GET /workers` continuaría mostrándolo
    /// como activo hasta que expire el TTL (hasta tres veces
    /// `HEARTBEAT_INTERVAL_MS`), aun cuando el worker ya cerró la
    /// conexión correctamente y no hay motivo para esperar ese plazo.
    pub async fn forget(&self, worker_id: &str) -> Result<(), QueueError> {
        let mut conn = self.conn.clone();
        conn.del::<_, ()>(heartbeat_key(worker_id)).await?;
        Ok(())
    }

    /// Identificadores de los workers activos en este momento. Se utiliza
    /// `SCAN` en lugar de `KEYS` para no bloquear Redis ante un volumen de
    /// datos elevado. Con la cantidad actual de workers el resultado es
    /// equivalente, pero se mantiene como práctica correcta.
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
