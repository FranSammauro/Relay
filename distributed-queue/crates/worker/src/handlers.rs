use serde_json::Value;

/// Registro de handlers por tipo de job (ver sección "Casos de uso" del
/// informe técnico). En Fase 1 los handlers son implementaciones simples que
/// demuestran el mecanismo de dispatch; la lógica real de negocio (resize,
/// email, etc.) es intencionalmente un stub.
pub async fn execute(job_type: &str, payload: &Value) -> Result<(), String> {
    match job_type {
        "resize_image" => resize_image(payload).await,
        "send_email" => send_email(payload).await,
        "generate_report" => generate_report(payload).await,
        "noop" => Ok(()),
        other => Err(format!("no handler registered for job type '{other}'")),
    }
}

async fn resize_image(payload: &Value) -> Result<(), String> {
    let width = payload.get("width").and_then(|v| v.as_i64());
    let height = payload.get("height").and_then(|v| v.as_i64());

    if width.is_none() || height.is_none() {
        return Err("resize_image requires 'width' and 'height'".to_string());
    }

    // Simula trabajo de I/O.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(())
}

async fn send_email(payload: &Value) -> Result<(), String> {
    if payload.get("to").is_none() {
        return Err("send_email requires 'to'".to_string());
    }
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    Ok(())
}

async fn generate_report(_payload: &Value) -> Result<(), String> {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    Ok(())
}
