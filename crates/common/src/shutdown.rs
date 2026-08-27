//! Fase 6: graceful shutdown. Vive en `common` porque tanto `api` como
//! `worker` necesitan exactamente lo mismo -- esperar a SIGTERM (el que
//! manda Docker/Kubernetes al bajar un contenedor) o Ctrl+C (para
//! desarrollo local), sin duplicar el `select!` en cada binario.

/// Se resuelve cuando llega SIGTERM o Ctrl+C. Pensado para usarse tal cual
/// con `axum::serve(...).with_graceful_shutdown(...)`, o adentro de un
/// `tokio::select!` en cualquier otro loop de fondo.
pub async fn signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("no se pudo instalar el handler de Ctrl+C");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("no se pudo instalar el handler de SIGTERM")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
