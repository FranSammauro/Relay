//! Fase 6: manejo de cierre ordenado (graceful shutdown). Se ubica en
//! `common` porque tanto `api` como `worker` requieren exactamente el
//! mismo comportamiento: esperar a SIGTERM, la señal que Docker o
//! Kubernetes envían al detener un contenedor, o a Ctrl+C durante el
//! desarrollo local, sin duplicar la lógica de `select!` en cada binario.

/// Se resuelve cuando llega SIGTERM o Ctrl+C. Pensado para utilizarse
/// directamente con `axum::serve(...).with_graceful_shutdown(...)`, o
/// dentro de un `tokio::select!` en cualquier otro ciclo de fondo.
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
