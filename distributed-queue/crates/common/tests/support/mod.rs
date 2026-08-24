//! Helper compartido entre los tests de integración de este crate. No es un
//! test en sí (por eso vive en `tests/support/`, no en `tests/*.rs` directo
//! -- así cargo no lo trata como su propio binario de test).

use common::Storage;

/// Se conecta a Postgres con un timeout corto. Si no hay base disponible,
/// devuelve None en vez de explotar -- el caller decide qué hacer (en
/// nuestro caso, saltar el test con un aviso). Sin esto, correr
/// `cargo test` en una máquina sin Docker levantado tarda 30s por test
/// solo esperando el timeout default de sqlx.
pub async fn connect_or_skip(database_url: &str) -> Option<Storage> {
    match tokio::time::timeout(std::time::Duration::from_secs(3), Storage::connect(database_url))
        .await
    {
        Ok(Ok(storage)) => {
            storage
                .migrate()
                .await
                .expect("las migraciones deberían aplicar limpio");
            Some(storage)
        }
        Ok(Err(e)) => {
            eprintln!(
                "saltando test: no hay Postgres en {database_url} ({e}). \
                 Levantá `docker compose up -d postgres` y corré `cargo test` de nuevo."
            );
            None
        }
        Err(_) => {
            eprintln!(
                "saltando test: timeout conectando a {database_url}. \
                 Levantá `docker compose up -d postgres` y corré `cargo test` de nuevo."
            );
            None
        }
    }
}

pub fn database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://queue:queue@localhost:5432/queue".to_string())
}
