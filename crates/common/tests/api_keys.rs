//! Tests de integración para API keys (requiere Postgres corriendo).

use common::{generate_api_key, parse_key, verify_api_key, Storage};
use std::env;

async fn connect_or_skip() -> Option<Storage> {
    let url = env::var("DATABASE_URL").ok()?;
    match Storage::connect(&url).await {
        Ok(s) => {
            s.migrate().await.ok()?;
            Some(s)
        }
        Err(_) => None,
    }
}

#[tokio::test]
async fn api_key_secret_not_recoverable_from_db() {
    let Some(storage) = connect_or_skip().await else {
        eprintln!("saltando test: sin DATABASE_URL o Postgres no disponible");
        return;
    };

    let (record, _secret) = storage.create_api_key("test-key", common::ApiKeyRole::Producer).await
        .expect("create api key");

    // El secreto completo NO está en la DB, solo el hash. El prefijo
    // persistido son los 8 caracteres alfanuméricos generados por
    // generate(), sin el marcador "dq_" (ver ApiKeySecret::prefix() en
    // common::api_keys): el marcador identifica el formato de la key
    // completa, no forma parte del prefijo que se guarda ni por el que se
    // busca en la base.
    assert!(!record.key_prefix.is_empty());
    assert_eq!(record.key_prefix.len(), 8);

    // El hash no permite recuperar el secreto.
    // ApiKeyRecord no tiene key_hash (es privado en StoredApiKey); solo ApiKeySecret.hash() lo da.
    // Esto verifica que al serializar ApiKeyRecord no se filtra el hash ni el secreto.
    let serialized = serde_json::to_string(&record).expect("serialize");
    assert!(!serialized.contains("hash"));
    assert!(!serialized.contains("secret"));
    assert!(serialized.contains(&record.key_prefix)); // el prefijo sí se serializa
}

#[tokio::test]
async fn api_key_verify_roundtrip() {
    let Some(storage) = connect_or_skip().await else {
        eprintln!("saltando test: sin DATABASE_URL o Postgres no disponible");
        return;
    };

    let (record, secret) = storage.create_api_key("roundtrip-key", common::ApiKeyRole::Worker).await
        .expect("create api key");

    // Fetch stored record to get the hash (key_prefix is just the prefix, not the hash)
    let stored = storage.find_api_key_by_prefix(&record.key_prefix).await
        .expect("lookup")
        .expect("found");
    assert!(verify_api_key(secret.as_str(), &stored.key_hash));
}

#[tokio::test]
async fn api_key_revoke() {
    let Some(storage) = connect_or_skip().await else {
        eprintln!("saltando test: sin DATABASE_URL o Postgres no disponible");
        return;
    };

    let (record, _secret) = storage.create_api_key("revoke-key", common::ApiKeyRole::Admin).await
        .expect("create api key");

    // Verificar que existe y está activa
    let found = storage.find_api_key_by_prefix(&record.key_prefix).await
        .expect("lookup")
        .expect("found");
    assert!(found.revoked_at.is_none());

    // Revocar.
    let revoked = storage.revoke_api_key_by_prefix(&record.key_prefix).await
        .expect("revoke");
    assert!(revoked);

    // find_api_key_by_prefix devuelve la fila igual, revocada o no: el
    // filtrado de revocadas es responsabilidad de quien llama (el
    // extractor de autenticación en crates/api/src/auth.rs), no de esta
    // consulta. Esto es deliberado: permite distinguir en el caller entre
    // "no existe" y "existe pero está revocada" sin tener que hacer una
    // segunda consulta, y ambos casos terminan devolviendo el mismo 401
    // hacia afuera para no filtrar información sobre cuál fue el motivo.
    let found_after = storage.find_api_key_by_prefix(&record.key_prefix).await
        .expect("lookup after revoke")
        .expect("la fila debería seguir encontrándose, ahora marcada como revocada");
    assert!(found_after.revoked_at.is_some());

    // Listar también debe devolver la key con su marca de revocación.
    let all = storage.list_api_keys().await.expect("list");
    let listed = all.iter().find(|k| k.key_prefix == record.key_prefix).expect("listed");
    assert!(listed.revoked_at.is_some());
}

#[tokio::test]
async fn api_key_generate_format_and_uniqueness() {
    // Test que generate_api_key produce formato correcto y prefijos únicos
    let mut prefixes = std::collections::HashSet::new();
    for _ in 0..100 {
        let secret = generate_api_key();
        assert!(secret.as_str().starts_with("dq_"));
        // split solo en los dos primeros '_' (marker y separador prefijo/secreto)
        let without_marker = &secret.as_str()[3..]; // quitar "dq_"
        let parts: Vec<&str> = without_marker.splitn(2, '_').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].len(), 8); // prefijo 8 chars
        // secreto base64url de 32 bytes ~ 43 chars (puede contener '_')
        assert!(parts[1].len() >= 43);
        prefixes.insert(secret.prefix().to_string());
    }
    assert_eq!(prefixes.len(), 100); // todos únicos
}

#[tokio::test]
async fn api_key_parse_rejects_malformed() {
    // parse_key solo valida el prefijo (8 alphanum después de "dq_" y antes de "_")
    // NO valida la longitud del secreto (eso lo hace verify_api_key comparando hash)
    
    // Válidos (prefijo correcto)
    assert!(parse_key("dq_abcdefgh_abcdefghijklmnopqrstuvwxyz1234567890").is_some());
    assert!(parse_key("dq_abc12345_short").is_some()); // prefijo válido, secreto corto -> OK para parse_key
    
    // Inválidos (prefijo malformado)
    assert!(parse_key("dq_abc12345678901234567890").is_none()); // solo prefijo largo, sin _
    assert!(parse_key("dq_abc12345678901234567890_").is_none()); // prefijo 20 chars
    assert!(parse_key("wrong_prefix_abc123456789012345678901234567890").is_none()); // marker erróneo
    assert!(parse_key("").is_none());
    assert!(parse_key("dq_").is_none());
    assert!(parse_key("dq_abc12345").is_none()); // sin separador _
    assert!(parse_key("dq_abc-123_secret").is_none()); // guion en prefijo (no alfanum)
}

#[tokio::test]
async fn api_key_hash_stability_and_consttime() {
    let key = "dq_abcdefgh_abcdefghijklmnopqrstuvwxyz1234567890";
    let h1 = common::api_keys::hash_api_key(key); // using internal since not exported
    let h2 = common::api_keys::hash_api_key(key);
    assert_eq!(h1, h2); // determinista
    assert_eq!(h1.len(), 64); // SHA-256 hex

    // Verificación constante-time: no paniquea ni falla por timing
    assert!(verify_api_key(key, &h1));
    assert!(!verify_api_key(key, &"0".repeat(64)));
    assert!(!verify_api_key("dq_xxxxxxxx_wrongsecret12345678901234567890", &h1));
}