//! Manejo de API keys: generación, hashing y verificación (ver ADR-007).
//!
//! Formato de una key: `dq_<prefijo_8_chars>_<secreto_32_bytes_base64url>`.
//! En la base solo vive el prefijo (en claro, para identificar la key en
//! logs o listados sin exponerla) y el hash de la key completa. La key en
//! texto plano existe únicamente en la respuesta de creación: después de
//! eso ni la API ni la base pueden volver a mostrarla.
//!
//! Deliberadamente NO se usa Argon2 ni bcrypt para el hash: son algoritmos
//! pensados para contraseñas de baja entropía elegidas por humanos, donde
//! el costo computacional deliberado sirve para frenar fuerza bruta. Una
//! API key es un secreto de alta entropía generado aleatoriamente; un hash
//! rápido y fuerte como SHA-256 alcanza y sobra, y no agrega latencia
//! artificial a cada request autenticada (ver ADR-007).

use std::fmt;
use std::str::FromStr;

use base64ct::{Base64UrlUnpadded, Encoding};
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Boca de la key que la identifica como una API key de este sistema.
pub const KEY_MARKER: &str = "dq_";
/// Largo del prefijo en claro, único por key.
pub const PREFIX_LEN: usize = 8;
/// Entropía del secreto: 32 bytes (256 bits).
const SECRET_BYTES: usize = 32;

/// Rol que una API key habilita. La matriz de permisos por endpoint vive
/// en la capa de rutas de la API (ver `crates/api/src/app.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiKeyRole {
    Producer,
    Worker,
    Admin,
}

impl ApiKeyRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            ApiKeyRole::Producer => "producer",
            ApiKeyRole::Worker => "worker",
            ApiKeyRole::Admin => "admin",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "producer" => Some(ApiKeyRole::Producer),
            "worker" => Some(ApiKeyRole::Worker),
            "admin" => Some(ApiKeyRole::Admin),
            _ => None,
        }
    }
}

impl fmt::Display for ApiKeyRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// El rol se persiste como TEXT en `api_keys`; estos impl hacen que sqlx
/// lo mapee de ida y vuelta de forma transparente, sin campos intermedios
/// de tipo String en las filas leídas.
impl sqlx::Type<sqlx::Postgres> for ApiKeyRole {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <String as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ApiKeyRole {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        ApiKeyRole::parse(&s).ok_or_else(|| format!("rol inválido en base de datos: '{s}'").into())
    }
}

impl FromStr for ApiKeyRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ApiKeyRole::parse(s).ok_or_else(|| format!("rol inválido '{s}' (esperado: producer, worker o admin)"))
    }
}

/// La key completa en texto plano. Se muestra una única vez, en la
/// respuesta de creación (ver `Storage::create_api_key`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeySecret {
    full: String,
}

impl ApiKeySecret {
    pub fn as_str(&self) -> &str {
        &self.full
    }

    /// Prefijo en claro (el segmento que además se persiste sin hashear).
    pub fn prefix(&self) -> &str {
        &self.full[KEY_MARKER.len()..KEY_MARKER.len() + PREFIX_LEN]
    }

    /// Hash SHA-256 (hex) de la key completa, tal como se persiste.
    pub fn hash(&self) -> String {
        hash_api_key(self.as_str())
    }
}

impl fmt::Display for ApiKeySecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.full)
    }
}

/// Registro público de una key: lo que un listado de admin puede ver.
/// A propósito NO incluye `key_hash`, el secreto, ni nada que permita
/// reconstruir la key completa.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ApiKeyRecord {
    pub id: Uuid,
    pub name: String,
    pub key_prefix: String,
    pub role: ApiKeyRole,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// Fila interna usada en el camino de verificación. Contiene el hash;
/// no debe salir del proceso de la API.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct StoredApiKey {
    pub id: Uuid,
    pub key_prefix: String,
    pub key_hash: String,
    pub role: ApiKeyRole,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Genera una key nueva con 256 bits de entropía.
pub fn generate() -> ApiKeySecret {
    let mut rng = rand::rngs::OsRng;
    let mut secret = [0u8; SECRET_BYTES];
    rng.fill_bytes(&mut secret);

    // Prefijo alfanumérico corto, fácil de leer y de tipear a mano en un
    // listado. No necesita entropía de secreto: su función es identificación.
    const CHARSET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut prefix = [0u8; PREFIX_LEN];
    for byte in prefix.iter_mut() {
        *byte = CHARSET[(rng.next_u32() as usize) % CHARSET.len()];
    }

    // base64ct se usa sin la feature `alloc` (encode_string no está
    // disponible); un buffer en stack alcanza: 32 bytes -> 43 chars. El
    // buffer vive hasta que se arma la key completa.
    let mut buf = [0u8; 48];
    let encoded = Base64UrlUnpadded::encode(&secret, &mut buf)
        .expect("48 bytes alcanzan para 32 bytes de entrada");
    ApiKeySecret {
        full: format!("{KEY_MARKER}{}_{encoded}", std::str::from_utf8(&prefix).expect("prefix es ASCII")),
    }
}

/// Hash SHA-256 (hex) de la key completa. Rápido a propósito: ver ADR-007.
pub fn hash_api_key(key: &str) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(key.as_bytes());
    to_hex(&hasher.finalize())
}

/// Verifica que `key` corresponda al hash almacenado. La comparación es en
/// tiempo constante para no filtrar por timing cuántos bytes de un intento
/// de adivinanza coinciden con la key real.
pub fn verify_api_key(key: &str, stored_hash: &str) -> bool {
    let candidate = hash_api_key(key);
    let a = candidate.as_bytes();
    let b = stored_hash.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Valida el formato de una key recibida (`dq_<prefijo>_<secreto>`) y
/// devuelve su prefijo. No se hashea la key si el prefijo no existe: el
/// lookup por prefijo descarta rápido las keys desconocidas o malescritas.
pub fn parse_key(raw: &str) -> Option<&str> {
    let bytes = raw.as_bytes();
    let rest = raw.strip_prefix(KEY_MARKER)?;
    let start = KEY_MARKER.len();
    let end = start + PREFIX_LEN;
    if bytes.len() < end + 2 {
        return None;
    }
    if rest.as_bytes()[PREFIX_LEN..PREFIX_LEN + 1] != *b"_" {
        return None;
    }
    if !raw[start..end].bytes().all(|b| b.is_ascii_alphanumeric()) {
        return None;
    }
    Some(&raw[start..end])
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_key_has_expected_format_and_prefix() {
        let key = generate();
        let full = key.as_str();
        assert!(full.starts_with(KEY_MARKER));
        assert!(full.len() > KEY_MARKER.len() + PREFIX_LEN + 1);
        assert_eq!(key.prefix().len(), PREFIX_LEN);
        assert!(key.prefix().bytes().all(|b| b.is_ascii_alphanumeric()));
        assert_eq!(parse_key(full), Some(key.prefix()));
    }

    #[test]
    fn parse_key_rejects_malformed_keys() {
        assert_eq!(parse_key(""), None);
        assert_eq!(parse_key("bearer abc"), None);
        assert_eq!(parse_key("dq_short_abc"), None);
        assert_eq!(parse_key("dq_abcd_efgh_abc"), None);
        assert_eq!(parse_key("dq_ABCDEFGH_"), None);
        assert_eq!(parse_key(&format!("dq_{}_abc", "!@#$%^&*")), None);
    }

    #[test]
    fn hash_is_stable_and_prefix_is_not_in_clear_in_hash() {
        let key = generate();
        let h1 = hash_api_key(key.as_str());
        let h2 = hash_api_key(key.as_str());
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
        assert!(!h1.contains(key.prefix()));
    }

    #[test]
    fn verify_matches_only_the_exact_key() {
        let key = generate();
        assert!(verify_api_key(key.as_str(), &key.hash()));
        assert!(!verify_api_key("dq_XXXXXXXX_otrakeytotalmente", &key.hash()));
    }
}