//! Parser de expresiones cron minimalista, hecho a mano.
//!
//! Deliberadamente NO usamos la crate `cron` de crates.io: ya nos comimos
//! suficientes vueltas de pines de versión por el tema del rustc viejo (ver
//! comentarios en Cargo.toml) como para sumar una dependencia más solo para
//! resolver un problema chico y bien acotado. Calcular "próxima vez que
//! matchea esta expresión" es un ejercicio de una tarde, no una pieza de
//! infraestructura -- y tenerlo acá adentro significa que no hay que volver
//! a pelear con MSRV cada vez que esa crate suba de versión.
//!
//! Soporta el formato estándar de 5 campos (sin segundos):
//! `minuto hora dia-del-mes mes dia-de-semana`, con `*`, listas (`1,2,3`),
//! rangos (`1-5`) y pasos (`*/15`, `1-30/5`). No soporta alias de texto
//! (`JAN`, `MON`) ni las extensiones no estándar (`@daily`, `L`, `W`, `#`)
//! -- si hace falta eso, ahí sí vale la pena traer una crate hecha y
//! derecha en vez de seguir haciendo crecer esto a mano.

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronError(String);

impl fmt::Display for CronError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid cron expression: {}", self.0)
    }
}

impl std::error::Error for CronError {}

#[derive(Debug, Clone)]
struct Field {
    // bitset de valores válidos. 64 bits alcanza y sobra para cualquiera de
    // los 5 campos (el más grande, día del mes, llega a 31).
    allowed: u64,
}

impl Field {
    fn matches(&self, value: u32) -> bool {
        self.allowed & (1 << value) != 0
    }

    fn parse(spec: &str, min: u32, max: u32) -> Result<Self, CronError> {
        let mut allowed: u64 = 0;

        for part in spec.split(',') {
            let (range_part, step) = match part.split_once('/') {
                Some((r, s)) => (
                    r,
                    s.parse::<u32>()
                        .map_err(|_| CronError(format!("paso inválido: '{s}'")))?,
                ),
                None => (part, 1),
            };

            let (start, end) = if range_part == "*" {
                (min, max)
            } else if let Some((a, b)) = range_part.split_once('-') {
                let a: u32 = a.parse().map_err(|_| CronError(format!("rango inválido: '{part}'")))?;
                let b: u32 = b.parse().map_err(|_| CronError(format!("rango inválido: '{part}'")))?;
                (a, b)
            } else {
                let v: u32 = range_part
                    .parse()
                    .map_err(|_| CronError(format!("valor inválido: '{part}'")))?;
                (v, v)
            };

            if start < min || end > max || start > end {
                return Err(CronError(format!(
                    "'{part}' fuera de rango ({min}-{max})"
                )));
            }

            let mut v = start;
            while v <= end {
                allowed |= 1 << v;
                v += step;
            }
        }

        Ok(Field { allowed })
    }
}

#[derive(Debug, Clone)]
pub struct CronExpr {
    minute: Field,
    hour: Field,
    day_of_month: Field,
    month: Field,
    day_of_week: Field,
}

/// Techo de búsqueda hacia adelante: si en 4 años no encontramos una
/// ocurrencia, la expresión es irrealizable (ej: 31 de febrero) y es mejor
/// devolver None de forma explícita que loopear para siempre.
const MAX_LOOKAHEAD_MINUTES: i64 = 4 * 365 * 24 * 60;

impl CronExpr {
    pub fn parse(expr: &str) -> Result<Self, CronError> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(CronError(format!(
                "se esperaban 5 campos (min hora dia-mes mes dia-semana), se recibieron {}",
                fields.len()
            )));
        }

        Ok(CronExpr {
            minute: Field::parse(fields[0], 0, 59)?,
            hour: Field::parse(fields[1], 0, 23)?,
            day_of_month: Field::parse(fields[2], 1, 31)?,
            month: Field::parse(fields[3], 1, 12)?,
            // domingo=0 igual que cron estándar (0 y 7 son ambos domingo,
            // pero para simplificar solo aceptamos 0-6 -- documentado).
            day_of_week: Field::parse(fields[4], 0, 6)?,
        })
    }

    /// Próxima vez que la expresión matchea, estrictamente después de
    /// `after`. Escanea minuto a minuto -- nada elegante, pero para un
    /// scheduler que corre cada `SCHEDULER_INTERVAL_MS` (segundos, no
    /// microsegundos) esto no es un hot path que necesite ser inteligente.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // arrancamos en el próximo minuto exacto, sin segundos/nanos --
        // cron no tiene resolución de segundos.
        let start = (after + Duration::minutes(1))
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();

        let mut candidate = start;
        for _ in 0..MAX_LOOKAHEAD_MINUTES {
            if self.matches(candidate) {
                return Some(candidate);
            }
            candidate += Duration::minutes(1);
        }
        None
    }

    fn matches(&self, dt: DateTime<Utc>) -> bool {
        // día del mes y día de semana: si ambos están restringidos (no son
        // "*"), cron estándar los trata como OR, no AND -- así es como lo
        // interpreta vixie-cron y es la convención que la gente espera.
        let dom_wildcard = self.day_of_month.allowed == full_mask(1, 31);
        let dow_wildcard = self.day_of_week.allowed == full_mask(0, 6);

        let day_matches = if dom_wildcard || dow_wildcard {
            self.day_of_month.matches(dt.day()) && self.day_of_week.matches(dt.weekday().num_days_from_sunday())
        } else {
            self.day_of_month.matches(dt.day()) || self.day_of_week.matches(dt.weekday().num_days_from_sunday())
        };

        self.minute.matches(dt.minute())
            && self.hour.matches(dt.hour())
            && self.month.matches(dt.month())
            && day_matches
    }
}

fn full_mask(min: u32, max: u32) -> u64 {
    let mut m = 0u64;
    for v in min..=max {
        m |= 1 << v;
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn dt(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    #[test]
    fn every_minute() {
        let s = CronExpr::parse("* * * * *").unwrap();
        let next = s.next_after(dt(2026, 1, 1, 10, 30)).unwrap();
        assert_eq!(next, dt(2026, 1, 1, 10, 31));
    }

    #[test]
    fn every_hour_on_the_hour() {
        let s = CronExpr::parse("0 * * * *").unwrap();
        let next = s.next_after(dt(2026, 1, 1, 10, 15)).unwrap();
        assert_eq!(next, dt(2026, 1, 1, 11, 0));
    }

    #[test]
    fn daily_at_specific_time() {
        let s = CronExpr::parse("30 4 * * *").unwrap();
        let next = s.next_after(dt(2026, 1, 1, 10, 0)).unwrap();
        assert_eq!(next, dt(2026, 1, 2, 4, 30));
    }

    #[test]
    fn daily_at_specific_time_same_day_if_still_ahead() {
        let s = CronExpr::parse("30 4 * * *").unwrap();
        let next = s.next_after(dt(2026, 1, 1, 0, 0)).unwrap();
        assert_eq!(next, dt(2026, 1, 1, 4, 30));
    }

    #[test]
    fn every_15_minutes() {
        let s = CronExpr::parse("*/15 * * * *").unwrap();
        let next = s.next_after(dt(2026, 1, 1, 10, 16)).unwrap();
        assert_eq!(next, dt(2026, 1, 1, 10, 30));
    }

    #[test]
    fn specific_weekday() {
        // lunes a las 9:00 -- 2026-01-01 es jueves
        let s = CronExpr::parse("0 9 * * 1").unwrap();
        let next = s.next_after(dt(2026, 1, 1, 0, 0)).unwrap();
        assert_eq!(next.weekday().num_days_from_sunday(), 1);
        assert_eq!(next, dt(2026, 1, 5, 9, 0));
    }

    #[test]
    fn range_of_hours() {
        let s = CronExpr::parse("0 9-17 * * *").unwrap();
        let next = s.next_after(dt(2026, 1, 1, 18, 0)).unwrap();
        assert_eq!(next, dt(2026, 1, 2, 9, 0));
    }

    #[test]
    fn list_of_months() {
        let s = CronExpr::parse("0 0 1 1,7 *").unwrap();
        let next = s.next_after(dt(2026, 2, 1, 0, 0)).unwrap();
        assert_eq!(next, dt(2026, 7, 1, 0, 0));
    }

    #[test]
    fn invalid_field_count() {
        assert!(CronExpr::parse("* * * *").is_err());
    }

    #[test]
    fn invalid_range() {
        assert!(CronExpr::parse("70 * * * *").is_err());
    }

    #[test]
    fn day_of_month_and_day_of_week_are_or_when_both_restricted() {
        // día 1 del mes, O lunes -- convención estándar de cron
        let s = CronExpr::parse("0 0 1 * 1").unwrap();
        // 2026-01-05 es lunes pero no es día 1 -- debería matchear igual
        let next = s.next_after(dt(2026, 1, 2, 0, 0)).unwrap();
        assert_eq!(next, dt(2026, 1, 5, 0, 0));
    }
}
