//! CLI de operación para el queue. Se comunica directamente con PostgreSQL
//! a través de `common::Storage`, de la misma manera que `api` y
//! `worker`, sin pasar por HTTP.
//!
//! Se decidió no utilizar `clap` ni ningún otro parser de argumentos de
//! terceros. Los comandos requeridos son pocos, y el parsing manual
//! representa una tarea acotada, no una pieza de infraestructura; el
//! mismo criterio aplicado al parser de cron de la Fase 5. Como ventaja
//! adicional, la CLI permanece operativa aunque la API esté caída, ya que
//! no depende de ella en ningún momento.

use common::{ApiKeyRole, NewCronSchedule, NewJob, Storage};
use std::process::ExitCode;
use uuid::Uuid;

#[tokio::main]
async fn main() -> ExitCode {
    dotenvy::dotenv().ok();

    let args: Vec<String> = std::env::args().skip(1).collect();

    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Vec<String>) -> anyhow::Result<()> {
    let mut args = args.into_iter();
    let command = args.next().unwrap_or_default();

    if command.is_empty() || command == "help" || command == "--help" || command == "-h" {
        print_help();
        return Ok(());
    }

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://queue:queue@localhost:5432/queue".to_string());
    let storage = Storage::connect(&database_url).await?;

    let subcommand = args.next().unwrap_or_default();
    let rest: Vec<String> = args.collect();

    match (command.as_str(), subcommand.as_str()) {
        ("jobs", "list") => jobs_list(&storage, &rest).await,
        ("jobs", "get") => jobs_get(&storage, &rest).await,
        ("jobs", "cancel") => jobs_cancel(&storage, &rest).await,
        ("jobs", "attempts") => jobs_attempts(&storage, &rest).await,
        ("stats", _) => stats(&storage).await,
        ("workers", _) => workers(&storage).await,
        ("cron", "list") => cron_list(&storage).await,
        ("cron", "create") => cron_create(&storage, &rest).await,
        ("cron", "delete") => cron_delete(&storage, &rest).await,
        ("api-key", "create") => api_key_create(&storage, &rest).await,
        ("api-key", "list") => api_key_list(&storage).await,
        ("api-key", "revoke") => api_key_revoke(&storage, &rest).await,
        ("bench", _) => {
            // "bench" no tiene subcomando propio, solo flags (--jobs, etc).
            // El parser general asume comando + subcomando + resto, así
            // que acá se reconstruye la lista completa de argumentos antes
            // de pasarla a la función.
            let mut bench_args = Vec::new();
            if !subcommand.is_empty() {
                bench_args.push(subcommand.clone());
            }
            bench_args.extend(rest.iter().cloned());
            bench(&storage, &bench_args).await
        }
        _ => {
            print_help();
            anyhow::bail!("comando desconocido: '{command} {subcommand}'");
        }
    }
}

fn print_help() {
    println!(
        r#"relay-cli: operación directa sobre el motor de colas distribuidas Relay (se comunica con PostgreSQL, no con la API)

USO:
  relay-cli jobs list [--status <status>] [--limit <n>]
  relay-cli jobs get <id>
  relay-cli jobs attempts <id>
  relay-cli jobs cancel <id>
  relay-cli stats
  relay-cli workers
  relay-cli cron list
  relay-cli cron create --name <name> --expr <cron-expr> --type <job-type> [--payload <json>] [--priority <n>] [--max-attempts <n>] [--timeout <seconds>]
  relay-cli cron delete <id>
  relay-cli api-key create --name <name> --role <producer|worker|admin>
  relay-cli api-key list
  relay-cli api-key revoke <prefijo>
  relay-cli bench [--jobs <n>] [--type <job-type>] [--timeout-secs <n>]

Variables de entorno:
  DATABASE_URL   default: postgres://queue:queue@localhost:5432/queue
"#
    );
}

// ---- helpers de parsing de flags ---------------------------------------

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn positional(args: &[String]) -> Option<&str> {
    args.iter().find(|a| !a.starts_with("--")).map(|s| s.as_str())
}

fn parse_uuid_arg(args: &[String], what: &str) -> anyhow::Result<Uuid> {
    let raw = positional(args).ok_or_else(|| anyhow::anyhow!("falta el id de {what}"))?;
    Uuid::parse_str(raw).map_err(|_| anyhow::anyhow!("'{raw}' no es un UUID válido"))
}

// ---- jobs ---------------------------------------------------------------

async fn jobs_list(storage: &Storage, args: &[String]) -> anyhow::Result<()> {
    let status = flag(args, "--status");
    let limit: i64 = flag(args, "--limit").and_then(|v| v.parse().ok()).unwrap_or(20);

    let jobs = storage.list_jobs(status.as_deref(), limit).await?;
    if jobs.is_empty() {
        println!("(sin jobs)");
        return Ok(());
    }

    println!("{:<36}  {:<20}  {:<16}  {:>7}  {:<20}", "ID", "TIPO", "ESTADO", "INTENTO", "CREADO");
    for j in jobs {
        println!(
            "{:<36}  {:<20}  {:<16}  {:>3}/{:<3}  {:<20}",
            j.id,
            truncate(&j.job_type, 20),
            j.status.as_str(),
            j.attempts,
            j.max_attempts,
            j.created_at.format("%Y-%m-%d %H:%M:%S")
        );
    }
    Ok(())
}

async fn jobs_get(storage: &Storage, args: &[String]) -> anyhow::Result<()> {
    let id = parse_uuid_arg(args, "job")?;
    let job = storage.get_job(id).await?.ok_or_else(|| anyhow::anyhow!("job {id} no encontrado"))?;
    println!("{}", serde_json::to_string_pretty(&job)?);
    Ok(())
}

async fn jobs_attempts(storage: &Storage, args: &[String]) -> anyhow::Result<()> {
    let id = parse_uuid_arg(args, "job")?;
    let attempts = storage.list_attempts(id).await?;
    if attempts.is_empty() {
        println!("(sin intentos todavía)");
        return Ok(());
    }
    println!("{:>3}  {:<20}  {:<12}  {:<20}  {}", "#", "WORKER", "RESULTADO", "INICIO", "ERROR");
    for a in attempts {
        println!(
            "{:>3}  {:<20}  {:<12}  {:<20}  {}",
            a.attempt_number,
            truncate(&a.worker_id, 20),
            a.status.as_deref().unwrap_or("-"),
            a.started_at.format("%Y-%m-%d %H:%M:%S"),
            a.error.as_deref().unwrap_or("")
        );
    }
    Ok(())
}

async fn jobs_cancel(storage: &Storage, args: &[String]) -> anyhow::Result<()> {
    let id = parse_uuid_arg(args, "job")?;
    if storage.cancel_job(id).await? {
        println!("job {id} cancelado");
        Ok(())
    } else {
        anyhow::bail!("job {id} no se pudo cancelar (¿ya no está pending?)");
    }
}

// ---- stats / workers ----------------------------------------------------

async fn stats(storage: &Storage) -> anyhow::Result<()> {
    let counts = storage.count_by_status().await?;
    if counts.is_empty() {
        println!("(sin datos)");
        return Ok(());
    }
    for c in counts {
        println!("{:<16} {}", c.status, c.count);
    }
    Ok(())
}

async fn workers(storage: &Storage) -> anyhow::Result<()> {
    // La CLI se comunica directamente con PostgreSQL y no necesita
    // establecer una conexión a Redis únicamente para este propósito: se
    // muestra el registro histórico (Fase 2). Para conocer quién está
    // activo en este momento, `GET /workers` en la API sigue siendo la
    // fuente completa, ya que combina esta información con los
    // heartbeats.
    let workers = storage.list_workers().await?;
    if workers.is_empty() {
        println!("(sin workers registrados)");
        return Ok(());
    }
    println!("{:<24}  {:>11}  {}", "ID", "CONCURRENCY", "REGISTRADO DESDE");
    for w in workers {
        println!(
            "{:<24}  {:>11}  {}",
            truncate(&w.id, 24),
            w.concurrency,
            w.started_at.format("%Y-%m-%d %H:%M:%S")
        );
    }
    println!("\n(para ver quién está vivo ahora mismo, usá GET /workers en la API)");
    Ok(())
}

// ---- cron -----------------------------------------------------------------

async fn cron_list(storage: &Storage) -> anyhow::Result<()> {
    let schedules = storage.list_cron_schedules().await?;
    if schedules.is_empty() {
        println!("(sin cron schedules)");
        return Ok(());
    }
    println!("{:<24}  {:<14}  {:<20}  {:<20}", "NOMBRE", "EXPR", "TIPO", "PRÓXIMA CORRIDA");
    for s in schedules {
        println!(
            "{:<24}  {:<14}  {:<20}  {}",
            truncate(&s.name, 24),
            s.cron_expr,
            truncate(&s.job_type, 20),
            s.next_run_at.format("%Y-%m-%d %H:%M:%S")
        );
    }
    Ok(())
}

async fn cron_create(storage: &Storage, args: &[String]) -> anyhow::Result<()> {
    let name = flag(args, "--name").ok_or_else(|| anyhow::anyhow!("falta --name"))?;
    let cron_expr = flag(args, "--expr").ok_or_else(|| anyhow::anyhow!("falta --expr"))?;
    let job_type = flag(args, "--type").ok_or_else(|| anyhow::anyhow!("falta --type"))?;
    let payload = match flag(args, "--payload") {
        Some(raw) => serde_json::from_str(&raw)?,
        None => serde_json::json!({}),
    };
    let priority = flag(args, "--priority").and_then(|v| v.parse().ok()).unwrap_or(50);
    let max_attempts = flag(args, "--max-attempts").and_then(|v| v.parse().ok()).unwrap_or(5);
    let timeout_seconds = flag(args, "--timeout").and_then(|v| v.parse().ok()).unwrap_or(30);

    let schedule = storage
        .create_cron_schedule(NewCronSchedule {
            name,
            cron_expr,
            job_type,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
        })
        .await?;

    println!("creado: {} (id {})", schedule.name, schedule.id);
    println!("próxima corrida: {}", schedule.next_run_at.format("%Y-%m-%d %H:%M:%S %Z"));
    Ok(())
}

async fn cron_delete(storage: &Storage, args: &[String]) -> anyhow::Result<()> {
    let id = parse_uuid_arg(args, "cron schedule")?;
    if storage.delete_cron_schedule(id).await? {
        println!("cron schedule {id} eliminado");
        Ok(())
    } else {
        anyhow::bail!("cron schedule {id} no encontrado");
    }
}

// ---- api keys (Fase 8) ---------------------------------------------------

/// Genera una API key `dq_<prefijo>_<secreto>` y la persiste. La base solo
/// guarda el prefijo y el hash SHA-256; la key completa se imprime una
/// única vez y no se puede volver a recuperar.
async fn api_key_create(storage: &Storage, args: &[String]) -> anyhow::Result<()> {
    let name = flag(args, "--name").ok_or_else(|| anyhow::anyhow!("falta --name"))?;
    let role_raw = flag(args, "--role").ok_or_else(|| anyhow::anyhow!("falta --role (producer|worker|admin)"))?;
    let role: ApiKeyRole = role_raw
        .parse()
        .map_err(|_: String| anyhow::anyhow!("rol inválido: '{role_raw}' (use producer, worker o admin)"))?;

    let (record, secret) = storage.create_api_key(&name, role).await?;

    println!("creada: {} (id {})", record.name, record.id);
    println!("rol: {}", record.role);
    println!("prefijo: {}", record.key_prefix);
    println!();
    println!("clave (se muestra una sola vez, no se puede recuperar):");
    println!("  {}", secret.as_str());
    Ok(())
}

async fn api_key_list(storage: &Storage) -> anyhow::Result<()> {
    let keys = storage.list_api_keys().await?;
    if keys.is_empty() {
        println!("(sin API keys)");
        return Ok(());
    }
    println!("{:<36}  {:<24}  {:<12}  {:<10}  {:<20}", "ID", "NOMBRE", "PREFIJO", "ROL", "ESTADO");
    for k in keys {
        let estado = if k.revoked_at.is_some() { "revocada" } else { "activa" };
        println!(
            "{:<36}  {:<24}  {:<12}  {:<10}  {}",
            k.id,
            truncate(&k.name, 24),
            k.key_prefix,
            k.role,
            estado
        );
    }
    Ok(())
}

async fn api_key_revoke(storage: &Storage, args: &[String]) -> anyhow::Result<()> {
    let prefix = positional(args).ok_or_else(|| anyhow::anyhow!("falta el prefijo de la API key"))?;
    if storage.revoke_api_key_by_prefix(prefix).await? {
        println!("API key con prefijo '{prefix}' revocada");
        Ok(())
    } else {
        anyhow::bail!("no se encontró ninguna API key activa con prefijo '{prefix}'");
    }
}

// ---- bench (Fase 7) --------------------------------------------------

/// Benchmark reproducible de tres latencias distintas del sistema:
///
/// - Envío: tiempo que tarda create_job en persistir un job (INSERT).
///   No incluye el overhead de la capa HTTP de la API, ya que esta CLI
///   habla directo con Postgres; el informe de resultados aclara esta
///   distinción.
/// - Cola: tiempo entre created_at y started_at, es decir, cuánto espera
///   un job hasta que un worker lo reclama. Depende de la concurrencia
///   disponible frente a la tasa de llegada de jobs.
/// - Ejecución: tiempo entre started_at y completed_at.
///
/// Requiere que haya al menos un worker corriendo contra la misma base
/// (`cargo run -p worker` o `docker compose up worker`), ya que la CLI
/// solo envía los jobs; no los ejecuta.
async fn bench(storage: &Storage, args: &[String]) -> anyhow::Result<()> {
    let job_count: usize = flag(args, "--jobs").and_then(|v| v.parse().ok()).unwrap_or(200);
    let job_type = flag(args, "--type").unwrap_or_else(|| "noop".to_string());
    let timeout_secs: u64 = flag(args, "--timeout-secs").and_then(|v| v.parse().ok()).unwrap_or(60);

    let run_id = Uuid::new_v4();
    let idempotency_prefix = format!("bench:{run_id}:");

    println!("enviando {job_count} jobs de tipo '{job_type}' (run_id {run_id})...");

    let mut submission_latencies_ms = Vec::with_capacity(job_count);
    let submission_start = std::time::Instant::now();

    for i in 0..job_count {
        let t0 = std::time::Instant::now();
        storage
            .create_job(NewJob {
                job_type: job_type.clone(),
                payload: serde_json::json!({}),
                priority: 50,
                max_attempts: 3,
                timeout_seconds: 30,
                scheduled_at: None,
                idempotency_key: Some(format!("{idempotency_prefix}{i}")),
            })
            .await?;
        submission_latencies_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
    }

    let submission_wall_secs = submission_start.elapsed().as_secs_f64();
    println!(
        "envío completo en {:.2}s ({:.1} jobs/s de throughput de envío)",
        submission_wall_secs,
        job_count as f64 / submission_wall_secs
    );

    println!("esperando a que los workers procesen los jobs (timeout {timeout_secs}s)...");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    let rows = loop {
        let rows = storage.bench_timestamps(&idempotency_prefix).await?;
        let pending = rows
            .iter()
            .filter(|r| matches!(r.status.as_str(), "pending" | "running" | "retry_scheduled"))
            .count();

        if pending == 0 {
            break rows;
        }
        if std::time::Instant::now() >= deadline {
            println!(
                "atención: se alcanzó el timeout con {pending} jobs todavía sin terminar. \
                 ¿hay algún worker corriendo contra esta base?"
            );
            break rows;
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    };

    let mut queue_latencies_ms = Vec::new();
    let mut execution_latencies_ms = Vec::new();
    let mut total_latencies_ms = Vec::new();
    let mut completed = 0usize;
    let mut dead_lettered = 0usize;

    for r in &rows {
        if let Some(started_at) = r.started_at {
            queue_latencies_ms.push((started_at - r.created_at).num_milliseconds() as f64);
        }
        if let (Some(started_at), Some(completed_at)) = (r.started_at, r.completed_at) {
            execution_latencies_ms.push((completed_at - started_at).num_milliseconds() as f64);
            total_latencies_ms.push((completed_at - r.created_at).num_milliseconds() as f64);
            completed += 1;
        }
        if r.status == "dead_letter" {
            dead_lettered += 1;
        }
    }

    println!("\n--- resultados ({} de {} jobs completados, {} en dead_letter) ---", completed, job_count, dead_lettered);
    print_percentiles("envío (ms)", &submission_latencies_ms);
    print_percentiles("cola (ms)", &queue_latencies_ms);
    print_percentiles("ejecución (ms)", &execution_latencies_ms);
    print_percentiles("total, extremo a extremo (ms)", &total_latencies_ms);

    Ok(())
}

fn print_percentiles(label: &str, values_ms: &[f64]) {
    if values_ms.is_empty() {
        println!("{label:<32} (sin datos)");
        return;
    }
    let mut sorted = values_ms.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let pct = |p: f64| -> f64 {
        let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    };

    println!(
        "{:<32} n={:<6} p50={:>8.1}  p95={:>8.1}  p99={:>8.1}  max={:>8.1}",
        label,
        sorted.len(),
        pct(50.0),
        pct(95.0),
        pct(99.0),
        sorted[sorted.len() - 1]
    );
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}
