//! CLI de operación para el queue. Habla directo con Postgres a través de
//! `common::Storage`, igual que `api` y `worker` -- no pasa por HTTP.
//!
//! Elección deliberada: nada de `clap` ni ningún parser de argumentos de
//! terceros. Los comandos que necesitamos son pocos y el parsing a mano es
//! una tarde de trabajo, no una pieza de infraestructura -- mismo criterio
//! que con el parser de cron en Fase 5. Ventaja extra: la CLI funciona
//! aunque la API esté caída, porque no depende de ella para nada.

use common::{NewCronSchedule, Storage};
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
        _ => {
            print_help();
            anyhow::bail!("comando desconocido: '{command} {subcommand}'");
        }
    }
}

fn print_help() {
    println!(
        r#"queue-cli -- operación directa del distributed job queue (habla con Postgres, no con la API)

USO:
  queue-cli jobs list [--status <status>] [--limit <n>]
  queue-cli jobs get <id>
  queue-cli jobs attempts <id>
  queue-cli jobs cancel <id>
  queue-cli stats
  queue-cli workers
  queue-cli cron list
  queue-cli cron create --name <name> --expr <cron-expr> --type <job-type> [--payload <json>] [--priority <n>] [--max-attempts <n>] [--timeout <seconds>]
  queue-cli cron delete <id>

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
    // La CLI habla directo con Postgres y no tiene por qué levantar una
    // conexión a Redis solo para esto -- muestra el registro histórico
    // (Fase 2). Para saber quién está vivo AHORA, `GET /workers` en la API
    // sigue siendo la fuente completa (cruza esto con los heartbeats).
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

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}
