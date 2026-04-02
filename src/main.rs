//! Stagecrew: Disk usage management with automatic cleanup policies.

mod audit;
mod cli;
mod config;
mod daemon;
mod db;
mod error;
mod removal;
mod scanner;
mod tui;

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use color_eyre::eyre::{Context, Result};
use inquire::{Confirm, Text};
use jiff::{Timestamp, Zoned};
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command, ConfigCommand};
use config::{AppConfig, AppPaths, Config};
use db::Database;
use error::Error;
use scanner::{Scanner, refresh};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize color_eyre for better error reports
    color_eyre::install()?;

    // Initialize paths (needed early for log file location)
    let paths = AppPaths::new();

    let cli = Cli::parse();
    let command = cli.command.unwrap_or_default();

    // Initialize tracing to an append-only log file so it never interferes
    // with the TUI's terminal output. The verbosity flag sets a baseline level
    // (-v for info, -vv for debug, -vvv for trace, -q for error-only).
    // RUST_LOG overrides the flag when set, giving power users precise control.
    let log_path = paths.log_file().context("Failed to create log directory")?;
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .context("Failed to open log file")?;

    let env_filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::from_default_env()
    } else {
        let level = cli.verbose.tracing_level().unwrap_or(tracing::Level::WARN);
        EnvFilter::new(level.to_string())
    };

    tracing_subscriber::fmt()
        .with_writer(log_file)
        .with_ansi(false)
        .with_env_filter(env_filter)
        .init();

    // Handle init and config subcommands before loading the full config/database,
    // since they may need to create config or only inspect paths.
    if let Command::Init {
        non_interactive,
        dry_run,
    } = command
    {
        handle_init(&paths, non_interactive, dry_run)?;
        return Ok(());
    }

    if let Command::Config(ref config_cmd) = command {
        return handle_config_command(config_cmd, &paths);
    }

    // For all other commands, load config and open database
    let global_config = Config::load(&paths).context("Failed to load configuration")?;

    // Open database (path derived from config)
    let db_path = paths
        .database_file(&global_config)
        .context("Failed to get database path")?;
    let db = Database::open(&db_path).context("Failed to open database")?;

    // Build AppConfig with per-root overrides
    let mut app_config = AppConfig::from_global(global_config);
    let db_roots: Vec<_> = db.list_roots()?.into_iter().map(|r| r.path).collect();
    app_config.load_per_root(&db_roots);

    match command {
        Command::Tui => {
            let mut app = tui::App::new();
            app.run(&app_config, &db, &db_path, &paths).await?;
        }

        Command::Daemon {
            interval,
            once,
            scan_only,
            dry_run,
        } => {
            let opts = daemon::DaemonOptions {
                interval_hours: interval,
                once: once || dry_run,
                scan_only: scan_only || dry_run,
                dry_run,
            };
            let daemon = daemon::Daemon::new(app_config, opts);
            daemon.run().await?;
        }

        Command::Status => {
            handle_status(&db)?;
        }

        Command::Scan { path } => {
            handle_scan(&app_config, &db, path).await?;
        }

        Command::Add { path, scan } => {
            handle_add(&app_config, &db, path, scan).await?;
        }

        Command::Init { .. } | Command::Config(_) => unreachable!("handled above"),
    }

    Ok(())
}

/// Handle the init subcommand.
///
/// Creates default configuration if none exists, and initializes the database.
/// Prints paths to both config and database files.
fn handle_init(paths: &AppPaths, non_interactive: bool, dry_run: bool) -> Result<()> {
    let config_path = paths.config_file()?;
    let config_exists = config_path.exists();

    if config_exists && !dry_run {
        let should_overwrite = if non_interactive {
            return Err(Error::Config(format!(
                "Refusing to overwrite existing configuration at {} in --non-interactive mode. Use `stagecrew config edit` to modify it, rerun `stagecrew init` interactively and confirm overwrite, or pass `--dry-run` to preview a fresh config.",
                config_path.display()
            ))
            .into());
        } else {
            eprintln!(
                "Warning: a Stagecrew configuration already exists at:\n\n  {}\n\nRunning init will overwrite that file with a newly generated configuration. Existing settings will not be merged.",
                config_path.display()
            );
            Confirm::new("Overwrite the existing config.toml?")
                .with_default(false)
                .prompt()
                .context("Failed to read overwrite confirmation")?
        };

        if !should_overwrite {
            eprintln!("Initialization cancelled. Existing configuration was left unchanged.");
            return Ok(());
        }
    }

    let fresh_config = default_init_config(Timestamp::now());

    let config = if non_interactive {
        fresh_config
    } else {
        prompt_for_init_config(&fresh_config)?
    };

    if dry_run {
        print!("{}", config.to_file_contents()?);
        return Ok(());
    }

    config.save(paths).context("Failed to save configuration")?;
    if config_exists {
        eprintln!("Configuration updated at: {}", config_path.display());
    } else {
        eprintln!("Configuration initialized at: {}", config_path.display());
    }

    // Initialize database (idempotent operation)
    let db_path = paths
        .database_file(&config)
        .context("Failed to get database path")?;

    let db_exists = db_path.exists();
    Database::open(&db_path).context("Failed to open database")?;

    if db_exists {
        eprintln!(
            "Database already exists at: {} (schema is up to date)",
            db_path.display()
        );
    } else {
        eprintln!("Database initialized at: {}", db_path.display());
    }

    Ok(())
}

fn default_init_config(now: Timestamp) -> Config {
    Config {
        scan_start_time: Some(now.to_string()),
        ..Config::default()
    }
}

fn prompt_for_init_config(base: &Config) -> Result<Config> {
    let expiration_days = Text::new("Default expiration period in days:")
        .with_default(&base.expiration_days.to_string())
        .prompt()
        .context("Failed to read expiration_days prompt")?
        .parse::<u32>()
        .context("Expiration period must be a positive integer")?;

    let warning_days = Text::new("Warning window in days:")
        .with_default(&base.warning_days.to_string())
        .prompt()
        .context("Failed to read warning_days prompt")?
        .parse::<u32>()
        .context("Warning window must be a positive integer")?;

    let auto_remove = Confirm::new("Automatically approve expired files for removal?")
        .with_default(base.auto_remove)
        .prompt()
        .context("Failed to read auto_remove prompt")?;

    let scan_interval_hours = Text::new("Scan interval in hours:")
        .with_default(&base.scan_interval_hours.to_string())
        .prompt()
        .context("Failed to read scan_interval_hours prompt")?
        .parse::<u32>()
        .context("Scan interval must be a positive integer")?;

    let default_scan_start = base
        .scan_start_time
        .clone()
        .unwrap_or_else(|| "now".to_string());
    let scan_start_input =
        Text::new("Scan start time (local time like 1:00 AM, RFC3339, or 'now'):")
            .with_default(&default_scan_start)
            .prompt()
            .context("Failed to read scan_start_time prompt")?;
    let now = Zoned::now();
    let scan_start_time = Some(resolve_scan_start_time_input(&scan_start_input, &now)?);

    Ok(Config {
        tracked_paths: base.tracked_paths.clone(),
        expiration_days,
        warning_days,
        auto_remove,
        scan_interval_hours,
        scan_start_time,
        database_path: base.database_path.clone(),
    })
}

fn resolve_scan_start_time_input(input: &str, now: &Zoned) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.eq_ignore_ascii_case("now") {
        return Ok(now.timestamp().to_string());
    }

    if let Ok(timestamp) = Timestamp::from_str(trimmed) {
        return Ok(timestamp.to_string());
    }

    let (hour, minute) = parse_local_time_of_day(trimmed)?;
    let local_occurrence = next_local_occurrence(now, hour, minute)?;
    Ok(local_occurrence.timestamp().to_string())
}

fn parse_local_time_of_day(input: &str) -> Result<(i8, i8)> {
    let normalized = input.trim().to_ascii_lowercase();
    let (time_part, meridiem) = if let Some(stripped) = normalized.strip_suffix("am") {
        (stripped.trim(), Some("am"))
    } else if let Some(stripped) = normalized.strip_suffix("pm") {
        (stripped.trim(), Some("pm"))
    } else {
        (normalized.as_str(), None)
    };

    let (hour_part, minute_part) = time_part
        .split_once(':')
        .ok_or_else(|| Error::Config("Time must be in HH:MM or H:MM AM/PM format".to_string()))?;

    let raw_hour = hour_part
        .trim()
        .parse::<u8>()
        .map_err(|_| Error::Config("Hour must be a number".to_string()))?;
    let minute = minute_part
        .trim()
        .parse::<u8>()
        .map_err(|_| Error::Config("Minute must be a number".to_string()))?;

    if minute > 59 {
        return Err(Error::Config("Minute must be between 0 and 59".to_string()).into());
    }

    let hour = match meridiem {
        Some("am") => {
            if raw_hour == 0 || raw_hour > 12 {
                return Err(Error::Config(
                    "12-hour clock hours must be between 1 and 12".to_string(),
                )
                .into());
            }
            if raw_hour == 12 { 0 } else { raw_hour }
        }
        Some("pm") => {
            if raw_hour == 0 || raw_hour > 12 {
                return Err(Error::Config(
                    "12-hour clock hours must be between 1 and 12".to_string(),
                )
                .into());
            }
            if raw_hour == 12 { 12 } else { raw_hour + 12 }
        }
        None => {
            if raw_hour > 23 {
                return Err(Error::Config(
                    "24-hour clock hours must be between 0 and 23".to_string(),
                )
                .into());
            }
            raw_hour
        }
        Some(_) => unreachable!("only am or pm suffixes are recognized"),
    };

    Ok((
        i8::try_from(hour).map_err(|_| Error::Config("Hour overflow".to_string()))?,
        i8::try_from(minute).map_err(|_| Error::Config("Minute overflow".to_string()))?,
    ))
}

fn next_local_occurrence(now: &Zoned, hour: i8, minute: i8) -> Result<Zoned> {
    let mut next = now
        .with()
        .hour(hour)
        .minute(minute)
        .second(0)
        .subsec_nanosecond(0)
        .build()
        .map_err(|e| Error::Config(format!("Invalid local scan start time: {e}")))?;

    if next.timestamp() <= now.timestamp() {
        next = next
            .tomorrow()
            .map_err(|e| Error::Config(format!("Failed to compute next local occurrence: {e}")))?;
    }

    Ok(next)
}

/// Handle config subcommands for inspecting and managing configuration.
fn handle_config_command(cmd: &ConfigCommand, paths: &AppPaths) -> Result<()> {
    match cmd {
        ConfigCommand::Show => {
            let config = Config::load(paths).context("Failed to load configuration")?;
            let toml_str =
                toml::to_string_pretty(&config).context("Failed to serialize configuration")?;
            println!("{toml_str}");
        }
        ConfigCommand::Path => {
            let config_path = paths.config_file()?;
            println!("{}", config_path.display());
        }
        ConfigCommand::DbPath => {
            let config = Config::load(paths).context("Failed to load configuration")?;
            let db_path = paths.database_file(&config)?;
            println!("{}", db_path.display());
        }
        ConfigCommand::LogPath => {
            let log_path = paths.log_file()?;
            println!("{}", log_path.display());
        }
        ConfigCommand::Edit => {
            let config_path = paths.config_file()?;
            let editor = std::env::var("VISUAL")
                .or_else(|_| std::env::var("EDITOR"))
                .unwrap_or_else(|_| "vi".to_string());

            let status = std::process::Command::new(&editor)
                .arg(&config_path)
                .status()
                .context(format!("Failed to launch editor: {editor}"))?;

            if !status.success() {
                eprintln!("Editor exited with non-zero status");
            }
        }
        ConfigCommand::Schema => {
            let schema = schemars::schema_for!(Config);
            let json =
                serde_json::to_string_pretty(&schema).context("Failed to serialize JSON schema")?;
            println!("{json}");
        }
    }
    Ok(())
}

/// Handle the add subcommand.
///
/// Validates that the path exists and is a directory, adds it as a root in the
/// database, and optionally runs an initial scan.
async fn handle_add(
    app_config: &AppConfig,
    db: &Database,
    path: PathBuf,
    run_scan: bool,
) -> Result<()> {
    debug_assert!(!path.as_os_str().is_empty(), "path should not be empty");

    // Canonicalize path to resolve symlinks and normalize, preventing duplicate entries
    // with different representations (e.g., /data/staging vs /data/./staging)
    let path = path
        .canonicalize()
        .map_err(|e| {
            if path.exists() {
                Error::Io(e)
            } else {
                Error::PathNotFound(path.clone())
            }
        })
        .context("Failed to canonicalize path")?;

    // Validate that path is a directory
    if !path.is_dir() {
        return Err(Error::NotADirectory(path.clone()).into());
    }

    // Check if already tracked in the database
    let existing_roots = db.list_roots().context("Failed to list roots")?;
    if existing_roots.iter().any(|r| r.path == path) {
        println!("Path is already tracked: {}", path.display());
        return Ok(());
    }

    // Insert as a root in the database
    db.insert_root(&path)
        .context("Failed to add root to database")?;

    println!("Added tracked path: {}", path.display());

    // Optionally run initial refresh (scan + transition)
    if run_scan {
        println!("Refreshing...");
        let scanner = Scanner::new();
        let summary = refresh(db, &scanner, app_config)
            .await
            .context("Failed to refresh tracked paths")?;

        println!(
            "{}",
            format_refresh_summary(
                summary.scan.total_directories,
                summary.scan.total_files,
                summary.scan.total_size_bytes,
                summary.scan.unique_files,
                summary.scan.unique_size_bytes,
            )
        );
        if summary.transitions.expired_to_pending > 0 {
            println!(
                "  {} files expired \u{2192} pending approval",
                summary.transitions.expired_to_pending
            );
        }
        if summary.transitions.expired_to_approved > 0 {
            println!(
                "  {} files expired \u{2192} approved for removal",
                summary.transitions.expired_to_approved
            );
        }
    }

    Ok(())
}

/// Handle the scan subcommand.
///
/// Refreshes all tracked roots by scanning the filesystem and transitioning
/// expired files. Config `tracked_paths` are seeded as roots in the database,
/// then all DB roots (config baseline + user-added) are refreshed.
/// If `--path` is provided, that path is added as a root before refreshing.
async fn handle_scan(app_config: &AppConfig, db: &Database, path: Option<PathBuf>) -> Result<()> {
    let scanner = Scanner::new();

    // If a specific path was provided, ensure it exists as a root in the DB
    if let Some(ref specific_path) = path {
        db.insert_root(specific_path)
            .context("Failed to add path as root")?;
        println!("Scanning {}...", specific_path.display());
    }

    // Check that we'll have something to scan (config paths + DB roots)
    let db_roots = db.list_roots().context("Failed to list roots")?;
    let tracked_paths = &app_config.global.tracked_paths;
    if tracked_paths.is_empty() && db_roots.is_empty() && path.is_none() {
        return Err(color_eyre::eyre::eyre!(
            "No tracked paths configured. Add paths with `stagecrew add` or set tracked_paths in config.toml."
        ));
    }

    if path.is_none() {
        let total_roots = {
            // Count unique roots: config paths that aren't already in DB + DB roots
            let db_paths: std::collections::HashSet<&std::path::Path> =
                db_roots.iter().map(|r| r.path.as_path()).collect();
            let new_from_config = tracked_paths
                .iter()
                .filter(|p| !db_paths.contains(p.as_path()))
                .count();
            db_roots.len() + new_from_config
        };
        if total_roots == 1 {
            let display_path = db_roots.first().map_or_else(
                || tracked_paths[0].display().to_string(),
                |r| r.path.display().to_string(),
            );
            println!("Scanning {display_path}...");
        } else {
            println!("Scanning {total_roots} paths...");
        }
    }

    // Refresh: scan filesystem then transition expired files using per-root configs
    let summary = refresh(db, &scanner, app_config)
        .await
        .context("Failed to refresh tracked paths")?;

    // Print summary
    println!(
        "{}",
        format_refresh_summary(
            summary.scan.total_directories,
            summary.scan.total_files,
            summary.scan.total_size_bytes,
            summary.scan.unique_files,
            summary.scan.unique_size_bytes,
        )
    );
    if summary.transitions.expired_to_pending > 0 {
        println!(
            "  {} files expired \u{2192} pending approval",
            summary.transitions.expired_to_pending
        );
    }
    if summary.transitions.expired_to_approved > 0 {
        println!(
            "  {} files expired \u{2192} approved for removal",
            summary.transitions.expired_to_approved
        );
    }
    if summary.transitions.deferred_reset > 0 {
        println!(
            "  {} deferred files reset to tracked",
            summary.transitions.deferred_reset
        );
    }

    Ok(())
}

/// Handle the status subcommand.
///
/// Queries the stats table and prints a fast, human-readable summary for use in shell hooks.
/// Output format varies based on urgency:
/// - If files are overdue or pending: shows warning with counts
/// - If nothing urgent: shows "All clear" message
///
/// Note: Output format is unstable and may change between versions.
fn handle_status(db: &Database) -> Result<()> {
    let stats = db.get_stats().context("Failed to query stats")?;

    // Verify invariants
    debug_assert!(stats.total_files >= 0, "total_files cannot be negative");
    debug_assert!(
        stats.total_size_bytes >= 0,
        "total_size_bytes cannot be negative"
    );
    debug_assert!(stats.files_overdue >= 0, "files_overdue cannot be negative");
    debug_assert!(
        stats.files_pending_approval >= 0,
        "files_pending_approval cannot be negative"
    );
    debug_assert!(
        stats.files_within_warning >= 0,
        "files_within_warning cannot be negative"
    );

    println!("{}", format_status_output(&stats));
    Ok(())
}

fn format_refresh_summary(
    total_directories: u64,
    tracked_files: u64,
    tracked_size_bytes: u64,
    unique_files: u64,
    unique_size_bytes: u64,
) -> String {
    if tracked_files == unique_files && tracked_size_bytes == unique_size_bytes {
        format!(
            "Refresh complete: {total_directories} directories, {tracked_files} files, {}",
            format_bytes(tracked_size_bytes)
        )
    } else {
        format!(
            "Refresh complete: {total_directories} directories, {tracked_files} tracked files ({} across roots, {} unique across disk)",
            format_bytes(tracked_size_bytes),
            format_bytes(unique_size_bytes)
        )
    }
}

/// Format status output based on urgency metrics.
///
/// Returns a human-readable status line for shell hook display.
/// The output follows a priority hierarchy: overdue > pending > warning > all clear.
fn format_status_output(stats: &db::Stats) -> String {
    let files_overdue = stats.files_overdue;
    let files_pending = stats.files_pending_approval;
    let files_within_warning = stats.files_within_warning;

    if files_overdue > 0 {
        // Most urgent: files are overdue
        if files_pending > 0 {
            format!("stagecrew: {files_overdue} files overdue, {files_pending} pending approval")
        } else {
            format!("stagecrew: {files_overdue} files overdue")
        }
    } else if files_pending > 0 {
        // Urgent: files need approval
        if files_within_warning > 0 {
            format!(
                "stagecrew: {files_pending} files pending approval, {files_within_warning} expiring soon"
            )
        } else {
            format!("stagecrew: {files_pending} files pending approval")
        }
    } else if files_within_warning > 0 {
        // Warning: files approaching expiration
        format!("stagecrew: {files_within_warning} files expiring soon")
    } else {
        // All clear
        let formatted_size = format_bytes(
            // Allow: Converting i64 to u64 for format_bytes. Stats table
            // constraints ensure total_size_bytes is non-negative.
            #[allow(clippy::cast_sign_loss)]
            {
                stats.total_size_bytes as u64
            },
        );
        format!(
            "stagecrew: All clear. {} files tracked, {formatted_size}",
            stats.total_files
        )
    }
}

/// Format byte count as human-readable string.
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];

    if bytes == 0 {
        return "0 B".to_string();
    }

    // Allow: Converting bytes to f64 for log calculation is standard practice for
    // human-readable size formatting. Precision loss is acceptable for display purposes.
    #[allow(clippy::cast_precision_loss)]
    let bytes_f64 = bytes as f64;

    // Allow: Exponent calculation is guaranteed to produce non-negative values (log10 of positive
    // numbers divided by 3). Sign loss and truncation are safe in this context. The exponent is
    // clamped to UNITS.len() - 1 so truncation to usize is safe.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let exponent = (bytes_f64.log10() / 3.0).floor() as usize;
    let exponent = exponent.min(UNITS.len() - 1);

    // Allow: Exponent is bounded by UNITS.len() (6), so casting to i32 is safe and won't wrap.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let value = bytes_f64 / 1000_f64.powi(exponent as i32);
    let unit = UNITS[exponent];

    if exponent == 0 {
        format!("{bytes} {unit}")
    } else {
        format!("{value:.1} {unit}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::tz::TimeZone;
    use tempfile::tempdir;

    #[test]
    fn format_bytes_zero() {
        assert_eq!(format_bytes(0), "0 B");
    }

    #[test]
    fn format_bytes_sub_kilobyte() {
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(999), "999 B");
    }

    #[test]
    fn format_bytes_kilobyte_boundary() {
        assert_eq!(format_bytes(1000), "1.0 KB");
        assert_eq!(format_bytes(1500), "1.5 KB");
    }

    #[test]
    fn format_bytes_megabyte_range() {
        assert_eq!(format_bytes(1_000_000), "1.0 MB");
        assert_eq!(format_bytes(1_500_000), "1.5 MB");
    }

    #[test]
    fn format_bytes_gigabyte_range() {
        assert_eq!(format_bytes(1_000_000_000), "1.0 GB");
        assert_eq!(format_bytes(1_234_567_890), "1.2 GB");
    }

    #[test]
    fn format_bytes_terabyte_range() {
        assert_eq!(format_bytes(1_000_000_000_000), "1.0 TB");
    }

    #[test]
    fn format_bytes_petabyte_range() {
        assert_eq!(format_bytes(1_000_000_000_000_000), "1.0 PB");
    }

    #[test]
    fn format_bytes_large_value_does_not_panic() {
        // Verify we handle large values gracefully (u64::MAX ≈ 18.4 EB)
        let result = format_bytes(u64::MAX);
        assert!(!result.is_empty());
        // Should show in PB since that's our largest unit
        assert!(result.contains("PB"));
    }

    #[test]
    fn format_refresh_summary_uses_simple_output_without_overlap() {
        assert_eq!(
            format_refresh_summary(12, 34, 1_000_000, 34, 1_000_000),
            "Refresh complete: 12 directories, 34 files, 1.0 MB"
        );
    }

    #[test]
    fn format_refresh_summary_explains_overlap_when_totals_differ() {
        assert_eq!(
            format_refresh_summary(12, 50, 90_000_000_000_000, 30, 60_000_000_000_000),
            "Refresh complete: 12 directories, 50 tracked files (90.0 TB across roots, 60.0 TB unique across disk)"
        );
    }

    #[test]
    fn default_init_config_anchors_scan_start_time_to_now() {
        let now = "2026-04-03T08:00:00Z"
            .parse::<Timestamp>()
            .expect("parse now timestamp");

        let config = default_init_config(now);

        assert_eq!(
            config.scan_start_time.as_deref(),
            Some("2026-04-03T08:00:00Z")
        );
    }

    #[test]
    fn parse_local_time_of_day_handles_12_hour_input() {
        assert_eq!(
            parse_local_time_of_day("1:15 AM").expect("parse 1:15 AM"),
            (1, 15)
        );
        assert_eq!(
            parse_local_time_of_day("12:30 PM").expect("parse 12:30 PM"),
            (12, 30)
        );
        assert_eq!(
            parse_local_time_of_day("12:00 AM").expect("parse 12:00 AM"),
            (0, 0)
        );
    }

    #[test]
    fn parse_local_time_of_day_handles_24_hour_input() {
        assert_eq!(
            parse_local_time_of_day("13:05").expect("parse 13:05"),
            (13, 5)
        );
        assert_eq!(
            parse_local_time_of_day("00:45").expect("parse 00:45"),
            (0, 45)
        );
    }

    #[test]
    fn parse_local_time_of_day_rejects_invalid_values() {
        assert!(parse_local_time_of_day("25:00").is_err());
        assert!(parse_local_time_of_day("12:99").is_err());
        assert!(parse_local_time_of_day("0:30 PM").is_err());
    }

    #[test]
    fn resolve_scan_start_time_input_accepts_now() {
        let now = "2026-04-03T08:00:00Z"
            .parse::<Timestamp>()
            .expect("parse now timestamp")
            .to_zoned(TimeZone::UTC);

        let resolved = resolve_scan_start_time_input("now", &now).expect("resolve now");

        assert_eq!(resolved, "2026-04-03T08:00:00Z");
    }

    #[test]
    fn resolve_scan_start_time_input_preserves_rfc3339_input() {
        let now = "2026-04-03T08:00:00Z"
            .parse::<Timestamp>()
            .expect("parse now timestamp")
            .to_zoned(TimeZone::UTC);

        let resolved = resolve_scan_start_time_input("2026-04-04T03:15:00Z", &now)
            .expect("resolve RFC3339 timestamp");

        assert_eq!(resolved, "2026-04-04T03:15:00Z");
    }

    #[test]
    fn next_local_occurrence_moves_to_tomorrow_after_past_time() {
        let now = "2026-04-03T23:30:00Z"
            .parse::<Timestamp>()
            .expect("parse now timestamp")
            .to_zoned(TimeZone::UTC);

        let next = next_local_occurrence(&now, 1, 0).expect("compute next occurrence");

        assert_eq!(next.timestamp().to_string(), "2026-04-04T01:00:00Z");
    }

    #[test]
    fn next_local_occurrence_keeps_later_same_day_time() {
        let now = "2026-04-03T08:30:00Z"
            .parse::<Timestamp>()
            .expect("parse now timestamp")
            .to_zoned(TimeZone::UTC);

        let next = next_local_occurrence(&now, 13, 0).expect("compute next occurrence");

        assert_eq!(next.timestamp().to_string(), "2026-04-03T13:00:00Z");
    }

    #[test]
    fn init_non_interactive_refuses_to_overwrite_existing_config() {
        let temp_dir = tempdir().expect("create temp dir");
        let config_path = temp_dir.path().join("config.toml");
        let db_path = temp_dir.path().join("stagecrew.db");
        let paths = AppPaths::with_overrides(Some(config_path.clone()), Some(db_path));

        Config::default()
            .save(&paths)
            .expect("write initial config");

        let err = handle_init(&paths, true, false).expect_err("should refuse overwrite");
        let msg = err.to_string();
        assert!(msg.contains("--non-interactive mode"));
    }

    #[test]
    fn init_dry_run_does_not_overwrite_existing_config() {
        let temp_dir = tempdir().expect("create temp dir");
        let config_path = temp_dir.path().join("config.toml");
        let db_path = temp_dir.path().join("stagecrew.db");
        let paths = AppPaths::with_overrides(Some(config_path.clone()), Some(db_path.clone()));

        Config::default()
            .save(&paths)
            .expect("write initial config");
        let before = std::fs::read_to_string(&config_path).expect("read initial config");

        handle_init(&paths, true, true).expect("dry-run should succeed without overwrite");

        let after = std::fs::read_to_string(&config_path).expect("read config after dry-run");
        assert_eq!(before, after, "dry-run should not modify existing config");
        assert!(
            !db_path.exists(),
            "dry-run should not initialize the database"
        );
    }

    // === Status Output Formatting Tests ===

    #[test]
    fn format_status_output_overdue_with_pending() {
        let stats = db::Stats {
            total_files: 10,
            total_size_bytes: 1_000_000,
            files_within_warning: 1,
            files_pending_approval: 2,
            files_overdue: 3,
            last_scan_completed: None,
            files_healthy: 0,
            bytes_healthy: 0,
            bytes_within_warning: 0,
            bytes_pending_approval: 0,
            bytes_overdue: 0,
            files_ignored: 0,
            bytes_ignored: 0,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: 3 files overdue, 2 pending approval"
        );
    }

    #[test]
    fn format_status_output_overdue_only() {
        let stats = db::Stats {
            total_files: 10,
            total_size_bytes: 1_000_000,
            files_within_warning: 1,
            files_pending_approval: 0,
            files_overdue: 3,
            last_scan_completed: None,
            files_healthy: 0,
            bytes_healthy: 0,
            bytes_within_warning: 0,
            bytes_pending_approval: 0,
            bytes_overdue: 0,
            files_ignored: 0,
            bytes_ignored: 0,
        };
        assert_eq!(format_status_output(&stats), "stagecrew: 3 files overdue");
    }

    #[test]
    fn format_status_output_pending_with_warning() {
        let stats = db::Stats {
            total_files: 10,
            total_size_bytes: 1_000_000,
            files_within_warning: 4,
            files_pending_approval: 2,
            files_overdue: 0,
            last_scan_completed: None,
            files_healthy: 0,
            bytes_healthy: 0,
            bytes_within_warning: 0,
            bytes_pending_approval: 0,
            bytes_overdue: 0,
            files_ignored: 0,
            bytes_ignored: 0,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: 2 files pending approval, 4 expiring soon"
        );
    }

    #[test]
    fn format_status_output_pending_only() {
        let stats = db::Stats {
            total_files: 10,
            total_size_bytes: 1_000_000,
            files_within_warning: 0,
            files_pending_approval: 2,
            files_overdue: 0,
            last_scan_completed: None,
            files_healthy: 0,
            bytes_healthy: 0,
            bytes_within_warning: 0,
            bytes_pending_approval: 0,
            bytes_overdue: 0,
            files_ignored: 0,
            bytes_ignored: 0,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: 2 files pending approval"
        );
    }

    #[test]
    fn format_status_output_warning_only() {
        let stats = db::Stats {
            total_files: 10,
            total_size_bytes: 1_000_000,
            files_within_warning: 5,
            files_pending_approval: 0,
            files_overdue: 0,
            last_scan_completed: None,
            files_healthy: 0,
            bytes_healthy: 0,
            bytes_within_warning: 0,
            bytes_pending_approval: 0,
            bytes_overdue: 0,
            files_ignored: 0,
            bytes_ignored: 0,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: 5 files expiring soon"
        );
    }

    #[test]
    fn format_status_output_all_clear() {
        let stats = db::Stats {
            total_files: 10,
            total_size_bytes: 1_234_567_890,
            files_within_warning: 0,
            files_pending_approval: 0,
            files_overdue: 0,
            last_scan_completed: None,
            files_healthy: 0,
            bytes_healthy: 0,
            bytes_within_warning: 0,
            bytes_pending_approval: 0,
            bytes_overdue: 0,
            files_ignored: 0,
            bytes_ignored: 0,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: All clear. 10 files tracked, 1.2 GB"
        );
    }

    #[test]
    fn format_status_output_all_clear_empty_database() {
        let stats = db::Stats {
            total_files: 0,
            total_size_bytes: 0,
            files_within_warning: 0,
            files_pending_approval: 0,
            files_overdue: 0,
            last_scan_completed: None,
            files_healthy: 0,
            bytes_healthy: 0,
            bytes_within_warning: 0,
            bytes_pending_approval: 0,
            bytes_overdue: 0,
            files_ignored: 0,
            bytes_ignored: 0,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: All clear. 0 files tracked, 0 B"
        );
    }
}
