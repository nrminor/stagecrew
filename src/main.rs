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

use std::path::PathBuf;

use clap::Parser;
use color_eyre::eyre::{Context, Result};
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command};
use config::{AppPaths, Config};
use db::Database;
use scanner::{Scanner, scan_and_persist};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize color_eyre for better error reports
    color_eyre::install()?;

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let command = cli.command.unwrap_or_default();

    // Initialize paths
    let paths = AppPaths::new();

    // Handle init and add commands separately since they may need to create/modify config
    if matches!(command, Command::Init) {
        handle_init(&paths)?;
        return Ok(());
    }

    if let Command::Add { path, scan } = command {
        handle_add(&paths, path, scan).await?;
        return Ok(());
    }

    // For all other commands, load config and open database
    let config = Config::load(&paths).context("Failed to load configuration")?;

    // Open database (path derived from config)
    let db_path = paths
        .database_file(&config)
        .context("Failed to get database path")?;
    let db = Database::open(&db_path).context("Failed to open database")?;

    match command {
        Command::Tui => {
            let mut app = tui::App::new();
            app.run(&config, &db).await?;
        }

        Command::Daemon => {
            let daemon = daemon::Daemon::new(config);
            daemon.run().await?;
        }

        Command::Status => {
            handle_status(&db)?;
        }

        Command::Scan { path } => {
            handle_scan(&config, &db, path).await?;
        }

        Command::Init => unreachable!("Init handled above"),
        Command::Add { .. } => unreachable!("Add handled above"),
    }

    Ok(())
}

/// Handle the init subcommand.
///
/// Creates default configuration if none exists, and initializes the database.
/// Prints paths to both config and database files.
fn handle_init(paths: &AppPaths) -> Result<()> {
    let config_path = paths.config_file()?;

    // Check if config already exists
    let config = if config_path.exists() {
        println!(
            "Configuration file already exists at: {}",
            config_path.display()
        );
        Config::load(paths).context("Failed to load existing configuration")?
    } else {
        let new_config = Config::default();
        new_config
            .save(paths)
            .context("Failed to save configuration")?;
        println!("Configuration initialized at: {}", config_path.display());
        new_config
    };

    // Initialize database (idempotent operation)
    let db_path = paths
        .database_file(&config)
        .context("Failed to get database path")?;

    let db_exists = db_path.exists();
    Database::open(&db_path).context("Failed to open database")?;

    if db_exists {
        println!(
            "Database already exists at: {} (schema is up to date)",
            db_path.display()
        );
    } else {
        println!("Database initialized at: {}", db_path.display());
    }

    Ok(())
}

/// Handle the add subcommand.
///
/// Validates that the path exists and is a directory, adds it to the config's `tracked_paths`,
/// saves the updated config, and optionally runs an initial scan if --scan flag is provided.
async fn handle_add(paths: &AppPaths, path: PathBuf, run_scan: bool) -> Result<()> {
    use crate::error::Error;

    debug_assert!(!path.as_os_str().is_empty(), "path should not be empty");

    // Canonicalize path to resolve symlinks and normalize, preventing duplicate entries
    // with different representations (e.g., /data/staging vs /data/./staging)
    let path = path
        .canonicalize()
        .map_err(|e| {
            // Check if the path exists vs doesn't exist to provide better error messages
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

    // Load existing config or create default if missing
    let config_path = paths.config_file()?;
    let mut config = if config_path.exists() {
        Config::load(paths).context("Failed to load existing configuration")?
    } else {
        Config::default()
    };

    debug_assert!(
        config.expiration_days > 0,
        "expiration_days must be positive"
    );

    // Check if path is already tracked (duplicate detection with canonicalized paths)
    if config.tracked_paths.contains(&path) {
        println!("Path is already tracked: {}", path.display());
        return Ok(());
    }

    // Add path to tracked_paths
    config.tracked_paths.push(path.clone());

    // Save updated config
    config.save(paths).context("Failed to save configuration")?;

    println!("Added tracked path: {}", path.display());

    // Optionally run initial scan
    if run_scan {
        // Open database (path derived from config)
        let db_path = paths
            .database_file(&config)
            .context("Failed to get database path")?;
        let db = Database::open(&db_path).context("Failed to open database")?;

        println!("Running initial scan...");
        let scanner = Scanner::new();
        // Use from_ref to create a single-element slice without allocation
        let summary = scan_and_persist(
            &db,
            &scanner,
            std::slice::from_ref(&path),
            config.expiration_days,
            config.warning_days,
        )
        .await
        .context("Failed to scan and persist path")?;

        println!(
            "Scan complete: {} directories, {} files, {}",
            summary.total_directories,
            summary.total_files,
            format_bytes(summary.total_size_bytes)
        );
    }

    Ok(())
}

/// Handle the scan subcommand.
///
/// Scans either all configured tracked paths or a specific path if provided via --path.
/// Prints progress messages and a summary of scan results.
async fn handle_scan(config: &Config, db: &Database, path: Option<PathBuf>) -> Result<()> {
    let scanner = Scanner::new();

    // Determine which paths to scan
    let paths_to_scan = if let Some(specific_path) = path {
        // Scan only the specified path
        vec![specific_path]
    } else if config.tracked_paths.is_empty() {
        // No tracked paths configured and no --path given
        return Err(color_eyre::eyre::eyre!(
            "No tracked paths configured. Either add paths to config.toml or use --path to specify a path to scan."
        ));
    } else {
        // Scan all configured tracked paths
        config.tracked_paths.clone()
    };

    // Print progress message
    if paths_to_scan.len() == 1 {
        println!("Scanning {}...", paths_to_scan[0].display());
    } else {
        println!("Scanning {} paths...", paths_to_scan.len());
    }

    // Run the scan
    let summary = scan_and_persist(
        db,
        &scanner,
        &paths_to_scan,
        config.expiration_days,
        config.warning_days,
    )
    .await
    .context("Failed to scan and persist paths")?;

    // Print summary
    println!(
        "Scan complete: {} directories, {} files, {}",
        summary.total_directories,
        summary.total_files,
        format_bytes(summary.total_size_bytes)
    );

    Ok(())
}

/// Handle the status subcommand.
///
/// Queries the stats table and prints a fast, human-readable summary for use in shell hooks.
/// Output format varies based on urgency:
/// - If paths are overdue or pending: shows warning with counts
/// - If nothing urgent: shows "All clear" message
///
/// Note: Output format is unstable and may change between versions.
fn handle_status(db: &Database) -> Result<()> {
    let stats = db.get_stats().context("Failed to query stats")?;

    // Verify invariants
    debug_assert!(
        stats.total_tracked_paths >= 0,
        "total_tracked_paths cannot be negative"
    );
    debug_assert!(
        stats.total_size_bytes >= 0,
        "total_size_bytes cannot be negative"
    );
    debug_assert!(stats.paths_overdue >= 0, "paths_overdue cannot be negative");
    debug_assert!(
        stats.paths_pending_approval >= 0,
        "paths_pending_approval cannot be negative"
    );
    debug_assert!(
        stats.paths_within_warning >= 0,
        "paths_within_warning cannot be negative"
    );

    println!("{}", format_status_output(&stats));
    Ok(())
}

/// Format status output based on urgency metrics.
///
/// Returns a human-readable status line for shell hook display.
/// The output follows a priority hierarchy: overdue > pending > warning > all clear.
fn format_status_output(stats: &db::Stats) -> String {
    let paths_overdue = stats.paths_overdue;
    let paths_pending = stats.paths_pending_approval;
    let paths_within_warning = stats.paths_within_warning;

    if paths_overdue > 0 {
        // Most urgent: paths are overdue
        if paths_pending > 0 {
            format!("stagecrew: {paths_overdue} paths overdue, {paths_pending} pending approval")
        } else {
            format!("stagecrew: {paths_overdue} paths overdue")
        }
    } else if paths_pending > 0 {
        // Urgent: paths need approval
        if paths_within_warning > 0 {
            format!(
                "stagecrew: {paths_pending} paths pending approval, {paths_within_warning} within warning period"
            )
        } else {
            format!("stagecrew: {paths_pending} paths pending approval")
        }
    } else if paths_within_warning > 0 {
        // Warning: paths approaching expiration
        format!("stagecrew: {paths_within_warning} paths within warning period")
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
            "stagecrew: All clear. {} paths tracked, {formatted_size}",
            stats.total_tracked_paths
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

    // === Status Output Formatting Tests ===

    #[test]
    fn format_status_output_overdue_with_pending() {
        let stats = db::Stats {
            total_tracked_paths: 10,
            total_size_bytes: 1_000_000,
            paths_within_warning: 1,
            paths_pending_approval: 2,
            paths_overdue: 3,
            last_scan_completed: None,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: 3 paths overdue, 2 pending approval"
        );
    }

    #[test]
    fn format_status_output_overdue_only() {
        let stats = db::Stats {
            total_tracked_paths: 10,
            total_size_bytes: 1_000_000,
            paths_within_warning: 1,
            paths_pending_approval: 0,
            paths_overdue: 3,
            last_scan_completed: None,
        };
        assert_eq!(format_status_output(&stats), "stagecrew: 3 paths overdue");
    }

    #[test]
    fn format_status_output_pending_with_warning() {
        let stats = db::Stats {
            total_tracked_paths: 10,
            total_size_bytes: 1_000_000,
            paths_within_warning: 4,
            paths_pending_approval: 2,
            paths_overdue: 0,
            last_scan_completed: None,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: 2 paths pending approval, 4 within warning period"
        );
    }

    #[test]
    fn format_status_output_pending_only() {
        let stats = db::Stats {
            total_tracked_paths: 10,
            total_size_bytes: 1_000_000,
            paths_within_warning: 0,
            paths_pending_approval: 2,
            paths_overdue: 0,
            last_scan_completed: None,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: 2 paths pending approval"
        );
    }

    #[test]
    fn format_status_output_warning_only() {
        let stats = db::Stats {
            total_tracked_paths: 10,
            total_size_bytes: 1_000_000,
            paths_within_warning: 5,
            paths_pending_approval: 0,
            paths_overdue: 0,
            last_scan_completed: None,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: 5 paths within warning period"
        );
    }

    #[test]
    fn format_status_output_all_clear() {
        let stats = db::Stats {
            total_tracked_paths: 10,
            total_size_bytes: 1_234_567_890,
            paths_within_warning: 0,
            paths_pending_approval: 0,
            paths_overdue: 0,
            last_scan_completed: None,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: All clear. 10 paths tracked, 1.2 GB"
        );
    }

    #[test]
    fn format_status_output_all_clear_empty_database() {
        let stats = db::Stats {
            total_tracked_paths: 0,
            total_size_bytes: 0,
            paths_within_warning: 0,
            paths_pending_approval: 0,
            paths_overdue: 0,
            last_scan_completed: None,
        };
        assert_eq!(
            format_status_output(&stats),
            "stagecrew: All clear. 0 paths tracked, 0 B"
        );
    }
}
