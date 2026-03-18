//! Background daemon for scanning and scheduled removals.

use std::time::Duration;

use tokio::time::sleep;

use crate::config::{AppConfig, AppPaths};
use crate::db::Database;
use crate::error::Result;
use crate::removal::remove_approved;
use crate::scanner::{Scanner, refresh};

#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

#[cfg(not(unix))]
use tokio::signal;

/// Runtime options parsed from CLI flags.
pub struct DaemonOptions {
    /// Override scan interval in hours (None = use config value).
    pub interval_hours: Option<u32>,
    /// Run one cycle and exit.
    pub once: bool,
    /// Skip the removal step (scan and transition only).
    pub scan_only: bool,
    /// Report what would happen without modifying files or DB state.
    pub dry_run: bool,
}

/// Background daemon that handles periodic scanning and removal execution.
pub struct Daemon {
    app_config: AppConfig,
    paths: AppPaths,
    opts: DaemonOptions,
}

impl Daemon {
    /// Create a new daemon with the given configuration and runtime options.
    pub fn new(app_config: AppConfig, opts: DaemonOptions) -> Self {
        Self {
            app_config,
            paths: AppPaths::new(),
            opts,
        }
    }

    /// Run the daemon's main loop.
    ///
    /// Periodically scans tracked paths, transitions expired entries, and
    /// executes approved removals. Runs continuously until interrupted by
    /// SIGINT or SIGTERM, unless `--once` was specified.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database cannot be opened
    /// - PID lock file cannot be acquired
    /// - Signal handlers cannot be registered (Unix only)
    // Allow: startup banner, lock acquisition, signal setup, and the main loop
    // are all part of one coherent lifecycle that reads better together.
    #[allow(clippy::too_many_lines)]
    pub async fn run(&self) -> Result<()> {
        let config = &self.app_config.global;

        // Open database (path derived from config)
        let db_path = self.paths.database_file(config)?;

        // Acquire PID lock to prevent multiple daemon instances.
        let lock_path = db_path.with_extension("daemon.lock");
        let _lock = acquire_pid_lock(&lock_path)?;

        let db = Database::open(&db_path)?;
        let scanner = Scanner::new();

        let interval_hours = self
            .opts
            .interval_hours
            .unwrap_or(config.scan_interval_hours);

        // Startup banner
        self.print_startup_banner(&db_path, interval_hours);

        if self.opts.dry_run {
            tracing::info!("Dry-run mode: no files will be modified");
            self.run_dry_run_cycle(&db, &scanner).await;
            return Ok(());
        }

        if self.opts.once {
            self.run_single_cycle(&db, &scanner).await;
            return Ok(());
        }

        // Set up graceful shutdown for both SIGINT and SIGTERM
        #[cfg(unix)]
        let mut sigint = signal(SignalKind::interrupt())?;
        #[cfg(unix)]
        let mut sigterm = signal(SignalKind::terminate())?;
        #[cfg(not(unix))]
        let mut shutdown = Box::pin(signal::ctrl_c());

        loop {
            // Reload configs to pick up any changes to local stagecrew.toml files
            let db_roots: Vec<_> = db.list_roots()?.into_iter().map(|r| r.path).collect();
            tracing::debug!(
                root_count = db_roots.len(),
                "Reloading per-root configuration"
            );
            let app_config = match AppConfig::load(&self.paths, &db_roots) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to reload config, continuing with previous");
                    self.app_config.clone()
                }
            };

            // Check for shutdown signal
            #[cfg(unix)]
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT, exiting gracefully");
                    break;
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM, exiting gracefully");
                    break;
                }
                () = Self::run_cycle_inner(&app_config, &db, &scanner, self.opts.scan_only) => {}
            }

            #[cfg(not(unix))]
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("Received shutdown signal, exiting gracefully");
                    break;
                }
                () = Self::run_cycle_inner(&app_config, &db, &scanner, self.opts.scan_only) => {}
            }

            // Sleep for configured interval
            let sleep_duration = Duration::from_secs(u64::from(interval_hours) * 3600);

            tracing::info!(
                ?sleep_duration,
                "Scan cycle complete, sleeping until next iteration"
            );

            #[cfg(unix)]
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT during sleep, exiting gracefully");
                    break;
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM during sleep, exiting gracefully");
                    break;
                }
                () = sleep(sleep_duration) => {}
            }

            #[cfg(not(unix))]
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("Received shutdown signal during sleep, exiting gracefully");
                    break;
                }
                () = sleep(sleep_duration) => {}
            }
        }

        Ok(())
    }

    fn print_startup_banner(&self, db_path: &std::path::Path, interval_hours: u32) {
        let config = &self.app_config.global;
        let mode = if self.opts.dry_run {
            "dry-run"
        } else if self.opts.once {
            "single cycle"
        } else if self.opts.scan_only {
            "scan-only (no removals)"
        } else {
            "continuous"
        };

        eprintln!("stagecrew daemon starting");
        eprintln!("  mode:            {mode}");
        eprintln!("  database:        {}", db_path.display());
        eprintln!("  scan interval:   {interval_hours}h");
        eprintln!("  expiration:      {} days", config.expiration_days);
        eprintln!("  warning window:  {} days", config.warning_days);
        eprintln!("  auto-remove:     {}", config.auto_remove);
        eprintln!("  tracked paths:   {}", config.tracked_paths.len());
        for path in &config.tracked_paths {
            eprintln!("    - {}", path.display());
        }
        eprintln!();

        tracing::info!(
            mode,
            scan_interval_hours = interval_hours,
            expiration_days = config.expiration_days,
            warning_days = config.warning_days,
            auto_remove = config.auto_remove,
            tracked_path_count = config.tracked_paths.len(),
            db_path = %db_path.display(),
            "Daemon started"
        );
    }

    async fn run_single_cycle(&self, db: &Database, scanner: &Scanner) {
        let db_roots: Vec<_> = db
            .list_roots()
            .unwrap_or_default()
            .into_iter()
            .map(|r| r.path)
            .collect();
        let app_config =
            AppConfig::load(&self.paths, &db_roots).unwrap_or_else(|_| self.app_config.clone());
        Self::run_cycle_inner(&app_config, db, scanner, self.opts.scan_only).await;
    }

    async fn run_dry_run_cycle(&self, db: &Database, scanner: &Scanner) {
        let db_roots: Vec<_> = db
            .list_roots()
            .unwrap_or_default()
            .into_iter()
            .map(|r| r.path)
            .collect();
        let app_config =
            AppConfig::load(&self.paths, &db_roots).unwrap_or_else(|_| self.app_config.clone());

        tracing::info!("Starting dry-run scan");
        match refresh(db, scanner, &app_config).await {
            Ok(summary) => {
                eprintln!("Scan complete:");
                eprintln!(
                    "  {} directories, {} files, {} bytes",
                    summary.scan.total_directories,
                    summary.scan.total_files,
                    summary.scan.total_size_bytes
                );
                if summary.transitions.expired_to_pending > 0
                    || summary.transitions.expired_to_approved > 0
                    || summary.transitions.deferred_reset > 0
                {
                    eprintln!("Transitions:");
                    eprintln!(
                        "  {} expired → pending",
                        summary.transitions.expired_to_pending
                    );
                    eprintln!(
                        "  {} expired → approved",
                        summary.transitions.expired_to_approved
                    );
                    eprintln!("  {} deferred reset", summary.transitions.deferred_reset);
                }
            }
            Err(e) => {
                eprintln!("Scan failed: {e}");
            }
        }

        // Report what would be removed without doing it.
        let roots = db.list_roots().unwrap_or_default();
        let mut total_removable = 0usize;
        let mut total_blocked = 0usize;
        for root in &roots {
            match crate::removal::dry_run_approved(db, root.id) {
                Ok(result) => {
                    total_removable += result.removable_count;
                    total_blocked += result.failures.len();
                    for failure in &result.failures {
                        eprintln!(
                            "  would fail: {} ({})",
                            failure.path.display(),
                            failure.reason
                        );
                    }
                }
                Err(e) => {
                    eprintln!("Dry run failed for {}: {e}", root.path.display());
                }
            }
        }
        eprintln!();
        eprintln!("Dry run summary: {total_removable} removable, {total_blocked} would fail");
    }

    /// Execute one complete refresh/removal cycle.
    ///
    /// This function:
    /// 1. Refreshes the database (scan filesystem + transition expired files)
    /// 2. Removes approved paths (unless `scan_only` is true)
    ///
    /// Errors are logged but do not stop the daemon. Both steps are attempted
    /// even if one fails, ensuring maximum progress on each cycle.
    async fn run_cycle_inner(
        app_config: &AppConfig,
        db: &Database,
        scanner: &Scanner,
        scan_only: bool,
    ) {
        // Step 1: Refresh (scan + transition expired files using per-root configs)
        tracing::info!("Starting refresh cycle");
        tracing::debug!(
            tracked_path_count = app_config.global.tracked_paths.len(),
            auto_remove = app_config.global.auto_remove,
            expiration_days = app_config.global.expiration_days,
            warning_days = app_config.global.warning_days,
            "Daemon cycle config snapshot"
        );
        match refresh(db, scanner, app_config).await {
            Ok(summary) => {
                tracing::info!(
                    total_directories = summary.scan.total_directories,
                    total_files = summary.scan.total_files,
                    total_size_bytes = summary.scan.total_size_bytes,
                    "Scan completed successfully"
                );
                if summary.transitions.expired_to_pending > 0
                    || summary.transitions.expired_to_approved > 0
                    || summary.transitions.deferred_reset > 0
                {
                    tracing::info!(
                        expired_to_pending = summary.transitions.expired_to_pending,
                        expired_to_approved = summary.transitions.expired_to_approved,
                        deferred_reset = summary.transitions.deferred_reset,
                        "State transitions completed"
                    );
                }
                tracing::debug!(
                    expired_to_pending = summary.transitions.expired_to_pending,
                    expired_to_approved = summary.transitions.expired_to_approved,
                    deferred_reset = summary.transitions.deferred_reset,
                    "Transition summary (debug detail)"
                );
            }
            Err(e) => {
                tracing::warn!(error = ?e, "Refresh failed, continuing to removal step");
            }
        }

        // Step 2: Remove approved paths
        if scan_only {
            tracing::info!("Scan-only mode, skipping removal step");
            return;
        }
        tracing::info!("Removing approved paths");
        match remove_approved(db) {
            Ok(summary) => {
                if summary.removed_count() > 0 || summary.blocked_count() > 0 {
                    tracing::info!(
                        removed_count = summary.removed_count(),
                        blocked_count = summary.blocked_count(),
                        total_bytes_freed = summary.total_bytes_freed(),
                        "Removal completed"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = ?e, "Removal failed");
            }
        }
    }
}

/// A held PID lock file that is cleaned up on drop.
struct PidLock {
    path: std::path::PathBuf,
}

impl Drop for PidLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Acquire a PID lock file to prevent multiple daemon instances.
///
/// Writes the current process ID to the lock file. If the file already exists
/// and the recorded PID is still running, returns an error. Stale lock files
/// from dead processes are automatically cleaned up.
fn acquire_pid_lock(path: &std::path::Path) -> Result<PidLock> {
    use std::io::Read;

    if path.exists() {
        let mut contents = String::new();
        if let Ok(mut file) = std::fs::File::open(path) {
            let _ = file.read_to_string(&mut contents);
        }
        if let Ok(pid) = contents.trim().parse::<u32>() {
            if is_process_running(pid) {
                return Err(crate::error::Error::Config(format!(
                    "Another daemon instance is already running (PID {pid}). \
                     Lock file: {}",
                    path.display()
                )));
            }
            tracing::info!(
                stale_pid = pid,
                "Removing stale lock file from dead process"
            );
        }
    }

    std::fs::write(path, std::process::id().to_string())?;
    Ok(PidLock {
        path: path.to_path_buf(),
    })
}

fn is_process_running(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}
