//! Background daemon for scanning and scheduled removals.

use std::path::Path;
use std::time::Duration;

use jiff::Timestamp;
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
    log_path: std::path::PathBuf,
    log_filter: String,
    log_filter_source: &'static str,
}

impl Daemon {
    /// Create a new daemon with the given configuration and runtime options.
    pub fn new(
        app_config: AppConfig,
        opts: DaemonOptions,
        log_path: std::path::PathBuf,
        log_filter: String,
        log_filter_source: &'static str,
    ) -> Self {
        Self {
            app_config,
            paths: AppPaths::new(),
            opts,
            log_path,
            log_filter,
            log_filter_source,
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
        let next_scheduled_scan = next_anchored_scan_from_config(
            config.scan_start_time.as_deref(),
            interval_hours,
            Timestamp::now(),
        )?;

        // Startup banner
        self.print_startup_banner(&db_path, interval_hours, config, next_scheduled_scan);

        if self.opts.dry_run {
            tracing::info!("Dry-run mode: no files will be modified");
            self.run_dry_run_cycle_interruptible(&db, &scanner).await?;
            return Ok(());
        }

        if self.opts.once {
            self.run_single_cycle_interruptible(&db, &db_path, &scanner)
                .await?;
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

            let cycle_interval_hours = self
                .opts
                .interval_hours
                .unwrap_or(app_config.global.scan_interval_hours);

            if let Some(next_scan) = next_anchored_scan_from_config(
                app_config.global.scan_start_time.as_deref(),
                cycle_interval_hours,
                Timestamp::now(),
            )? {
                let sleep_duration = duration_until(next_scan, Timestamp::now())?;

                tracing::info!(
                    configured_start = %app_config.global.scan_start_time.as_deref().unwrap_or_default(),
                    next_scan = %next_scan,
                    ?sleep_duration,
                    "Waiting for next anchored scan slot"
                );

                #[cfg(unix)]
                tokio::select! {
                    _ = sigint.recv() => {
                        tracing::info!("Received SIGINT before anchored scan, exiting gracefully");
                        break;
                    }
                    _ = sigterm.recv() => {
                        tracing::info!("Received SIGTERM before anchored scan, exiting gracefully");
                        break;
                    }
                    () = sleep(sleep_duration) => {}
                }

                #[cfg(not(unix))]
                tokio::select! {
                    _ = &mut shutdown => {
                        tracing::info!("Received shutdown signal before anchored scan, exiting gracefully");
                        break;
                    }
                    () = sleep(sleep_duration) => {}
                }
            }

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
                () = Self::run_cycle_inner(&app_config, &db, &db_path, &scanner, self.opts.scan_only) => {}
            }

            #[cfg(not(unix))]
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("Received shutdown signal, exiting gracefully");
                    break;
                }
                () = Self::run_cycle_inner(&app_config, &db, &db_path, &scanner, self.opts.scan_only) => {}
            }

            if app_config.global.scan_start_time.is_some() {
                continue;
            }

            // No configured anchor: preserve legacy immediate-start behavior and
            // sleep for the full interval after each completed cycle.
            let sleep_duration = Duration::from_secs(u64::from(cycle_interval_hours) * 3600);

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

    fn print_startup_banner(
        &self,
        db_path: &std::path::Path,
        interval_hours: u32,
        config: &crate::config::Config,
        next_scheduled_scan: Option<Timestamp>,
    ) {
        let mode = if self.opts.dry_run {
            "dry-run"
        } else if self.opts.once {
            "single cycle"
        } else if self.opts.scan_only {
            "scan-only (no removals)"
        } else {
            "continuous"
        };

        eprintln!("{}", crate::cli::INFO);
        eprintln!();
        eprintln!("  mode:            {mode}");
        eprintln!("  database:        {}", db_path.display());
        eprintln!("  logs:            {}", self.log_path.display());
        eprintln!(
            "  log filter:      {} ({})",
            self.log_filter, self.log_filter_source
        );
        eprintln!("  scan interval:   {interval_hours}h");
        if let Some(scan_start_time) = &config.scan_start_time {
            eprintln!("  scan start:      {scan_start_time}");
            if let Some(next_scan) = next_scheduled_scan {
                eprintln!("  next scan:       {next_scan}");
            }
        } else {
            eprintln!("  scan start:      immediate on launch");
        }
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
            scan_start_time = ?config.scan_start_time,
            next_scheduled_scan = ?next_scheduled_scan,
            expiration_days = config.expiration_days,
            warning_days = config.warning_days,
            auto_remove = config.auto_remove,
            tracked_path_count = config.tracked_paths.len(),
            db_path = %db_path.display(),
            log_path = %self.log_path.display(),
            log_filter = %self.log_filter,
            log_filter_source = self.log_filter_source,
            "Daemon started"
        );
    }

    async fn run_single_cycle_interruptible(
        &self,
        db: &Database,
        db_path: &Path,
        scanner: &Scanner,
    ) -> Result<()> {
        let db_roots: Vec<_> = db
            .list_roots()
            .unwrap_or_default()
            .into_iter()
            .map(|r| r.path)
            .collect();
        let app_config =
            AppConfig::load(&self.paths, &db_roots).unwrap_or_else(|_| self.app_config.clone());

        #[cfg(unix)]
        {
            let mut sigint = signal(SignalKind::interrupt())?;
            let mut sigterm = signal(SignalKind::terminate())?;
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT during single-cycle run, exiting gracefully");
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM during single-cycle run, exiting gracefully");
                }
                () = Self::run_cycle_inner(&app_config, db, db_path, scanner, self.opts.scan_only) => {}
            }
        }

        #[cfg(not(unix))]
        {
            let mut shutdown = Box::pin(signal::ctrl_c());
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("Received shutdown signal during single-cycle run, exiting gracefully");
                }
                () = Self::run_cycle_inner(&app_config, db, db_path, scanner, self.opts.scan_only) => {}
            }
        }

        Ok(())
    }

    async fn run_dry_run_cycle_interruptible(
        &self,
        db: &Database,
        scanner: &Scanner,
    ) -> Result<()> {
        let db_roots: Vec<_> = db
            .list_roots()
            .unwrap_or_default()
            .into_iter()
            .map(|r| r.path)
            .collect();
        let app_config =
            AppConfig::load(&self.paths, &db_roots).unwrap_or_else(|_| self.app_config.clone());

        #[cfg(unix)]
        {
            let mut sigint = signal(SignalKind::interrupt())?;
            let mut sigterm = signal(SignalKind::terminate())?;
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT during dry-run cycle, exiting gracefully");
                    return Ok(());
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM during dry-run cycle, exiting gracefully");
                    return Ok(());
                }
                () = self.run_dry_run_cycle_inner(db, scanner, &app_config) => {}
            }
        }

        #[cfg(not(unix))]
        {
            let mut shutdown = Box::pin(signal::ctrl_c());
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("Received shutdown signal during dry-run cycle, exiting gracefully");
                    return Ok(());
                }
                () = self.run_dry_run_cycle_inner(db, scanner, &app_config) => {}
            }
        }

        Ok(())
    }

    async fn run_dry_run_cycle_inner(
        &self,
        db: &Database,
        scanner: &Scanner,
        app_config: &AppConfig,
    ) {
        tracing::info!("Starting dry-run scan");
        match refresh(db, scanner, app_config).await {
            Ok(summary) => {
                eprintln!("Scan complete:");
                if summary.scan.total_files == summary.scan.unique_files
                    && summary.scan.total_size_bytes == summary.scan.unique_size_bytes
                {
                    eprintln!(
                        "  {} directories, {} files, {} bytes",
                        summary.scan.total_directories,
                        summary.scan.total_files,
                        summary.scan.total_size_bytes
                    );
                } else {
                    eprintln!(
                        "  {} directories, {} tracked files ({} bytes across roots, {} bytes unique)",
                        summary.scan.total_directories,
                        summary.scan.total_files,
                        summary.scan.total_size_bytes,
                        summary.scan.unique_size_bytes
                    );
                }
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
        db_path: &Path,
        scanner: &Scanner,
        scan_only: bool,
    ) {
        let cycle_start = std::time::Instant::now();

        // Accumulate context for the wide event emitted at cycle end.
        let mut scan_directories: u64 = 0;
        let mut scan_files: u64 = 0;
        let mut scan_bytes: u64 = 0;
        let mut expired_to_pending: u64 = 0;
        let mut expired_to_approved: u64 = 0;
        let mut deferred_reset: u64 = 0;
        let mut scan_outcome = "success";
        let mut removed_count: usize = 0;
        let mut blocked_count: usize = 0;
        let mut bytes_freed: i64 = 0;
        let mut removal_outcome = if scan_only { "skipped" } else { "success" };

        // Step 1: Refresh (scan + transition expired files)
        match refresh(db, scanner, app_config).await {
            Ok(summary) => {
                scan_directories = summary.scan.total_directories;
                scan_files = summary.scan.total_files;
                scan_bytes = summary.scan.total_size_bytes;
                expired_to_pending = summary.transitions.expired_to_pending;
                expired_to_approved = summary.transitions.expired_to_approved;
                deferred_reset = summary.transitions.deferred_reset;
            }
            Err(e) => {
                scan_outcome = "failed";
                tracing::warn!(error = ?e, "Refresh failed, continuing to removal step");
            }
        }

        tokio::task::yield_now().await;

        // Step 2: Remove approved paths
        if !scan_only {
            let db_path = db_path.to_path_buf();
            match tokio::task::spawn_blocking(move || {
                let removal_db = Database::open(&db_path)?;
                remove_approved(&removal_db)
            })
            .await
            {
                Ok(Ok(summary)) => {
                    removed_count = summary.removed_count();
                    blocked_count = summary.blocked_count();
                    bytes_freed = summary.total_bytes_freed();
                }
                Ok(Err(e)) => {
                    removal_outcome = "failed";
                    tracing::warn!(error = ?e, "Removal failed");
                }
                Err(e) => {
                    removal_outcome = "failed";
                    tracing::warn!(error = ?e, "Removal task was cancelled or panicked");
                }
            }
        }

        let cycle_duration_ms = cycle_start.elapsed().as_millis();

        // Wide event: one emission per cycle with full context.
        tracing::info!(
            target: "stagecrew::daemon",
            cycle_duration_ms,
            scan.outcome = scan_outcome,
            scan.directories = scan_directories,
            scan.files = scan_files,
            scan.bytes = scan_bytes,
            transitions.expired_to_pending = expired_to_pending,
            transitions.expired_to_approved = expired_to_approved,
            transitions.deferred_reset = deferred_reset,
            removal.outcome = removal_outcome,
            removal.removed = removed_count,
            removal.blocked = blocked_count,
            removal.bytes_freed = bytes_freed,
            config.expiration_days = app_config.global.expiration_days,
            config.warning_days = app_config.global.warning_days,
            config.auto_remove = app_config.global.auto_remove,
            config.tracked_paths = app_config.global.tracked_paths.len(),
            "daemon_cycle"
        );
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

fn next_anchored_scan_from_config(
    scan_start_time: Option<&str>,
    interval_hours: u32,
    now: Timestamp,
) -> Result<Option<Timestamp>> {
    let Some(scan_start_time) = scan_start_time else {
        return Ok(None);
    };

    let anchor = scan_start_time.parse::<Timestamp>().map_err(|e| {
        crate::error::Error::Config(format!(
            "scan_start_time must be a valid RFC 3339 timestamp: {e}"
        ))
    })?;

    Ok(Some(next_anchored_scan(anchor, interval_hours, now)))
}

fn next_anchored_scan(anchor: Timestamp, interval_hours: u32, now: Timestamp) -> Timestamp {
    let anchor_seconds = anchor.as_second();
    let now_seconds = now.as_second();
    if now_seconds <= anchor_seconds {
        return anchor;
    }

    let interval_seconds = i64::from(interval_hours) * 3600;
    let elapsed = now_seconds - anchor_seconds;
    if elapsed % interval_seconds == 0 {
        return now;
    }
    let periods_elapsed = elapsed / interval_seconds;
    let next_seconds = anchor_seconds + ((periods_elapsed + 1) * interval_seconds);
    Timestamp::from_second(next_seconds).expect("computed next scan timestamp should be valid")
}

fn duration_until(target: Timestamp, now: Timestamp) -> Result<Duration> {
    let delta_seconds = target.as_second() - now.as_second();
    let seconds = u64::try_from(delta_seconds.max(0))
        .map_err(|_| crate::error::Error::Config("sleep duration overflow".to_string()))?;
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use jiff::Timestamp;

    use super::{next_anchored_scan, next_anchored_scan_from_config};

    #[test]
    fn anchored_schedule_returns_none_when_unset() {
        let now = "2026-04-03T08:30:00Z"
            .parse::<Timestamp>()
            .expect("parse now");
        let next = next_anchored_scan_from_config(None, 24, now).expect("compute schedule");
        assert!(next.is_none());
    }

    #[test]
    fn anchored_schedule_uses_future_anchor_directly() {
        let anchor = "2026-04-04T08:00:00Z"
            .parse::<Timestamp>()
            .expect("parse anchor");
        let now = "2026-04-03T08:30:00Z"
            .parse::<Timestamp>()
            .expect("parse now");
        assert_eq!(next_anchored_scan(anchor, 24, now), anchor);
    }

    #[test]
    fn anchored_schedule_snaps_to_next_interval_slot() {
        let anchor = "2026-04-03T08:00:00Z"
            .parse::<Timestamp>()
            .expect("parse anchor");
        let now = "2026-04-03T10:30:00Z"
            .parse::<Timestamp>()
            .expect("parse now");
        let expected = "2026-04-04T08:00:00Z"
            .parse::<Timestamp>()
            .expect("parse expected");
        assert_eq!(next_anchored_scan(anchor, 24, now), expected);
    }

    #[test]
    fn anchored_schedule_honors_exact_slot_without_drift() {
        let anchor = "2026-04-03T08:00:00Z"
            .parse::<Timestamp>()
            .expect("parse anchor");
        let now = "2026-04-04T08:00:00Z"
            .parse::<Timestamp>()
            .expect("parse now");
        assert_eq!(next_anchored_scan(anchor, 24, now), now);
    }

    #[test]
    fn anchored_schedule_supports_shorter_intervals() {
        let anchor = "2026-04-03T08:00:00Z"
            .parse::<Timestamp>()
            .expect("parse anchor");
        let now = "2026-04-03T13:01:00Z"
            .parse::<Timestamp>()
            .expect("parse now");
        let expected = "2026-04-03T14:00:00Z"
            .parse::<Timestamp>()
            .expect("parse expected");
        assert_eq!(next_anchored_scan(anchor, 6, now), expected);
    }
}
