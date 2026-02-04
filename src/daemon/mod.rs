//! Background daemon for scanning and scheduled removals.

use std::time::Duration;

use tokio::time::sleep;

use crate::config::{AppPaths, Config};
use crate::db::Database;
use crate::error::Result;
use crate::removal::remove_approved;
use crate::scanner::{Scanner, refresh};

#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

#[cfg(not(unix))]
use tokio::signal;

/// Background daemon that handles periodic scanning and removal execution.
pub struct Daemon {
    config: Config,
}

impl Daemon {
    /// Create a new daemon with the given configuration.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Run the daemon's main loop.
    ///
    /// This will:
    /// - Periodically scan tracked paths
    /// - Transition expired paths to pending or approved status
    /// - Execute approved removals
    /// - Sleep for the configured interval
    ///
    /// The daemon runs continuously until interrupted by SIGINT or SIGTERM.
    /// Errors during scan/transition/removal cycles are logged but do not stop the daemon.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database cannot be opened
    /// - Signal handlers cannot be registered (Unix only)
    pub async fn run(&self) -> Result<()> {
        tracing::info!(
            scan_interval_hours = self.config.scan_interval_hours,
            tracked_paths = ?self.config.tracked_paths,
            "Starting stagecrew daemon"
        );

        // Open database (path derived from config)
        let paths = AppPaths::new();
        let db_path = paths.database_file(&self.config)?;
        let db = Database::open(&db_path)?;
        let scanner = Scanner::new();

        // Set up graceful shutdown for both SIGINT and SIGTERM
        #[cfg(unix)]
        let mut sigint = signal(SignalKind::interrupt())?;
        #[cfg(unix)]
        let mut sigterm = signal(SignalKind::terminate())?;
        #[cfg(not(unix))]
        let mut shutdown = Box::pin(signal::ctrl_c());

        loop {
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
                () = self.run_cycle(&db, &scanner) => {}
            }

            #[cfg(not(unix))]
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("Received shutdown signal, exiting gracefully");
                    break;
                }
                () = self.run_cycle(&db, &scanner) => {}
            }

            // Sleep for configured interval
            let sleep_duration =
                Duration::from_secs(u64::from(self.config.scan_interval_hours) * 3600);

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

    /// Execute one complete refresh/removal cycle.
    ///
    /// This function:
    /// 1. Refreshes the database (scan filesystem + transition expired files)
    /// 2. Removes approved paths
    ///
    /// Errors are logged but do not stop the daemon. Both steps are attempted
    /// even if one fails, ensuring maximum progress on each cycle.
    async fn run_cycle(&self, db: &Database, scanner: &Scanner) {
        // Step 1: Refresh (scan + transition expired files)
        tracing::info!("Starting refresh cycle");
        match refresh(
            db,
            scanner,
            &self.config.tracked_paths,
            self.config.expiration_days,
            self.config.warning_days,
            self.config.auto_remove,
        )
        .await
        {
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
            }
            Err(e) => {
                tracing::warn!(error = ?e, "Refresh failed, continuing to removal step");
            }
        }

        // Step 2: Remove approved paths
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
