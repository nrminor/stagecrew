//! Background daemon for scanning and scheduled removals.

use crate::config::Config;
use crate::error::Result;

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
    /// - Execute approved removals
    /// - Update statistics for the shell hook
    // TODO(cleanup): Remove allow once tokio::interval and async scanning are implemented.
    #[allow(clippy::unused_async)]
    pub async fn run(&self) -> Result<()> {
        tracing::info!(
            scan_interval_hours = self.config.scan_interval_hours,
            "Starting stagecrew daemon"
        );

        // TODO: Implement daemon loop with:
        // - Periodic scanning via tokio interval
        // - Removal execution for approved items
        // - Graceful shutdown handling

        Ok(())
    }
}
