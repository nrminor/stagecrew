//! CLI subcommand definitions and handlers.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Stagecrew: Disk usage management with automatic cleanup policies.
#[derive(Debug, Parser)]
#[command(name = "stagecrew")]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Default, Subcommand)]
pub enum Command {
    /// Launch the interactive TUI (default if no command specified)
    #[default]
    Tui,

    /// Run the background scanner and removal daemon
    Daemon,

    /// Show current status (used by shell hook)
    ///
    /// Note: The output format is intended for human consumption and may change
    /// between versions. For scripting, consider parsing the database directly.
    Status,

    /// Trigger a manual scan of tracked paths
    ///
    /// Note: The output format is intended for human consumption and may change
    /// between versions. For scripting, consider parsing the database directly.
    Scan {
        /// Specific path to scan (defaults to all configured paths)
        #[arg(short, long)]
        path: Option<PathBuf>,
    },

    /// Initialize or update configuration
    Init,

    /// Add a tracked path to the configuration
    ///
    /// Note: The output format is intended for human consumption and may change
    /// between versions. For scripting, consider editing the config file directly.
    Add {
        /// Path to track (must be a directory)
        path: PathBuf,

        /// Run an initial scan after adding the path
        #[arg(short, long)]
        scan: bool,
    },
}
