//! CLI subcommand definitions and handlers.

use clap::{
    Parser, Subcommand,
    builder::{
        Styles,
        styling::{AnsiColor, Effects},
    },
};
use std::path::PathBuf;

pub const INFO: &str = "\
━━━━━━━━━━━━━━━━━━━━━━━━━━━
┏━┓╺┳╸┏━┓┏━╸┏━╸┏━╸┏━┓┏━╸╻ ╻
┗━┓ ┃ ┣━┫┃╺┓┣╸ ┃  ┣┳┛┣╸ ┃╻┃
┗━┛ ╹ ╹ ╹┗━┛┗━╸┗━╸╹┗╸┗━╸┗┻┛
━━━━━━━━━━━━━━━━━━━━━━━━━━━";

// const AFTER_HELP: &str = "\
// Examples:
//   samx input.bam -r reference.gbk              Stream to stdout (Arrow IPC)
//   samx input.bam -r reference.gbk | duckdb     Pipe to DuckDB
//   samx input.bam -r reference.gbk -o out.parquet   Write to file
//   samtools view -h input.cram | samx - -r ref.gbk  Read from stdin";

const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Green.on_default().effects(Effects::BOLD))
    .usage(AnsiColor::Green.on_default().effects(Effects::BOLD))
    .literal(AnsiColor::Cyan.on_default().effects(Effects::BOLD))
    .placeholder(AnsiColor::Yellow.on_default());

/// Stagecrew: Disk usage management with automatic cleanup policies.
#[derive(Debug, Parser)]
#[command(name = "stagecrew")]
#[command(version, about = INFO, styles = STYLES, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[command(flatten)]
    pub verbose: clap_verbosity_flag::Verbosity,
}

#[derive(Debug, Default, Subcommand)]
pub enum Command {
    /// Launch the interactive TUI (default if no command specified)
    #[default]
    Tui,

    /// Run the background scanner and removal daemon.
    ///
    /// The daemon periodically scans all tracked directories, transitions
    /// expired files to pending or approved status based on configuration,
    /// and executes approved removals. It runs with the current user's
    /// permissions and never uses sudo or elevated privileges.
    ///
    /// Files are never removed without explicit approval (unless `auto_remove`
    /// is enabled in configuration). The daemon logs all actions to the audit
    /// trail for accountability.
    ///
    /// Stop the daemon gracefully with Ctrl+C or SIGTERM.
    Daemon {
        /// Override the scan interval (in hours) from configuration.
        #[arg(long, value_name = "HOURS")]
        interval: Option<u32>,

        /// Run one scan/removal cycle and exit instead of looping.
        #[arg(long)]
        once: bool,

        /// Scan and transition files but skip the removal step.
        /// Useful for observing what would happen without deleting anything.
        #[arg(long)]
        scan_only: bool,

        /// Scan and report what would be removed, but do not modify any files
        /// or database state. Implies --once.
        #[arg(long)]
        dry_run: bool,
    },

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

    /// View and manage stagecrew configuration.
    ///
    /// Without a subcommand, prints the current effective configuration
    /// as TOML. Use subcommands to inspect file paths or open the config
    /// in your editor.
    #[command(subcommand)]
    Config(ConfigCommand),

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

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Print the current effective configuration as TOML.
    Show,

    /// Print the path to the configuration file.
    Path,

    /// Print the path to the database file.
    DbPath,

    /// Print the path to the log file.
    LogPath,

    /// Open the configuration file in $VISUAL or $EDITOR.
    Edit,

    /// Print the JSON Schema for the configuration file.
    ///
    /// The schema describes all valid configuration fields and their types.
    /// Pipe to a file for offline editor integration, or use the versioned
    /// URL in the generated config's `#:schema` comment for automatic support.
    Schema,
}
