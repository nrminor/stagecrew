//! CLI subcommand definitions and handlers.

use clap::{
    Parser, Subcommand,
    builder::{
        Styles,
        styling::{AnsiColor, Effects},
    },
};
use std::path::PathBuf;

pub const INFO: &str = "
┏━┓╺┳╸┏━┓┏━╸┏━╸┏━╸┏━┓┏━╸╻ ╻
┗━┓ ┃ ┣━┫┃╺┓┣╸ ┃  ┣┳┛┣╸ ┃╻┃
┗━┛ ╹ ╹ ╹┗━┛┗━╸┗━╸╹┗╸┗━╸┗┻┛
";

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
