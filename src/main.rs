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

use clap::Parser;
use color_eyre::eyre::{Context, Result};
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command};
use config::{AppPaths, Config};
use db::Database;

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

    // Initialize paths and config
    let paths = AppPaths::new();
    let config = Config::load(&paths).context("Failed to load configuration")?;

    // Open database
    let db_path = paths
        .database_file()
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
            // TODO: Query stats and print summary for shell hook
            println!("stagecrew status: not yet implemented");
        }

        Command::Scan { path } => {
            let scanner = scanner::Scanner::new();
            let target = path.unwrap_or_else(|| std::path::PathBuf::from("."));
            tracing::info!(?target, "Starting scan");
            let _result = scanner.scan(&target).await?;
            println!("Scan complete");
        }

        Command::Init => {
            config
                .save(&paths)
                .context("Failed to save configuration")?;
            println!(
                "Configuration initialized at: {}",
                paths.config_file()?.display()
            );
        }
    }

    Ok(())
}
