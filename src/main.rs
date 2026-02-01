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

    // Initialize paths
    let paths = AppPaths::new();

    // Handle init command separately since it may need to create config
    if matches!(command, Command::Init) {
        handle_init(&paths)?;
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

        Command::Init => unreachable!("Init handled above"),
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
