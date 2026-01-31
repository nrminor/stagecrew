//! Configuration loading, saving, and path management.

// TODO(cleanup): Remove these allows as functionality is implemented and used.
// Tracking issue: log_dir is defined but not yet wired into daemon.
#![allow(dead_code)]

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use xdg::BaseDirectories;

use crate::error::{Error, Result};

/// Manages application paths following XDG Base Directory Specification.
pub struct AppPaths {
    xdg: BaseDirectories,
}

impl AppPaths {
    /// Initialize with application prefix "stagecrew".
    pub fn new() -> Self {
        let xdg = BaseDirectories::with_prefix("stagecrew");
        Self { xdg }
    }

    /// Path to config file: ~/.config/stagecrew/config.toml
    pub fn config_file(&self) -> std::io::Result<PathBuf> {
        self.xdg.place_config_file("config.toml")
    }

    /// Path to `SQLite` database: `~/.local/share/stagecrew/stagecrew.db`
    pub fn database_file(&self) -> std::io::Result<PathBuf> {
        self.xdg.place_data_file("stagecrew.db")
    }

    /// Path to log directory: ~/.local/state/stagecrew/
    pub fn log_dir(&self) -> std::io::Result<PathBuf> {
        self.xdg.create_state_directory("")
    }
}

impl Default for AppPaths {
    fn default() -> Self {
        Self::new()
    }
}

/// Application configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Paths to track for cleanup.
    pub tracked_paths: Vec<PathBuf>,

    /// Default expiration period in days.
    pub expiration_days: u32,

    /// Days before expiration to start warning.
    pub warning_days: u32,

    /// Whether to auto-remove files without approval.
    pub auto_remove: bool,

    /// Scan interval in hours for the daemon.
    pub scan_interval_hours: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tracked_paths: Vec::new(),
            expiration_days: 90,
            warning_days: 14,
            auto_remove: false,
            scan_interval_hours: 24,
        }
    }
}

impl Config {
    /// Load configuration from the default path.
    pub fn load(paths: &AppPaths) -> Result<Self> {
        let config_path = paths.config_file()?;

        if !config_path.exists() {
            return Ok(Self::default());
        }

        let contents = std::fs::read_to_string(&config_path).map_err(|e| Error::Filesystem {
            path: config_path.clone(),
            source: e,
        })?;

        toml::from_str(&contents).map_err(|e| Error::Config(e.to_string()))
    }

    /// Save configuration to the default path.
    pub fn save(&self, paths: &AppPaths) -> Result<()> {
        let config_path = paths.config_file()?;
        let contents = toml::to_string_pretty(self).map_err(|e| Error::Config(e.to_string()))?;

        std::fs::write(&config_path, contents).map_err(|e| Error::Filesystem {
            path: config_path,
            source: e,
        })?;

        Ok(())
    }
}
