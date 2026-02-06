//! Configuration loading, saving, and path management.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use xdg::BaseDirectories;

use crate::error::{Error, Result};

/// Environment variable for overriding the config file path.
pub const ENV_CONFIG_PATH: &str = "STAGECREW_CONFIG_PATH";

/// Environment variable for overriding the database file path.
pub const ENV_DB_PATH: &str = "STAGECREW_DB_PATH";

/// Manages application paths following XDG Base Directory Specification.
///
/// The `STAGECREW_CONFIG_PATH` and `STAGECREW_DB_PATH` environment variables
/// can override the default config and database locations respectively. The
/// `with_overrides()` constructor allows tests to specify paths directly without
/// modifying environment variables.
pub struct AppPaths {
    xdg: BaseDirectories,
    config_path_override: Option<PathBuf>,
    db_path_override: Option<PathBuf>,
}

impl AppPaths {
    /// Initialize with application prefix "stagecrew", reading overrides from environment.
    ///
    /// Checks `STAGECREW_CONFIG_PATH` and `STAGECREW_DB_PATH` environment variables.
    /// Empty values are treated as unset.
    #[must_use]
    pub fn new() -> Self {
        Self::with_overrides(Self::env_path(ENV_CONFIG_PATH), Self::env_path(ENV_DB_PATH))
    }

    /// Initialize with explicit path overrides, primarily for testing.
    #[must_use]
    pub fn with_overrides(
        config_path_override: Option<PathBuf>,
        db_path_override: Option<PathBuf>,
    ) -> Self {
        let xdg = BaseDirectories::with_prefix("stagecrew");
        Self {
            xdg,
            config_path_override,
            db_path_override,
        }
    }

    /// Read a path from an environment variable, treating empty values as unset.
    fn env_path(var: &str) -> Option<PathBuf> {
        std::env::var(var)
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from)
    }

    /// Ensure the parent directory of a path exists, creating it if necessary.
    fn ensure_parent_exists(path: &std::path::Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    /// Path to config file. If `STAGECREW_CONFIG_PATH` is set (or an override was
    /// provided via `with_overrides()`), that path is used. Otherwise falls back to
    /// the XDG config directory (`~/.config/stagecrew/config.toml`).
    ///
    /// Creates the parent directory if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the config directory cannot be created.
    pub fn config_file(&self) -> std::io::Result<PathBuf> {
        if let Some(path) = &self.config_path_override {
            Self::ensure_parent_exists(path)?;
            return Ok(path.clone());
        }
        self.xdg.place_config_file("config.toml")
    }

    /// Path to log file: `~/.cache/stagecrew/stagecrew.log`
    ///
    /// # Errors
    ///
    /// Returns an error if the cache directory cannot be created.
    pub fn log_file(&self) -> std::io::Result<PathBuf> {
        self.xdg.place_cache_file("stagecrew.log")
    }

    /// Determine the database file path. Resolution order: environment variable
    /// override (`STAGECREW_DB_PATH`), then `config.database_path`, then derived
    /// from the first tracked path's parent, and finally the XDG data directory.
    ///
    /// This allows multiple users on a shared filesystem (like `CephFS`) to share
    /// a database located near the tracked paths.
    ///
    /// Creates the parent directory if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the database directory cannot be created.
    pub fn database_file(&self, config: &Config) -> std::io::Result<PathBuf> {
        if let Some(path) = &self.db_path_override {
            Self::ensure_parent_exists(path)?;
            return Ok(path.clone());
        }

        if let Some(db_path) = &config.database_path {
            Self::ensure_parent_exists(db_path)?;
            return Ok(db_path.clone());
        }

        if let Some(first_tracked) = config.tracked_paths.first()
            && let Some(parent) = first_tracked.parent()
        {
            let db_dir = parent.join(".stagecrew");
            std::fs::create_dir_all(&db_dir)?;
            return Ok(db_dir.join("stagecrew.db"));
        }

        self.xdg.place_data_file("stagecrew.db")
    }
}

impl Default for AppPaths {
    fn default() -> Self {
        Self::new()
    }
}

/// Application configuration.
///
/// Use `Config::default()` or `..Config::default()` to construct, as new fields
/// may be added in future versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
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

    /// Optional explicit path to the `SQLite` database.
    ///
    /// If not set, the database path is derived from the first tracked path's
    /// parent directory (e.g., `/shared/staging/.stagecrew/stagecrew.db`).
    /// This enables multiple users on a shared filesystem to use the same database.
    pub database_path: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tracked_paths: Vec::new(),
            expiration_days: 90,
            warning_days: 14,
            auto_remove: false,
            scan_interval_hours: 24,
            database_path: None,
        }
    }
}

impl Config {
    /// Load configuration from the default path.
    ///
    /// Performs tilde expansion on `tracked_paths` and `database_path` using the
    /// `shellexpand` crate. This allows configs to use `~/Downloads` and have it
    /// expanded to the user's home directory at load time.
    ///
    /// # Errors
    ///
    /// Returns an error if the config file cannot be read or parsed.
    pub fn load(paths: &AppPaths) -> Result<Self> {
        let config_path = paths.config_file()?;

        if !config_path.exists() {
            return Ok(Self::default());
        }

        let contents = std::fs::read_to_string(&config_path).map_err(|e| Error::Filesystem {
            path: config_path.clone(),
            source: e,
        })?;

        let mut config: Self =
            toml::from_str(&contents).map_err(|e| Error::Config(e.to_string()))?;

        // Expand tildes in tracked_paths
        config.tracked_paths = config
            .tracked_paths
            .into_iter()
            .map(|p| {
                let path_str = p.to_string_lossy();
                let expanded = shellexpand::tilde(&path_str);
                PathBuf::from(expanded.as_ref())
            })
            .collect();

        // Expand tilde in database_path if present
        if let Some(db_path) = config.database_path.take() {
            let path_str = db_path.to_string_lossy();
            let expanded = shellexpand::tilde(&path_str);
            config.database_path = Some(PathBuf::from(expanded.as_ref()));
        }

        Ok(config)
    }

    /// Save configuration to the default path.
    ///
    /// # Errors
    ///
    /// Returns an error if the config cannot be serialized or written to disk.
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn config_default_values() {
        let config = Config::default();
        assert!(config.tracked_paths.is_empty());
        assert_eq!(config.expiration_days, 90);
        assert_eq!(config.warning_days, 14);
        assert!(!config.auto_remove);
        assert_eq!(config.scan_interval_hours, 24);
        assert!(config.database_path.is_none());
    }

    #[test]
    fn config_serializes_to_toml() {
        let config = Config {
            tracked_paths: vec![PathBuf::from("/data/staging")],
            expiration_days: 60,
            warning_days: 7,
            auto_remove: true,
            scan_interval_hours: 12,
            database_path: Some(PathBuf::from("/shared/.stagecrew/db.sqlite")),
        };

        let toml_str = toml::to_string_pretty(&config).expect("serialization should succeed");

        // Verify key fields are present
        assert!(toml_str.contains("tracked_paths"));
        assert!(toml_str.contains("/data/staging"));
        assert!(toml_str.contains("expiration_days = 60"));
        assert!(toml_str.contains("warning_days = 7"));
        assert!(toml_str.contains("auto_remove = true"));
        assert!(toml_str.contains("scan_interval_hours = 12"));
        assert!(toml_str.contains("database_path"));
    }

    #[test]
    fn config_deserializes_from_toml() {
        let toml_str = r#"
            tracked_paths = ["/data/staging", "/scratch/user"]
            expiration_days = 30
            warning_days = 5
            auto_remove = false
            scan_interval_hours = 6
            database_path = "/custom/path/db.sqlite"
        "#;

        let config: Config = toml::from_str(toml_str).expect("deserialization should succeed");

        assert_eq!(config.tracked_paths.len(), 2);
        assert_eq!(config.tracked_paths[0], PathBuf::from("/data/staging"));
        assert_eq!(config.tracked_paths[1], PathBuf::from("/scratch/user"));
        assert_eq!(config.expiration_days, 30);
        assert_eq!(config.warning_days, 5);
        assert!(!config.auto_remove);
        assert_eq!(config.scan_interval_hours, 6);
        assert_eq!(
            config.database_path,
            Some(PathBuf::from("/custom/path/db.sqlite"))
        );
    }

    #[test]
    fn config_uses_defaults_for_missing_fields() {
        let toml_str = r#"
            tracked_paths = ["/data/staging"]
        "#;

        let config: Config = toml::from_str(toml_str).expect("deserialization should succeed");

        assert_eq!(config.tracked_paths.len(), 1);
        // All other fields should have defaults
        assert_eq!(config.expiration_days, 90);
        assert_eq!(config.warning_days, 14);
        assert!(!config.auto_remove);
        assert_eq!(config.scan_interval_hours, 24);
        assert!(config.database_path.is_none());
    }

    #[test]
    fn database_file_uses_explicit_path_when_set() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let explicit_db = temp_dir.path().join("custom/db.sqlite");

        let config = Config {
            database_path: Some(explicit_db.clone()),
            ..Config::default()
        };

        let paths = AppPaths::new();
        let result = paths.database_file(&config).expect("should resolve path");

        assert_eq!(result, explicit_db);
        // Parent directory should have been created
        assert!(
            explicit_db
                .parent()
                .expect(
                    "database path should have a parent directory - check that the path is not root"
                )
                .exists()
        );
    }

    #[test]
    fn database_file_derives_from_tracked_paths() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let tracked = temp_dir.path().join("staging/project");
        std::fs::create_dir_all(&tracked).expect("create tracked dir");

        let config = Config {
            tracked_paths: vec![tracked.clone()],
            database_path: None,
            ..Config::default()
        };

        let paths = AppPaths::new();
        let result = paths.database_file(&config).expect("should resolve path");

        // Should be: parent_of_tracked/.stagecrew/stagecrew.db
        let expected = temp_dir.path().join("staging/.stagecrew/stagecrew.db");
        assert_eq!(result, expected);
        // .stagecrew directory should have been created
        assert!(
            expected
                .parent()
                .expect(
                    "database path should have a parent directory - check that the path is not root"
                )
                .exists()
        );
    }

    #[test]
    fn database_file_falls_back_to_xdg_when_no_tracked_paths() {
        let config = Config::default();

        let paths = AppPaths::new();
        let result = paths.database_file(&config).expect("should resolve path");

        // Should end with stagecrew.db and be in an XDG data directory
        assert!(result.ends_with("stagecrew.db"));
        assert!(result.to_string_lossy().contains("stagecrew"));
    }

    #[test]
    fn database_file_explicit_path_takes_precedence_over_tracked_paths() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let explicit_db = temp_dir.path().join("explicit/db.sqlite");
        let tracked = temp_dir.path().join("staging/project");
        std::fs::create_dir_all(&tracked).expect("create tracked dir");

        let config = Config {
            database_path: Some(explicit_db.clone()),
            tracked_paths: vec![tracked],
            ..Config::default()
        };

        let paths = AppPaths::new();
        let result = paths.database_file(&config).expect("should resolve path");

        // Explicit path should win over derived path from tracked_paths
        assert_eq!(result, explicit_db);
    }

    #[test]
    fn config_load_expands_tilde_in_tracked_paths() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_path = temp_dir.path().join("config.toml");

        // Write config with tilde in tracked_paths
        let toml_content = r#"
tracked_paths = ["~/Downloads", "~/Documents/staging"]
expiration_days = 90
"#;
        std::fs::write(&config_path, toml_content).expect("write config file");

        // Load config using manual parse + expansion (mimics Config::load() logic)
        let contents = std::fs::read_to_string(&config_path).expect("read config file");
        let mut config: Config = toml::from_str(&contents).expect("parse config");

        // Apply the same expansion logic as Config::load()
        config.tracked_paths = config
            .tracked_paths
            .into_iter()
            .map(|p| {
                let path_str = p.to_string_lossy();
                let expanded = shellexpand::tilde(&path_str);
                PathBuf::from(expanded.as_ref())
            })
            .collect();

        // Verify tildes were expanded to actual home directory
        let home_dir = dirs::home_dir().expect("home directory should be available");
        assert_eq!(config.tracked_paths[0], home_dir.join("Downloads"));
        assert_eq!(config.tracked_paths[1], home_dir.join("Documents/staging"));
    }

    #[test]
    fn config_expands_tilde_in_database_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_path = temp_dir.path().join("config.toml");

        // Write config with tilde in database_path
        let toml_content = r#"
            tracked_paths = ["/data/staging"]
            database_path = "~/.local/share/stagecrew/db.sqlite"
        "#;
        std::fs::write(&config_path, toml_content).expect("write config file");

        // Load and verify expansion
        let contents = std::fs::read_to_string(&config_path).expect("read config file");
        let mut config: Config = toml::from_str(&contents).expect("parse config");

        // Manually perform tilde expansion (same as Config::load)
        if let Some(db_path) = config.database_path.take() {
            let path_str = db_path.to_string_lossy();
            let expanded = shellexpand::tilde(&path_str);
            config.database_path = Some(PathBuf::from(expanded.as_ref()));
        }

        // Verify tilde was expanded
        let home_dir = dirs::home_dir().expect("home directory should be available");
        assert_eq!(
            config.database_path,
            Some(home_dir.join(".local/share/stagecrew/db.sqlite"))
        );
    }

    #[test]
    fn config_handles_paths_without_tilde() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_path = temp_dir.path().join("config.toml");

        // Write config without tildes (absolute and relative paths)
        let toml_content = r#"
            tracked_paths = ["/data/staging", "./relative/path"]
            database_path = "/var/lib/stagecrew/db.sqlite"
        "#;
        std::fs::write(&config_path, toml_content).expect("write config file");

        // Load and verify no changes to paths without tildes
        let contents = std::fs::read_to_string(&config_path).expect("read config file");
        let mut config: Config = toml::from_str(&contents).expect("parse config");

        // Manually perform tilde expansion (same as Config::load)
        config.tracked_paths = config
            .tracked_paths
            .into_iter()
            .map(|p| {
                let path_str = p.to_string_lossy();
                let expanded = shellexpand::tilde(&path_str);
                PathBuf::from(expanded.as_ref())
            })
            .collect();

        if let Some(db_path) = config.database_path.take() {
            let path_str = db_path.to_string_lossy();
            let expanded = shellexpand::tilde(&path_str);
            config.database_path = Some(PathBuf::from(expanded.as_ref()));
        }

        // Verify paths unchanged when no tilde present
        assert_eq!(config.tracked_paths[0], PathBuf::from("/data/staging"));
        assert_eq!(config.tracked_paths[1], PathBuf::from("./relative/path"));
        assert_eq!(
            config.database_path,
            Some(PathBuf::from("/var/lib/stagecrew/db.sqlite"))
        );
    }

    #[test]
    fn config_expands_tilde_only_prefix() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_path = temp_dir.path().join("config.toml");

        // Write config with ~ at start but also a literal tilde in middle (should not expand middle)
        let toml_content = r#"
            tracked_paths = ["~/projects/~backup"]
        "#;
        std::fs::write(&config_path, toml_content).expect("write config file");

        // Load and verify only leading tilde is expanded
        let contents = std::fs::read_to_string(&config_path).expect("read config file");
        let mut config: Config = toml::from_str(&contents).expect("parse config");

        // Manually perform tilde expansion (same as Config::load)
        config.tracked_paths = config
            .tracked_paths
            .into_iter()
            .map(|p| {
                let path_str = p.to_string_lossy();
                let expanded = shellexpand::tilde(&path_str);
                PathBuf::from(expanded.as_ref())
            })
            .collect();

        // Verify only leading tilde was expanded
        let home_dir = dirs::home_dir().expect("home directory should be available");
        let expected = home_dir.join("projects/~backup");
        assert_eq!(config.tracked_paths[0], expected);
    }

    #[test]
    fn config_file_uses_override_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let custom_config = temp_dir.path().join("custom/config.toml");

        let paths = AppPaths::with_overrides(Some(custom_config.clone()), None);
        let result = paths.config_file().expect("should resolve path");

        assert_eq!(result, custom_config);
        // Parent directory should have been created
        assert!(custom_config.parent().expect("has parent").exists());
    }

    #[test]
    fn database_file_uses_override_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let custom_db = temp_dir.path().join("custom/stagecrew.db");

        let config = Config {
            database_path: Some(PathBuf::from("/should/be/ignored")),
            tracked_paths: vec![PathBuf::from("/also/ignored")],
            ..Config::default()
        };

        let paths = AppPaths::with_overrides(None, Some(custom_db.clone()));
        let result = paths.database_file(&config).expect("should resolve path");

        // Override should win over config.database_path and tracked_paths
        assert_eq!(result, custom_db);
        // Parent directory should have been created
        assert!(custom_db.parent().expect("has parent").exists());
    }

    #[test]
    fn database_file_override_beats_config_database_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let override_db = temp_dir.path().join("override/db.sqlite");
        let config_db = temp_dir.path().join("config/db.sqlite");

        let config = Config {
            database_path: Some(config_db),
            ..Config::default()
        };

        let paths = AppPaths::with_overrides(None, Some(override_db.clone()));
        let result = paths.database_file(&config).expect("should resolve path");

        // Override wins
        assert_eq!(result, override_db);
    }

    #[test]
    fn config_file_falls_back_to_xdg_when_no_override() {
        let paths = AppPaths::with_overrides(None, None);
        let result = paths.config_file().expect("should resolve path");

        assert!(result.ends_with(std::path::Path::new("stagecrew/config.toml")));
    }

    #[test]
    fn database_file_falls_back_to_config_when_no_override() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_db = temp_dir.path().join("config/db.sqlite");

        let config = Config {
            database_path: Some(config_db.clone()),
            ..Config::default()
        };

        let paths = AppPaths::with_overrides(None, None);
        let result = paths.database_file(&config).expect("should resolve path");

        // Should use config.database_path since no override
        assert_eq!(result, config_db);
    }
}
