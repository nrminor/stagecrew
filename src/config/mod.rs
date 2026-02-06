//! Configuration loading, saving, and path management.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

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

/// The filename used for per-root local configuration files.
pub const LOCAL_CONFIG_FILENAME: &str = "stagecrew.toml";

/// Per-root configuration overrides parsed from local `stagecrew.toml` files.
///
/// This struct intentionally excludes `tracked_paths` and `database_path` since
/// those settings only make sense at the global level (there's one database for
/// all roots). The `deny_unknown_fields` attribute ensures users get a clear
/// error if they try to set unsupported fields.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct LocalConfig {
    expiration_days: Option<u32>,
    warning_days: Option<u32>,
    auto_remove: Option<bool>,
    scan_interval_hours: Option<u32>,
}

impl LocalConfig {
    /// Load a local config from a root directory, if present.
    ///
    /// Returns `Ok(None)` if no config file exists. Returns `Err` if the file
    /// exists but cannot be read or parsed.
    fn load(root: &Path) -> Result<Option<Self>> {
        let config_path = root.join(LOCAL_CONFIG_FILENAME);

        if !config_path.exists() {
            return Ok(None);
        }

        let contents = std::fs::read_to_string(&config_path).map_err(|e| Error::Filesystem {
            path: config_path.clone(),
            source: e,
        })?;

        let local: Self = toml::from_str(&contents).map_err(|e| Error::Config(e.to_string()))?;

        Ok(Some(local))
    }

    /// Merge this local config into a base config, returning a new config with
    /// local values overriding the base where present.
    fn merge_into(&self, base: &Config) -> Config {
        Config {
            tracked_paths: base.tracked_paths.clone(),
            expiration_days: self.expiration_days.unwrap_or(base.expiration_days),
            warning_days: self.warning_days.unwrap_or(base.warning_days),
            auto_remove: self.auto_remove.unwrap_or(base.auto_remove),
            scan_interval_hours: self.scan_interval_hours.unwrap_or(base.scan_interval_hours),
            database_path: base.database_path.clone(),
        }
    }
}

/// Application configuration with per-root overrides.
///
/// This struct holds the global configuration and pre-merged per-root configs.
/// Use `for_root()` to get the effective configuration for a specific root,
/// which will return the merged local+global config if a local config exists,
/// or the global config otherwise.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// The global configuration loaded from the user's config file.
    pub global: Config,
    per_root: HashMap<PathBuf, Config>,
}

impl AppConfig {
    /// Create an `AppConfig` from a global config with no per-root overrides.
    ///
    /// Call `load_per_root()` afterward to discover and load local configs.
    #[must_use]
    pub fn from_global(global: Config) -> Self {
        Self {
            global,
            per_root: HashMap::new(),
        }
    }

    /// Discover and load per-root configuration files.
    ///
    /// For each root path, looks for a `stagecrew.toml` file and merges it with
    /// the global config. If a local config file exists but has parse errors,
    /// logs a warning and uses the global config for that root.
    ///
    /// This method clears any previously loaded per-root configs before loading.
    pub fn load_per_root(&mut self, roots: &[PathBuf]) {
        self.per_root.clear();

        for root in roots {
            match LocalConfig::load(root) {
                Ok(Some(local)) => {
                    let merged = local.merge_into(&self.global);
                    self.per_root.insert(root.clone(), merged);
                }
                Ok(None) => {
                    // No local config file, will fall back to global
                }
                Err(e) => {
                    tracing::warn!(
                        root = %root.display(),
                        error = %e,
                        "Failed to load local config, using global config for this root"
                    );
                }
            }
        }
    }

    /// Get the effective configuration for a root.
    ///
    /// Returns the merged local+global config if a local config was loaded for
    /// this root, otherwise returns the global config.
    #[must_use]
    pub fn for_root(&self, root: &Path) -> &Config {
        self.per_root.get(root).unwrap_or(&self.global)
    }

    /// Create a fresh `AppConfig` by reloading from disk.
    ///
    /// Loads the global config and discovers per-root configs for the given roots.
    /// This is a convenience method equivalent to `from_global` + `load_per_root`.
    ///
    /// # Errors
    ///
    /// Returns an error if the global config cannot be loaded.
    pub fn load(paths: &AppPaths, roots: &[PathBuf]) -> Result<Self> {
        let global = Config::load(paths)?;
        let mut app_config = Self::from_global(global);
        app_config.load_per_root(roots);
        Ok(app_config)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

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

    #[test]
    fn local_config_loads_from_root_directory() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root = temp_dir.path();

        let toml_content = r"
            expiration_days = 30
            warning_days = 7
            auto_remove = true
        ";
        std::fs::write(root.join(LOCAL_CONFIG_FILENAME), toml_content).expect("write local config");

        let local = LocalConfig::load(root)
            .expect("load should succeed")
            .expect("local config should exist");

        assert_eq!(local.expiration_days, Some(30));
        assert_eq!(local.warning_days, Some(7));
        assert_eq!(local.auto_remove, Some(true));
        assert_eq!(local.scan_interval_hours, None);
    }

    #[test]
    fn local_config_returns_none_when_missing() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root = temp_dir.path();

        let result = LocalConfig::load(root).expect("load should succeed");

        assert!(result.is_none());
    }

    #[test]
    fn local_config_rejects_tracked_paths() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root = temp_dir.path();

        let toml_content = r#"
            tracked_paths = ["/should/not/work"]
            expiration_days = 30
        "#;
        std::fs::write(root.join(LOCAL_CONFIG_FILENAME), toml_content).expect("write local config");

        let result = LocalConfig::load(root);

        assert!(
            result.is_err(),
            "should reject tracked_paths in local config"
        );
    }

    #[test]
    fn local_config_rejects_unknown_fields() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root = temp_dir.path();

        let toml_content = r#"
            expiration_days = 30
            unknown_field = "should fail"
        "#;
        std::fs::write(root.join(LOCAL_CONFIG_FILENAME), toml_content).expect("write local config");

        let result = LocalConfig::load(root);

        assert!(result.is_err(), "should reject unknown fields");
    }

    #[test]
    #[cfg(unix)]
    fn local_config_unreadable_file_returns_error() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root = temp_dir.path();
        let config_path = root.join(LOCAL_CONFIG_FILENAME);

        std::fs::write(&config_path, "expiration_days = 30").expect("write local config");
        std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o000))
            .expect("set permissions");

        let result = LocalConfig::load(root);

        // Restore permissions so temp_dir cleanup works
        std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o644))
            .expect("restore permissions");

        assert!(result.is_err(), "should fail on unreadable file");
    }

    #[test]
    fn local_config_nonexistent_root_returns_none() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let nonexistent_root = temp_dir.path().join("does_not_exist");

        let result = LocalConfig::load(&nonexistent_root);

        // The root directory doesn't exist, so there's no config file to find
        let local_config = result.expect("should not error when root doesn't exist");
        assert!(
            local_config.is_none(),
            "should return None when root doesn't exist"
        );
    }

    #[test]
    fn local_config_path_is_directory_returns_error() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root = temp_dir.path();

        // Create stagecrew.toml as a directory instead of a file
        std::fs::create_dir(root.join(LOCAL_CONFIG_FILENAME)).expect("create config as directory");

        let result = LocalConfig::load(root);

        assert!(
            result.is_err(),
            "should error when config path is a directory"
        );
    }

    #[test]
    fn local_config_merge_overrides_base_values() {
        let base = Config {
            tracked_paths: vec![PathBuf::from("/data")],
            expiration_days: 90,
            warning_days: 14,
            auto_remove: false,
            scan_interval_hours: 24,
            database_path: Some(PathBuf::from("/global.db")),
        };

        let local = LocalConfig {
            expiration_days: Some(30),
            warning_days: None,
            auto_remove: Some(true),
            scan_interval_hours: None,
        };

        let merged = local.merge_into(&base);

        assert_eq!(merged.tracked_paths, vec![PathBuf::from("/data")]);
        assert_eq!(merged.expiration_days, 30);
        assert_eq!(merged.warning_days, 14);
        assert!(merged.auto_remove);
        assert_eq!(merged.scan_interval_hours, 24);
        // database_path is global-only, so it's always preserved from base
        assert_eq!(merged.database_path, Some(PathBuf::from("/global.db")));
    }

    #[test]
    fn local_config_merge_preserves_base_when_none() {
        let base = Config {
            tracked_paths: vec![PathBuf::from("/data")],
            expiration_days: 90,
            warning_days: 14,
            auto_remove: false,
            scan_interval_hours: 24,
            database_path: Some(PathBuf::from("/global.db")),
        };

        let local = LocalConfig::default();

        let merged = local.merge_into(&base);

        assert_eq!(merged.expiration_days, 90);
        assert_eq!(merged.warning_days, 14);
        assert!(!merged.auto_remove);
        assert_eq!(merged.scan_interval_hours, 24);
        assert_eq!(merged.database_path, Some(PathBuf::from("/global.db")));
    }

    #[test]
    fn local_config_rejects_wrong_field_types() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root = temp_dir.path();

        let toml_content = r#"expiration_days = "not a number""#;
        std::fs::write(root.join(LOCAL_CONFIG_FILENAME), toml_content).expect("write local config");

        let result = LocalConfig::load(root);

        assert!(result.is_err(), "should reject wrong field type");
    }

    #[test]
    fn local_config_rejects_database_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root = temp_dir.path();

        let toml_content = r#"database_path = "/absolute/path/to/db.sqlite""#;
        std::fs::write(root.join(LOCAL_CONFIG_FILENAME), toml_content).expect("write local config");

        let result = LocalConfig::load(root);

        assert!(
            result.is_err(),
            "should reject database_path in local config"
        );
    }

    #[test]
    fn local_config_merge_overrides_non_expiration_fields() {
        let base = Config::default();

        let local = LocalConfig {
            expiration_days: None,
            warning_days: Some(7),
            auto_remove: Some(true),
            scan_interval_hours: Some(12),
        };

        let merged = local.merge_into(&base);

        assert_eq!(merged.expiration_days, 90);
        assert_eq!(merged.warning_days, 7);
        assert!(merged.auto_remove);
        assert_eq!(merged.scan_interval_hours, 12);
        // database_path is global-only, preserved from base (which is None for default)
        assert_eq!(merged.database_path, None);
    }

    #[test]
    fn app_config_from_global_has_empty_per_root() {
        let global = Config::default();
        let app_config = AppConfig::from_global(global.clone());

        assert_eq!(app_config.global.expiration_days, global.expiration_days);
        assert!(app_config.per_root.is_empty());
    }

    #[test]
    fn app_config_for_root_returns_global_when_no_local() {
        let global = Config {
            expiration_days: 90,
            ..Config::default()
        };
        let app_config = AppConfig::from_global(global);

        let result = app_config.for_root(Path::new("/some/root"));

        assert_eq!(result.expiration_days, 90);
    }

    #[test]
    fn app_config_load_per_root_discovers_local_configs() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root1 = temp_dir.path().join("root1");
        let root2 = temp_dir.path().join("root2");
        std::fs::create_dir_all(&root1).expect("create root1");
        std::fs::create_dir_all(&root2).expect("create root2");

        std::fs::write(root1.join(LOCAL_CONFIG_FILENAME), "expiration_days = 30")
            .expect("write root1 config");

        let global = Config {
            expiration_days: 90,
            ..Config::default()
        };
        let mut app_config = AppConfig::from_global(global);
        app_config.load_per_root(&[root1.clone(), root2.clone()]);

        assert_eq!(app_config.for_root(&root1).expiration_days, 30);
        assert_eq!(app_config.for_root(&root2).expiration_days, 90);
    }

    #[test]
    fn app_config_load_per_root_clears_previous() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root1 = temp_dir.path().join("root1");
        let root2 = temp_dir.path().join("root2");
        std::fs::create_dir_all(&root1).expect("create root1");
        std::fs::create_dir_all(&root2).expect("create root2");

        std::fs::write(root1.join(LOCAL_CONFIG_FILENAME), "expiration_days = 30")
            .expect("write root1 config");
        std::fs::write(root2.join(LOCAL_CONFIG_FILENAME), "expiration_days = 60")
            .expect("write root2 config");

        let global = Config::default();
        let mut app_config = AppConfig::from_global(global);

        app_config.load_per_root(&[root1.clone(), root2.clone()]);
        assert_eq!(app_config.for_root(&root1).expiration_days, 30);
        assert_eq!(app_config.for_root(&root2).expiration_days, 60);

        app_config.load_per_root(std::slice::from_ref(&root2));
        // root1 should now fall back to global since it's no longer in the roots list
        assert_eq!(
            app_config.for_root(&root1).expiration_days,
            app_config.global.expiration_days
        );
        assert_eq!(app_config.for_root(&root2).expiration_days, 60);
    }

    #[test]
    fn app_config_handles_malformed_local_config() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root = temp_dir.path().join("root");
        std::fs::create_dir_all(&root).expect("create root");

        std::fs::write(
            root.join(LOCAL_CONFIG_FILENAME),
            "this is not valid toml {{{",
        )
        .expect("write bad config");

        let global = Config {
            expiration_days: 90,
            ..Config::default()
        };
        let mut app_config = AppConfig::from_global(global);
        app_config.load_per_root(std::slice::from_ref(&root));

        assert_eq!(app_config.for_root(&root).expiration_days, 90);
    }

    #[test]
    fn app_config_reload_refreshes_configs() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_path = temp_dir.path().join("config.toml");
        let root = temp_dir.path().join("root");
        std::fs::create_dir_all(&root).expect("create root");

        std::fs::write(&config_path, "expiration_days = 90").expect("write global config");
        std::fs::write(root.join(LOCAL_CONFIG_FILENAME), "expiration_days = 30")
            .expect("write local config");

        let paths = AppPaths::with_overrides(Some(config_path.clone()), None);
        let global = Config::load(&paths).expect("load global");
        let mut app_config = AppConfig::from_global(global);
        app_config.load_per_root(std::slice::from_ref(&root));

        assert_eq!(app_config.global.expiration_days, 90);
        assert_eq!(app_config.for_root(&root).expiration_days, 30);

        std::fs::write(&config_path, "expiration_days = 120").expect("update global config");
        std::fs::write(root.join(LOCAL_CONFIG_FILENAME), "expiration_days = 45")
            .expect("update local config");

        let app_config = AppConfig::load(&paths, std::slice::from_ref(&root)).expect("reload");

        assert_eq!(app_config.global.expiration_days, 120);
        assert_eq!(app_config.for_root(&root).expiration_days, 45);
    }
}
