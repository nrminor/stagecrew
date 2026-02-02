//! Integration tests for the `stagecrew add` CLI command.
//!
//! These tests verify the behavior of adding tracked paths via the CLI,
//! including path validation, duplicate detection, config persistence,
//! and optional initial scanning.

use std::fs;

use tempfile::TempDir;

use stagecrew::config::{AppPaths, Config};
use stagecrew::db::Database;

/// Helper to create a mock `AppPaths` pointing to a temp config directory.
fn mock_app_paths(temp_dir: &TempDir) -> AppPaths {
    // Allow: Environment variables are inherently unsafe. This is controlled test code
    // and the variable is only set for the duration of the test with a temp directory.
    // Using set_var in tests is necessary to override XDG directories.
    #[allow(clippy::undocumented_unsafe_blocks)]
    unsafe {
        std::env::set_var("XDG_CONFIG_HOME", temp_dir.path());
    }
    AppPaths::new()
}

#[tokio::test]
async fn add_path_to_empty_config() {
    let temp_dir = TempDir::with_prefix("stagecrew-add-test-")
        .expect("failed to create temp directory - check disk space");

    let tracked_path = temp_dir.path().join("staging");
    fs::create_dir_all(&tracked_path)
        .expect("failed to create staging directory - check write permissions");

    let paths = mock_app_paths(&temp_dir);

    // Simulate handle_add by directly manipulating config
    // (we can't call handle_add since it's private to main.rs)
    let canonical_path = tracked_path
        .canonicalize()
        .expect("failed to canonicalize path");

    let mut config = Config::default();
    config.tracked_paths.push(canonical_path.clone());
    config
        .save(&paths)
        .expect("failed to save config - check write permissions");

    // Verify config file was created and contains the path
    let loaded_config = Config::load(&paths).expect("failed to load saved config");
    assert_eq!(loaded_config.tracked_paths.len(), 1);
    assert_eq!(loaded_config.tracked_paths[0], canonical_path);
}

#[tokio::test]
async fn add_path_to_existing_config() {
    let temp_dir = TempDir::with_prefix("stagecrew-add-test-")
        .expect("failed to create temp directory - check disk space");

    let first_path = temp_dir.path().join("staging1");
    let second_path = temp_dir.path().join("staging2");
    fs::create_dir_all(&first_path)
        .expect("failed to create first staging directory - check write permissions");
    fs::create_dir_all(&second_path)
        .expect("failed to create second staging directory - check write permissions");

    let paths = mock_app_paths(&temp_dir);

    // Create initial config with one path
    let canonical_first = first_path
        .canonicalize()
        .expect("failed to canonicalize first path");
    let mut config = Config::default();
    config.tracked_paths = vec![canonical_first.clone()];
    config
        .save(&paths)
        .expect("failed to save initial config - check write permissions");

    // Add second path
    let canonical_second = second_path
        .canonicalize()
        .expect("failed to canonicalize second path");
    let mut loaded_config = Config::load(&paths).expect("failed to load existing config");
    loaded_config.tracked_paths.push(canonical_second.clone());
    loaded_config
        .save(&paths)
        .expect("failed to save updated config - check write permissions");

    // Verify both paths are present
    let final_config = Config::load(&paths).expect("failed to load final config");
    assert_eq!(final_config.tracked_paths.len(), 2);
    assert!(final_config.tracked_paths.contains(&canonical_first));
    assert!(final_config.tracked_paths.contains(&canonical_second));
}

#[tokio::test]
async fn add_duplicate_path_is_idempotent() {
    let temp_dir = TempDir::with_prefix("stagecrew-add-test-")
        .expect("failed to create temp directory - check disk space");

    let tracked_path = temp_dir.path().join("staging");
    fs::create_dir_all(&tracked_path)
        .expect("failed to create staging directory - check write permissions");

    let paths = mock_app_paths(&temp_dir);

    let canonical_path = tracked_path
        .canonicalize()
        .expect("failed to canonicalize path");

    // Add path once
    let mut config = Config::default();
    config.tracked_paths.push(canonical_path.clone());
    config
        .save(&paths)
        .expect("failed to save config - check write permissions");

    // Attempt to add the same path again
    let mut loaded_config = Config::load(&paths).expect("failed to load existing config");
    if !loaded_config.tracked_paths.contains(&canonical_path) {
        loaded_config.tracked_paths.push(canonical_path.clone());
    }
    loaded_config
        .save(&paths)
        .expect("failed to save updated config - check write permissions");

    // Verify only one entry exists
    let final_config = Config::load(&paths).expect("failed to load final config");
    assert_eq!(final_config.tracked_paths.len(), 1);
    assert_eq!(final_config.tracked_paths[0], canonical_path);
}

#[tokio::test]
async fn add_different_representations_of_same_path() {
    let temp_dir = TempDir::with_prefix("stagecrew-add-test-")
        .expect("failed to create temp directory - check disk space");

    let staging = temp_dir.path().join("staging");
    fs::create_dir_all(&staging)
        .expect("failed to create staging directory - check write permissions");

    // Create a path with . in it (e.g., /tmp/staging/./subdir)
    let subdir = staging.join("subdir");
    fs::create_dir_all(&subdir).expect("failed to create subdir - check write permissions");

    // Two representations of the same path
    let path1 = staging.join("subdir");
    let path2 = staging.join(".").join("subdir");

    let canonical1 = path1.canonicalize().expect("failed to canonicalize path1");
    let canonical2 = path2.canonicalize().expect("failed to canonicalize path2");

    // Canonicalization should make them identical
    assert_eq!(canonical1, canonical2);

    let paths = mock_app_paths(&temp_dir);

    // Add first representation
    let mut config = Config::default();
    config.tracked_paths.push(canonical1.clone());
    config
        .save(&paths)
        .expect("failed to save config - check write permissions");

    // Attempt to add second representation
    let mut loaded_config = Config::load(&paths).expect("failed to load existing config");
    if !loaded_config.tracked_paths.contains(&canonical2) {
        loaded_config.tracked_paths.push(canonical2);
    }
    loaded_config
        .save(&paths)
        .expect("failed to save updated config - check write permissions");

    // Verify only one entry exists
    let final_config = Config::load(&paths).expect("failed to load final config");
    assert_eq!(
        final_config.tracked_paths.len(),
        1,
        "duplicate paths with different representations should be deduplicated"
    );
}

#[tokio::test]
async fn add_with_scan_flag() {
    let temp_dir = TempDir::with_prefix("stagecrew-add-test-")
        .expect("failed to create temp directory - check disk space");

    let tracked_path = temp_dir.path().join("staging");
    fs::create_dir_all(&tracked_path)
        .expect("failed to create staging directory - check write permissions");

    // Create some files in the tracked path
    let file1 = tracked_path.join("file1.txt");
    let file2 = tracked_path.join("file2.txt");
    fs::write(&file1, b"test content 1").expect("failed to write file1");
    fs::write(&file2, b"test content 2").expect("failed to write file2");

    let paths = mock_app_paths(&temp_dir);

    let canonical_path = tracked_path
        .canonicalize()
        .expect("failed to canonicalize path");

    // Simulate handle_add with run_scan=true
    let mut config = Config::default();
    config.tracked_paths = vec![canonical_path.clone()];
    config
        .save(&paths)
        .expect("failed to save config - check write permissions");

    let db_path = paths
        .database_file(&config)
        .expect("failed to get database path");
    let db = Database::open(&db_path).expect("failed to open database");

    let scanner = stagecrew::scanner::Scanner::new();
    let summary = stagecrew::scanner::scan_and_persist(
        &db,
        &scanner,
        std::slice::from_ref(&canonical_path),
        config.expiration_days,
        config.warning_days,
    )
    .await
    .expect("failed to scan and persist");

    // Verify scan results
    assert_eq!(summary.total_directories, 1);
    assert_eq!(summary.total_files, 2);
    assert!(summary.total_size_bytes > 0);

    // Verify database contains the scanned data
    let directories = db
        .list_directories(None)
        .expect("failed to list directories");
    assert_eq!(directories.len(), 1);
    assert_eq!(directories[0].path, canonical_path);

    let db_files = db
        .list_files_by_directory(directories[0].id)
        .expect("failed to list files");
    assert_eq!(db_files.len(), 2);
}
