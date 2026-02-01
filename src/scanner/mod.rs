//! Filesystem scanning logic.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use jwalk::WalkDir;

use crate::audit::{AuditAction, AuditService};
use crate::db::Database;
use crate::error::{Error, Result};

/// Scan tracked paths and persist results to the database.
///
/// This function orchestrates the full scan workflow:
/// 1. Scan each tracked path using the scanner
/// 2. Upsert directories and files into the database
/// 3. Update the stats table with aggregated totals
/// 4. Record the scan action in the audit log
///
/// # Errors
///
/// Returns an error if:
/// - Any path cannot be scanned (permission errors, invalid paths)
/// - Database operations fail (connection, writes)
/// - Audit logging fails
///
/// # Examples
///
/// ```no_run
/// # use stagecrew::scanner::{scan_and_persist, Scanner};
/// # use stagecrew::db::Database;
/// # use std::path::PathBuf;
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let db = Database::open(std::path::Path::new("test.db"))?;
/// let scanner = Scanner::new();
/// let paths = vec![PathBuf::from("/data/staging")];
///
/// scan_and_persist(&db, &scanner, &paths).await?;
/// # Ok::<(), stagecrew::error::Error>(())
/// # }).unwrap();
/// ```
// TODO(cleanup): Remove allow once main.rs scan command or daemon uses this function.
#[allow(dead_code)]
pub async fn scan_and_persist(
    db: &Database,
    scanner: &Scanner,
    tracked_paths: &[PathBuf],
) -> Result<ScanSummary> {
    let mut total_directories = 0u64;
    let mut total_files = 0u64;
    let mut total_size_bytes = 0u64;
    let scan_timestamp = jiff::Timestamp::now().as_second();

    // Scan each tracked path
    for path in tracked_paths {
        tracing::info!(?path, "Scanning path");

        let scan_result = scanner.scan(path).await?;

        // Upsert directories
        for dir_info in &scan_result.directories_found {
            let path_str = dir_info.path.to_string_lossy();
            let oldest_mtime_unix = dir_info.oldest_mtime.map(jiff::Timestamp::as_second);

            // Allow: size_bytes and file_count are realistic filesystem values that won't
            // exceed i64::MAX in practice. SQLite uses i64 for INTEGER columns.
            #[allow(clippy::cast_possible_wrap)]
            let dir_id = db.insert_or_update_directory(
                &path_str,
                dir_info.size_bytes as i64,
                dir_info.file_count as i64,
                oldest_mtime_unix,
                scan_timestamp,
            )?;

            // Upsert files for this directory
            // We need to re-walk to get individual file info since DirectoryInfo is aggregated
            for entry in jwalk::WalkDir::new(&dir_info.path)
                .skip_hidden(false)
                .follow_links(false)
                .into_iter()
                .filter_map(std::result::Result::ok)
            {
                let file_path = entry.path();

                // Only process files in this directory (not subdirectories)
                if file_path.parent() != Some(&dir_info.path) {
                    continue;
                }

                // Get metadata for the file
                if let Ok(metadata) = get_metadata(&file_path)
                    && metadata.is_file
                {
                    let file_path_str = file_path.to_string_lossy();
                    let mtime_unix = metadata.mtime.map_or(0, jiff::Timestamp::as_second);

                    // Allow: size_bytes is a realistic file size that won't exceed i64::MAX.
                    #[allow(clippy::cast_possible_wrap)]
                    db.insert_or_update_file(
                        dir_id,
                        &file_path_str,
                        metadata.size_bytes as i64,
                        mtime_unix,
                    )?;
                }
            }

            total_directories += 1;
        }

        total_files += scan_result.total_files;
        total_size_bytes += scan_result.total_size_bytes;
    }

    // Update stats table
    // Allow: Total counts are realistic filesystem statistics that won't exceed i64::MAX.
    #[allow(clippy::cast_possible_wrap)]
    update_stats(
        db,
        total_directories as i64,
        total_files as i64,
        total_size_bytes as i64,
        scan_timestamp,
    )?;

    // Record scan in audit log
    let audit = AuditService::new(db);
    let user = AuditService::current_user();
    audit.record(
        &user,
        AuditAction::Scan,
        None, // System-wide scan, no specific target path
        Some(&format!(
            "Scanned {} paths: {} directories, {} files, {} bytes",
            tracked_paths.len(),
            total_directories,
            total_files,
            total_size_bytes
        )),
        None,
    )?;

    tracing::info!(
        total_directories,
        total_files,
        total_size_bytes,
        "Scan complete"
    );

    Ok(ScanSummary {
        total_directories,
        total_files,
        total_size_bytes,
    })
}

/// Update the stats table with scan results.
///
/// This updates the singleton stats row (id=1) with total counts and timestamps.
/// The stats table is used by the status command for fast queries.
///
/// Note: `paths_within_warning` and `paths_pending_approval` are computed by
/// the expiration calculation logic in US-008, so we don't update them here.
fn update_stats(
    db: &Database,
    total_directories: i64,
    _total_files: i64,
    total_size_bytes: i64,
    scan_timestamp: i64,
) -> Result<()> {
    db.conn().execute(
        "UPDATE stats SET
            total_tracked_paths = ?1,
            total_size_bytes = ?2,
            last_scan_completed = ?3
         WHERE id = 1",
        (total_directories, total_size_bytes, scan_timestamp),
    )?;
    Ok(())
}

/// Summary of a scan-and-persist operation.
// TODO(cleanup): Remove allow once main.rs or TUI displays this summary.
#[allow(dead_code)]
#[derive(Debug, Clone)]
// Allow: The `total_` prefix provides clarity that these are aggregate counts
// across the entire scan operation, not per-directory values.
#[allow(clippy::struct_field_names)]
#[must_use = "scan summary should be logged or displayed"]
#[non_exhaustive]
pub struct ScanSummary {
    pub total_directories: u64,
    pub total_files: u64,
    pub total_size_bytes: u64,
}

/// Scanner for walking filesystem trees and collecting metadata.
///
/// The scanner uses jwalk for parallel filesystem traversal, collecting file metadata
/// including size and modification time. Symlinks are resolved to track the actual
/// file's mtime, and permission errors are handled gracefully with warnings.
// TODO(cleanup): Remove allow once database integration uses this struct.
#[allow(dead_code)]
pub struct Scanner {
    // Configuration will be added here
}

impl Scanner {
    /// Create a new scanner.
    pub fn new() -> Self {
        Self {}
    }

    /// Scan a directory tree and return file metadata.
    ///
    /// This method walks the directory tree rooted at `root` using jwalk for parallel
    /// traversal. It collects file sizes and modification times, resolving symlinks
    /// to track the actual file's mtime rather than the symlink's mtime.
    ///
    /// The scan runs in a blocking task via `tokio::task::spawn_blocking` to avoid
    /// blocking the async runtime, as filesystem operations are inherently blocking.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The root path does not exist or is not a directory
    /// - A critical I/O error occurs during traversal (permission errors on individual
    ///   files are logged but do not fail the scan)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use stagecrew::scanner::Scanner;
    /// # use std::path::Path;
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let scanner = Scanner::new();
    /// let result = scanner.scan(Path::new("/data/staging")).await?;
    /// println!("Scanned {} files, {} bytes", result.total_files, result.total_size_bytes);
    /// # Ok::<(), stagecrew::error::Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn scan(&self, root: &Path) -> Result<ScanResult> {
        // Validate root path exists and is a directory
        if !root.exists() {
            return Err(Error::PathNotFound(root.to_path_buf()));
        }
        if !root.is_dir() {
            return Err(Error::Filesystem {
                path: root.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotADirectory,
                    "path is not a directory",
                ),
            });
        }

        let root = root.to_path_buf();

        // Run the blocking scan in a separate thread pool
        tokio::task::spawn_blocking(move || scan_directory_tree(&root))
            .await
            .map_err(|e| Error::Config(format!("Scan task panicked: {e}")))
    }
}

impl Default for Scanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Perform the actual directory tree scan.
///
/// This function walks the directory tree using jwalk, collecting file metadata
/// and aggregating results by directory.
fn scan_directory_tree(root: &Path) -> ScanResult {
    let mut total_files = 0u64;
    let mut total_size_bytes = 0u64;
    let mut dir_map: HashMap<PathBuf, DirectoryAggregator> = HashMap::new();

    // Walk the tree in parallel
    for entry in WalkDir::new(root)
        .skip_hidden(false)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| match e {
            Ok(entry) => Some(entry),
            Err(e) => {
                // Log permission errors and continue scanning
                tracing::warn!("Skipping entry due to error: {e}");
                None
            }
        })
    {
        let path = entry.path();

        // Get metadata for the entry
        let metadata = match get_metadata(&path) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("Failed to get metadata for {}: {e}", path.display());
                continue;
            }
        };

        // Only process files (not directories)
        if !metadata.is_file {
            continue;
        }

        total_files += 1;
        total_size_bytes += metadata.size_bytes;

        // Determine the parent directory for aggregation
        let parent_dir = path
            .parent()
            .map_or_else(|| root.to_path_buf(), std::path::Path::to_path_buf);

        // Aggregate into parent directory
        let aggregator = dir_map
            .entry(parent_dir.clone())
            .or_insert_with(|| DirectoryAggregator {
                path: parent_dir,
                size_bytes: 0,
                file_count: 0,
                oldest_mtime: None,
            });

        aggregator.size_bytes += metadata.size_bytes;
        aggregator.file_count += 1;

        // Track oldest mtime
        if let Some(mtime) = metadata.mtime {
            aggregator.oldest_mtime = Some(match aggregator.oldest_mtime {
                Some(existing) if mtime < existing => mtime,
                Some(existing) => existing,
                None => mtime,
            });
        }
    }

    // Convert aggregators to DirectoryInfo
    let directories_found = dir_map
        .into_values()
        .map(|agg| DirectoryInfo {
            path: agg.path,
            size_bytes: agg.size_bytes,
            file_count: agg.file_count,
            oldest_mtime: agg.oldest_mtime,
        })
        .collect();

    ScanResult {
        total_files,
        total_size_bytes,
        directories_found,
    }
}

/// File metadata collected during scan.
struct FileMetadata {
    size_bytes: u64,
    mtime: Option<jiff::Timestamp>,
    is_file: bool,
}

/// Get metadata for a file, resolving symlinks.
///
/// If the path is a symlink, this resolves it and returns the target's metadata.
/// For broken symlinks, a warning is logged and an error is returned.
fn get_metadata(path: &Path) -> Result<FileMetadata> {
    // Get the metadata, resolving symlinks
    // This uses fs::metadata which follows symlinks automatically
    let metadata = fs::metadata(path)?;

    let is_file = metadata.is_file();
    let size_bytes = metadata.len();

    // Get modification time
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|systime| jiff::Timestamp::try_from(systime).ok());

    Ok(FileMetadata {
        size_bytes,
        mtime,
        is_file,
    })
}

/// Intermediate structure for aggregating directory statistics.
struct DirectoryAggregator {
    path: PathBuf,
    size_bytes: u64,
    file_count: u64,
    oldest_mtime: Option<jiff::Timestamp>,
}

/// Result of a filesystem scan.
///
/// Contains aggregated statistics about the scanned tree, including total file counts,
/// total size, and per-directory information.
// TODO(cleanup): Remove allow once database integration reads these fields.
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
#[must_use = "scan results should be processed"]
#[non_exhaustive]
pub struct ScanResult {
    pub total_files: u64,
    pub total_size_bytes: u64,
    pub directories_found: Vec<DirectoryInfo>,
}

/// Information about a scanned directory.
///
/// Represents aggregated metadata for all files within a directory. The `oldest_mtime`
/// field tracks the oldest modification time of any file in the directory, which is used
/// for expiration calculations.
// TODO(cleanup): Remove allow once database integration reads these fields.
#[allow(dead_code)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DirectoryInfo {
    pub path: PathBuf,
    pub size_bytes: u64,
    pub file_count: u64,
    pub oldest_mtime: Option<jiff::Timestamp>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    /// Helper to create a temporary directory with test files.
    fn create_test_tree() -> TempDir {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create a simple directory structure:
        // root/
        //   file1.txt (100 bytes)
        //   subdir/
        //     file2.txt (200 bytes)
        //     file3.txt (300 bytes)

        let mut file1 = File::create(root.join("file1.txt")).unwrap();
        file1.write_all(&[0u8; 100]).unwrap();
        file1.sync_all().unwrap();

        let subdir = root.join("subdir");
        fs::create_dir(&subdir).unwrap();

        let mut file2 = File::create(subdir.join("file2.txt")).unwrap();
        file2.write_all(&[0u8; 200]).unwrap();
        file2.sync_all().unwrap();

        let mut file3 = File::create(subdir.join("file3.txt")).unwrap();
        file3.write_all(&[0u8; 300]).unwrap();
        file3.sync_all().unwrap();

        temp_dir
    }

    #[tokio::test]
    async fn scanner_finds_all_files() {
        let temp_dir = create_test_tree();
        let root = temp_dir.path();

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.unwrap();

        assert_eq!(result.total_files, 3, "Expected 3 files to be found");
        assert_eq!(
            result.total_size_bytes, 600,
            "Expected total size of 600 bytes"
        );
    }

    #[tokio::test]
    async fn scanner_aggregates_by_directory() {
        let temp_dir = create_test_tree();
        let root = temp_dir.path();

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.unwrap();

        assert_eq!(
            result.directories_found.len(),
            2,
            "Expected 2 directories (root and subdir)"
        );

        // Find the root directory aggregation
        let root_agg = result
            .directories_found
            .iter()
            .find(|d| d.path == root)
            .expect("Root directory should be in results");

        assert_eq!(root_agg.file_count, 1, "Root should have 1 file");
        assert_eq!(root_agg.size_bytes, 100, "Root file should be 100 bytes");

        // Find the subdir aggregation
        let subdir = root.join("subdir");
        let subdir_agg = result
            .directories_found
            .iter()
            .find(|d| d.path == subdir)
            .expect("Subdir should be in results");

        assert_eq!(subdir_agg.file_count, 2, "Subdir should have 2 files");
        assert_eq!(
            subdir_agg.size_bytes, 500,
            "Subdir files should total 500 bytes"
        );
    }

    #[tokio::test]
    async fn scanner_tracks_oldest_mtime() {
        let temp_dir = create_test_tree();
        let root = temp_dir.path();

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.unwrap();

        // All directories should have an oldest_mtime
        for dir_info in &result.directories_found {
            assert!(
                dir_info.oldest_mtime.is_some(),
                "Directory {} should have oldest_mtime",
                dir_info.path.display()
            );
        }
    }

    #[tokio::test]
    async fn scanner_fails_on_nonexistent_path() {
        let scanner = Scanner::new();
        let result = scanner
            .scan(Path::new("/nonexistent/path/that/does/not/exist"))
            .await;

        assert!(result.is_err(), "Expected error for nonexistent path");
        match result.unwrap_err() {
            Error::PathNotFound(_) => {}
            e => panic!("Expected PathNotFound error, got: {e:?}"),
        }
    }

    #[tokio::test]
    async fn scanner_fails_on_file_path() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("file.txt");
        File::create(&file_path).unwrap();

        let scanner = Scanner::new();
        let result = scanner.scan(&file_path).await;

        assert!(result.is_err(), "Expected error when scanning a file");
        match result.unwrap_err() {
            Error::Filesystem { .. } => {}
            e => panic!("Expected Filesystem error about directory, got: {e:?}"),
        }
    }

    #[tokio::test]
    async fn scanner_handles_empty_directory() {
        let temp_dir = TempDir::new().unwrap();

        let scanner = Scanner::new();
        let result = scanner.scan(temp_dir.path()).await.unwrap();

        assert_eq!(result.total_files, 0, "Expected no files");
        assert_eq!(result.total_size_bytes, 0, "Expected zero size");
        assert_eq!(
            result.directories_found.len(),
            0,
            "Expected no directory aggregations"
        );
    }

    #[tokio::test]
    async fn scanner_resolves_symlinks() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create a file
        let target_file = root.join("target.txt");
        let mut file = File::create(&target_file).unwrap();
        file.write_all(&[0u8; 150]).unwrap();
        file.sync_all().unwrap();

        // Create a symlink to the file
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            symlink(&target_file, root.join("link.txt")).unwrap();
        }

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.unwrap();

        // On Unix, we should see both the file and the symlink as files
        // (fs::metadata resolves symlinks)
        #[cfg(unix)]
        {
            assert_eq!(result.total_files, 2, "Expected 2 files (target + link)");
            assert_eq!(result.total_size_bytes, 300, "Expected 300 bytes (150 * 2)");
        }

        #[cfg(not(unix))]
        {
            // On non-Unix, just verify the target file
            assert_eq!(result.total_files, 1);
            assert_eq!(result.total_size_bytes, 150);
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn scanner_skips_broken_symlinks_gracefully() {
        use std::os::unix::fs::symlink;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create a valid file
        let mut file = File::create(root.join("valid.txt")).unwrap();
        file.write_all(&[0u8; 100]).unwrap();
        file.sync_all().unwrap();

        // Create a broken symlink pointing to a non-existent target
        symlink("/nonexistent/target", root.join("broken_link")).unwrap();

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.unwrap();

        // Scan should succeed, only counting the valid file
        // The broken symlink will cause a metadata error which is logged and skipped
        assert_eq!(
            result.total_files, 1,
            "Broken symlink should not be counted"
        );
        assert_eq!(result.total_size_bytes, 100);
    }

    #[tokio::test]
    async fn scanner_correctly_identifies_oldest_mtime() {
        use filetime::{FileTime, set_file_mtime};

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create two files
        let file1 = root.join("old.txt");
        let file2 = root.join("new.txt");
        File::create(&file1).unwrap().write_all(&[0u8; 10]).unwrap();
        File::create(&file2).unwrap().write_all(&[0u8; 10]).unwrap();

        // Set file1 to an older time (2001-09-09)
        let old_time = FileTime::from_unix_time(1_000_000_000, 0);
        set_file_mtime(&file1, old_time).unwrap();

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.unwrap();

        let root_dir = result
            .directories_found
            .iter()
            .find(|d| d.path == root)
            .expect("Root directory should be in results");

        let oldest = root_dir
            .oldest_mtime
            .expect("Root directory should have oldest_mtime");

        // Verify it's the older file's mtime
        // We can't compare exact timestamps due to precision differences,
        // but we can verify it's close to the old timestamp
        let expected = jiff::Timestamp::from_second(1_000_000_000).unwrap();

        // Should be within 1 second (accounting for filesystem timestamp precision)
        let diff_seconds = (oldest.as_second() - expected.as_second()).abs();
        assert!(
            diff_seconds <= 1,
            "oldest_mtime should be close to the old file's mtime (expected ~{expected}, got {oldest}, diff={diff_seconds}s)"
        );
    }

    #[tokio::test]
    async fn scanner_includes_hidden_files() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create a hidden file (starts with dot)
        let mut hidden = File::create(root.join(".hidden")).unwrap();
        hidden.write_all(&[0u8; 50]).unwrap();
        hidden.sync_all().unwrap();

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.unwrap();

        assert_eq!(result.total_files, 1, "Hidden file should be counted");
        assert_eq!(result.total_size_bytes, 50);
    }

    // === Integration Tests ===

    #[tokio::test]
    async fn scan_and_persist_creates_directory_records() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create test directory structure
        let project_dir = root.join("project");
        fs::create_dir(&project_dir).unwrap();
        let mut file1 = File::create(project_dir.join("file1.txt")).unwrap();
        file1.write_all(&[0u8; 100]).unwrap();
        file1.sync_all().unwrap();

        // Create database
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();

        // Run scan_and_persist
        let scanner = Scanner::new();
        let summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir))
            .await
            .unwrap();

        // Verify summary
        assert_eq!(summary.total_directories, 1);
        assert_eq!(summary.total_files, 1);
        assert_eq!(summary.total_size_bytes, 100);

        // Verify directory was persisted
        let dir = db
            .get_directory_by_path(&project_dir.to_string_lossy())
            .unwrap()
            .expect("directory should exist in database");

        assert_eq!(dir.path, project_dir.to_string_lossy());
        assert_eq!(dir.size_bytes, 100);
        assert_eq!(dir.file_count, 1);
        assert!(dir.oldest_mtime.is_some());
        assert!(dir.last_scanned.is_some());
        assert_eq!(dir.status, "tracked");
    }

    #[tokio::test]
    async fn scan_and_persist_creates_file_records() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create test files
        let project_dir = root.join("project");
        fs::create_dir(&project_dir).unwrap();
        File::create(project_dir.join("a.txt"))
            .unwrap()
            .write_all(&[0u8; 10])
            .unwrap();
        File::create(project_dir.join("b.txt"))
            .unwrap()
            .write_all(&[0u8; 20])
            .unwrap();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir))
            .await
            .unwrap();

        // Get directory id
        let dir = db
            .get_directory_by_path(&project_dir.to_string_lossy())
            .unwrap()
            .expect("directory should exist");

        // Verify files were persisted
        let files = db.list_files_by_directory(dir.id).unwrap();
        assert_eq!(files.len(), 2, "Expected 2 files");

        // Files should be ordered by path
        assert!(files[0].path.ends_with("a.txt"));
        assert_eq!(files[0].size_bytes, 10);
        assert!(files[1].path.ends_with("b.txt"));
        assert_eq!(files[1].size_bytes, 20);
    }

    #[tokio::test]
    async fn scan_and_persist_updates_stats_table() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create test structure
        let project_dir = root.join("project");
        fs::create_dir(&project_dir).unwrap();
        File::create(project_dir.join("file.txt"))
            .unwrap()
            .write_all(&[0u8; 500])
            .unwrap();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[project_dir])
            .await
            .unwrap();

        // Verify stats were updated
        let stats: (i64, i64, Option<i64>) = db
            .conn()
            .query_row(
                "SELECT total_tracked_paths, total_size_bytes, last_scan_completed
                 FROM stats WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(stats.0, 1, "Expected 1 tracked directory");
        assert_eq!(stats.1, 500, "Expected 500 bytes total");
        assert!(stats.2.is_some(), "Expected last_scan_completed to be set");
    }

    #[tokio::test]
    async fn scan_and_persist_records_audit_entry() {
        use crate::audit::AuditService;
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create test structure
        let project_dir = root.join("project");
        fs::create_dir(&project_dir).unwrap();
        File::create(project_dir.join("file.txt"))
            .unwrap()
            .write_all(&[0u8; 100])
            .unwrap();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[project_dir])
            .await
            .unwrap();

        // Verify audit entry was created
        let audit = AuditService::new(&db);
        let entries = audit.list_recent(10).unwrap();

        assert_eq!(entries.len(), 1, "Expected 1 audit entry");
        assert_eq!(entries[0].action, "scan");
        assert!(entries[0].details.is_some());
        assert!(
            entries[0]
                .details
                .as_ref()
                .unwrap()
                .contains("1 directories"),
            "Expected details to mention directories"
        );
    }

    #[tokio::test]
    async fn scan_and_persist_handles_multiple_paths() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create two separate directories
        let dir1 = root.join("project1");
        let dir2 = root.join("project2");
        fs::create_dir(&dir1).unwrap();
        fs::create_dir(&dir2).unwrap();

        File::create(dir1.join("file1.txt"))
            .unwrap()
            .write_all(&[0u8; 100])
            .unwrap();
        File::create(dir2.join("file2.txt"))
            .unwrap()
            .write_all(&[0u8; 200])
            .unwrap();

        // Create database and scan both paths
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let summary = scan_and_persist(&db, &scanner, &[dir1.clone(), dir2.clone()])
            .await
            .unwrap();

        // Verify both directories were scanned
        assert_eq!(summary.total_directories, 2);
        assert_eq!(summary.total_files, 2);
        assert_eq!(summary.total_size_bytes, 300);

        // Verify both are in database
        assert!(
            db.get_directory_by_path(&dir1.to_string_lossy())
                .unwrap()
                .is_some()
        );
        assert!(
            db.get_directory_by_path(&dir2.to_string_lossy())
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn scan_and_persist_upserts_existing_directories() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create test directory
        let project_dir = root.join("project");
        fs::create_dir(&project_dir).unwrap();
        File::create(project_dir.join("file1.txt"))
            .unwrap()
            .write_all(&[0u8; 100])
            .unwrap();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir))
            .await
            .unwrap();

        // Change directory status manually (simulating user action)
        let dir = db
            .get_directory_by_path(&project_dir.to_string_lossy())
            .unwrap()
            .unwrap();
        db.update_directory_status(dir.id, "approved").unwrap();

        // Add a new file
        File::create(project_dir.join("file2.txt"))
            .unwrap()
            .write_all(&[0u8; 50])
            .unwrap();

        // Scan again
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir))
            .await
            .unwrap();

        // Verify directory was updated but status preserved
        let updated_dir = db
            .get_directory_by_path(&project_dir.to_string_lossy())
            .unwrap()
            .unwrap();

        assert_eq!(updated_dir.id, dir.id, "ID should not change");
        assert_eq!(updated_dir.status, "approved", "Status should be preserved");
        assert_eq!(updated_dir.file_count, 2, "File count should be updated");
        assert_eq!(
            updated_dir.size_bytes, 150,
            "Size should reflect both files"
        );
    }
}
