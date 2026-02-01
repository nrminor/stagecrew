//! Filesystem scanning logic.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use jwalk::WalkDir;

use crate::audit::{AuditAction, AuditService};
use crate::db::Database;
use crate::error::{Error, Result};

/// Seconds in a 24-hour day (not calendar-aware).
const SECS_PER_DAY: i64 = 86400;

/// Calculate days remaining until expiration based on oldest modification time.
///
/// Calculates the number of days remaining until a path expires, based on the
/// oldest file's modification time and the configured expiration period. Returns
/// a negative value if the path is already expired.
///
/// # Arguments
///
/// * `oldest_mtime` - Unix timestamp of the oldest file in the directory
/// * `expiration_days` - Number of days until expiration
///
/// # Returns
///
/// Days remaining until expiration (can be negative if already expired)
///
/// # Examples
///
/// ```no_run
/// # use jiff::Timestamp;
/// // File modified 30 days ago, expires in 90 days
/// const SECS_PER_DAY: i64 = 86400;
/// let now = Timestamp::now();
/// let thirty_days_ago = now.checked_sub(jiff::SignedDuration::from_secs(30 * SECS_PER_DAY)).unwrap();
/// // In real code: let days_remaining = calculate_expiration(thirty_days_ago.as_second(), 90);
/// // assert!(days_remaining > 59 && days_remaining <= 60);
/// ```
// TODO(cleanup): Remove allow once daemon or TUI uses this function.
#[allow(dead_code)]
#[must_use = "expiration calculation result should be used"]
pub fn calculate_expiration(oldest_mtime: i64, expiration_days: u32) -> i64 {
    let now = jiff::Timestamp::now();
    let oldest = jiff::Timestamp::from_second(oldest_mtime).unwrap_or(now);

    // Calculate expiration timestamp (days as 24-hour periods)
    let expiration_secs = i64::from(expiration_days) * SECS_PER_DAY;
    let expiration_duration = jiff::SignedDuration::from_secs(expiration_secs);
    let expires_at = oldest.checked_add(expiration_duration).unwrap_or(now);

    // Calculate days remaining (using 86400-second days)
    let duration_remaining = expires_at.duration_since(now);
    duration_remaining.as_secs() / SECS_PER_DAY
}

/// Transition directories based on expiration and deferral status.
///
/// This function implements the core business logic for the removal-by-default
/// policy. It processes directories in the database and transitions them between
/// states based on their expiration status:
///
/// - **Tracked paths**: If expired, transition to `pending` (or `approved` if `auto_remove` is enabled)
/// - **Deferred paths**: If the deferral period has ended, reset status to `tracked` and clear `deferred_until`
/// - **Ignored paths**: Never transitioned (permanent exemption)
///
/// This function is typically called after a scan to update the workflow state.
///
/// # Arguments
///
/// * `db` - Database connection
/// * `expiration_days` - Number of days until expiration
/// * `auto_remove` - If true, expired paths go to `approved` instead of `pending`
///
/// # Returns
///
/// A `TransitionSummary` containing counts of transitions performed.
///
/// # Errors
///
/// Returns an error if database operations fail or if audit logging fails.
///
/// # Examples
///
/// ```no_run
/// # use std::path::Path;
/// // In real code:
/// // let db = Database::open(Path::new("test.db"))?;
/// // let summary = transition_expired_paths(&db, 90, false)?;
/// // println!("Transitioned {} to pending, {} reset from deferred",
/// //          summary.expired_to_pending, summary.deferred_reset);
/// ```
// TODO(cleanup): Remove allow once daemon uses this function.
#[allow(dead_code)]
#[must_use = "transition summary should be logged or displayed"]
pub fn transition_expired_paths(
    db: &Database,
    expiration_days: u32,
    auto_remove: bool,
) -> Result<TransitionSummary> {
    let mut expired_to_pending = 0u64;
    let mut expired_to_approved = 0u64;
    let mut deferred_reset = 0u64;

    let now = jiff::Timestamp::now().as_second();

    // Get all directories with status 'tracked' or 'deferred'
    let conn = db.conn();
    let mut stmt = conn.prepare(
        "SELECT id, path, oldest_mtime, status, deferred_until
         FROM directories
         WHERE status IN ('tracked', 'deferred')",
    )?;

    let mut rows = stmt.query([])?;
    let mut transitions = Vec::new();

    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let path: String = row.get(1)?;
        let oldest_mtime: Option<i64> = row.get(2)?;
        let status: String = row.get(3)?;
        let deferred_until: Option<i64> = row.get(4)?;

        // Handle deferred paths
        if status == "deferred"
            && let Some(deferred_until_ts) = deferred_until
            && now >= deferred_until_ts
        {
            // Deferral period ended, reset to tracked
            transitions.push((id, path, "tracked".to_string(), true));
            deferred_reset += 1;
            continue;
        }

        // Handle tracked paths (check expiration)
        if status == "tracked"
            && let Some(oldest_mtime_ts) = oldest_mtime
        {
            let days_remaining = calculate_expiration(oldest_mtime_ts, expiration_days);

            if days_remaining <= 0 {
                // Path has expired
                let new_status = if auto_remove { "approved" } else { "pending" };
                transitions.push((id, path, new_status.to_string(), false));

                if auto_remove {
                    expired_to_approved += 1;
                } else {
                    expired_to_pending += 1;
                }
            }
        }
    }

    // Drop stmt and rows to release the borrow on conn
    drop(rows);
    drop(stmt);

    // Apply transitions
    let audit = AuditService::new(db);
    let user = AuditService::current_user();

    for (id, path, new_status, is_deferral_reset) in transitions {
        // Update status
        if is_deferral_reset {
            // Clear deferred_until when resetting to tracked
            conn.execute(
                "UPDATE directories SET status = ?1, deferred_until = NULL, updated_at = strftime('%s', 'now') WHERE id = ?2",
                (&new_status, id),
            )?;
        } else {
            db.update_directory_status(id, &new_status)?;
        }

        // Record audit entry
        let action_desc = if is_deferral_reset {
            "Deferral period ended, reset to tracked"
        } else if new_status == "approved" {
            "Expired and auto-approved for removal"
        } else {
            "Expired, pending approval for removal"
        };

        audit.record(
            &user,
            AuditAction::Scan,
            Some(&path),
            Some(action_desc),
            Some(id),
        )?;
    }

    Ok(TransitionSummary {
        expired_to_pending,
        expired_to_approved,
        deferred_reset,
    })
}

/// Summary of state transitions performed.
///
/// This struct is marked `#[non_exhaustive]`; new fields may be added in
/// minor versions. Use `..` when destructuring to remain forward-compatible.
// TODO(cleanup): Remove allow once daemon or TUI displays this summary.
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
#[must_use = "transition summary should be logged or displayed"]
#[non_exhaustive]
pub struct TransitionSummary {
    /// Number of tracked paths transitioned to pending status.
    pub expired_to_pending: u64,
    /// Number of tracked paths transitioned to approved status (auto-remove).
    pub expired_to_approved: u64,
    /// Number of deferred paths reset to tracked status.
    pub deferred_reset: u64,
}

/// Scan tracked paths and persist results to the database.
///
/// This function orchestrates the full scan workflow:
/// 1. Scan each tracked path using the scanner
/// 2. Upsert directories and files into the database
/// 3. Update the stats table with aggregated totals and expiration counts
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
/// # use stagecrew::config::Config;
/// # use std::path::PathBuf;
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let db = Database::open(std::path::Path::new("test.db"))?;
/// let scanner = Scanner::new();
/// let config = Config::default();
/// let paths = vec![PathBuf::from("/data/staging")];
///
/// scan_and_persist(&db, &scanner, &paths, config.expiration_days, config.warning_days).await?;
/// # Ok::<(), stagecrew::error::Error>(())
/// # }).unwrap();
/// ```
pub async fn scan_and_persist(
    db: &Database,
    scanner: &Scanner,
    tracked_paths: &[PathBuf],
    expiration_days: u32,
    warning_days: u32,
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
        total_size_bytes as i64,
        scan_timestamp,
        expiration_days,
        warning_days,
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
/// This updates the singleton stats row (id=1) with total counts, warning counts,
/// and timestamps. The stats table is used by the status command for fast queries.
///
/// This function calculates:
/// - `paths_within_warning`: directories with `days_remaining` <= `warning_days` AND > 0 AND status = 'tracked'
/// - `paths_pending_approval`: directories with status = 'pending'
/// - `paths_overdue`: directories with `days_remaining` <= 0 AND status = 'tracked'
///
/// All calculations are performed in a single SQL UPDATE statement using subqueries
/// for efficiency.
///
/// # Arguments
///
/// * `db` - Database connection
/// * `total_directories` - Total number of tracked directories
/// * `total_size_bytes` - Total size in bytes across all tracked directories
/// * `scan_timestamp` - Unix timestamp when the scan completed
/// * `expiration_days` - Number of days until expiration (from config)
/// * `warning_days` - Number of days before expiration to start warning (from config)
///
/// # Errors
///
/// Returns an error if the database UPDATE fails.
fn update_stats(
    db: &Database,
    total_directories: i64,
    total_size_bytes: i64,
    scan_timestamp: i64,
    expiration_days: u32,
    warning_days: u32,
) -> Result<()> {
    let now = jiff::Timestamp::now().as_second();

    let expiration_days_i64 = i64::from(expiration_days);
    let warning_days_i64 = i64::from(warning_days);

    // Calculate paths_within_warning, paths_pending_approval, and paths_overdue
    // using a single UPDATE with subqueries for efficiency.
    //
    // days_remaining calculation:
    //   (oldest_mtime + (expiration_days * 86400) - now) / 86400
    //
    // paths_within_warning: 0 < days_remaining <= warning_days AND status = 'tracked'
    // paths_pending_approval: status = 'pending'
    // paths_overdue: days_remaining <= 0 AND status = 'tracked'
    db.conn().execute(
        "UPDATE stats SET
            total_tracked_paths = ?1,
            total_size_bytes = ?2,
            last_scan_completed = ?3,
            paths_within_warning = (
                SELECT COUNT(*)
                FROM directories
                WHERE oldest_mtime IS NOT NULL
                  AND ((oldest_mtime + (?4 * 86400) - ?5) / 86400) <= ?6
                  AND ((oldest_mtime + (?4 * 86400) - ?5) / 86400) > 0
                  AND status = 'tracked'
            ),
            paths_pending_approval = (
                SELECT COUNT(*)
                FROM directories
                WHERE status = 'pending'
            ),
            paths_overdue = (
                SELECT COUNT(*)
                FROM directories
                WHERE oldest_mtime IS NOT NULL
                  AND ((oldest_mtime + (?4 * 86400) - ?5) / 86400) <= 0
                  AND status = 'tracked'
            )
         WHERE id = 1",
        (
            total_directories,
            total_size_bytes,
            scan_timestamp,
            expiration_days_i64,
            now,
            warning_days_i64,
        ),
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
    #[must_use]
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
    use filetime::{FileTime, set_file_mtime};
    use std::fs::File;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    /// Creates a temporary database for testing.
    fn temp_database() -> (NamedTempFile, crate::db::Database) {
        let temp_file = NamedTempFile::new().expect("failed to create temp file");
        let db = crate::db::Database::open(temp_file.path()).expect("failed to open database");
        (temp_file, db)
    }

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

    // === Expiration Calculation Tests ===

    #[test]
    fn calculate_expiration_returns_positive_for_recent_files() {
        let now = jiff::Timestamp::now();
        // File modified 10 days ago
        let ten_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(10 * SECS_PER_DAY))
            .unwrap();

        let days_remaining = super::calculate_expiration(ten_days_ago.as_second(), 90);

        // Should have ~80 days remaining (90 - 10)
        assert!(
            (79..=80).contains(&days_remaining),
            "Expected ~80 days remaining, got {days_remaining}"
        );
    }

    #[test]
    fn calculate_expiration_returns_negative_for_expired_files() {
        let now = jiff::Timestamp::now();
        // File modified 100 days ago (expired for 90-day policy)
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .unwrap();

        let days_remaining = super::calculate_expiration(hundred_days_ago.as_second(), 90);

        // Should be negative (expired by ~10 days)
        assert!(
            (-11..=-9).contains(&days_remaining),
            "Expected ~-10 days remaining, got {days_remaining}"
        );
    }

    #[test]
    fn calculate_expiration_returns_zero_on_expiration_day() {
        let now = jiff::Timestamp::now();
        // File modified exactly 90 days ago
        let ninety_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(90 * SECS_PER_DAY))
            .unwrap();

        let days_remaining = super::calculate_expiration(ninety_days_ago.as_second(), 90);

        // Should be at or very close to 0
        assert!(
            (-1..=0).contains(&days_remaining),
            "Expected 0 days remaining, got {days_remaining}"
        );
    }

    #[test]
    fn calculate_expiration_handles_custom_expiration_period() {
        let now = jiff::Timestamp::now();
        // File modified 20 days ago
        let twenty_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(20 * SECS_PER_DAY))
            .unwrap();

        // 30-day expiration policy
        let days_remaining = super::calculate_expiration(twenty_days_ago.as_second(), 30);

        // Should have ~10 days remaining (30 - 20)
        assert!(
            (9..=10).contains(&days_remaining),
            "Expected ~10 days remaining, got {days_remaining}"
        );
    }

    // === State Transition Tests ===

    #[test]
    fn transition_expired_paths_moves_expired_tracked_to_pending() {
        let (_temp, db) = temp_database();

        // Insert a directory with an old mtime (100 days ago)
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .unwrap();

        let _id = db
            .insert_or_update_directory(
                "/data/expired",
                1024,
                5,
                Some(hundred_days_ago.as_second()),
                now.as_second(),
            )
            .expect("insert directory");

        // Verify initial status is 'tracked'
        let dir_before = db
            .get_directory_by_path("/data/expired")
            .expect("query")
            .expect("exists");
        assert_eq!(dir_before.status, "tracked");

        // Run transition with 90-day expiration policy and auto_remove=false
        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        assert_eq!(summary.expired_to_pending, 1);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status changed to 'pending'
        let dir_after = db
            .get_directory_by_path("/data/expired")
            .expect("query")
            .expect("exists");
        assert_eq!(dir_after.status, "pending");

        // Verify audit entry was created
        let audit = crate::audit::AuditService::new(&db);
        let entries = audit.list_by_path("/data/expired").expect("query audit");
        assert_eq!(entries.len(), 1);
        assert!(
            entries[0]
                .details
                .as_ref()
                .unwrap()
                .contains("pending approval")
        );
    }

    #[test]
    fn transition_expired_paths_moves_expired_tracked_to_approved_with_auto_remove() {
        let (_temp, db) = temp_database();

        // Insert a directory with an old mtime
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .unwrap();

        db.insert_or_update_directory(
            "/data/expired",
            1024,
            5,
            Some(hundred_days_ago.as_second()),
            now.as_second(),
        )
        .expect("insert directory");

        // Run transition with auto_remove=true
        let summary = super::transition_expired_paths(&db, 90, true).expect("transition");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 1);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status changed to 'approved'
        let dir = db
            .get_directory_by_path("/data/expired")
            .expect("query")
            .expect("exists");
        assert_eq!(dir.status, "approved");
    }

    #[test]
    fn transition_expired_paths_does_not_transition_non_expired() {
        let (_temp, db) = temp_database();

        // Insert a directory with recent mtime (10 days ago)
        let now = jiff::Timestamp::now();
        let ten_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(10 * SECS_PER_DAY))
            .unwrap();

        db.insert_or_update_directory(
            "/data/recent",
            1024,
            5,
            Some(ten_days_ago.as_second()),
            now.as_second(),
        )
        .expect("insert directory");

        // Run transition with 90-day policy
        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let dir = db
            .get_directory_by_path("/data/recent")
            .expect("query")
            .expect("exists");
        assert_eq!(dir.status, "tracked");
    }

    #[test]
    fn transition_expired_paths_resets_expired_deferral() {
        let (_temp, db) = temp_database();

        let now = jiff::Timestamp::now();
        let yesterday = now
            .checked_sub(jiff::SignedDuration::from_secs(SECS_PER_DAY))
            .unwrap();

        // Insert a directory with deferred status
        let id = db
            .insert_or_update_directory("/data/deferred", 1024, 5, None, now.as_second())
            .expect("insert directory");

        // Set status to 'deferred' with deferred_until in the past
        db.conn()
            .execute(
                "UPDATE directories SET status = 'deferred', deferred_until = ?1 WHERE id = ?2",
                (yesterday.as_second(), id),
            )
            .expect("set deferred status");

        // Verify initial state
        let dir_before = db
            .get_directory_by_path("/data/deferred")
            .expect("query")
            .expect("exists");
        assert_eq!(dir_before.status, "deferred");
        assert!(dir_before.deferred_until.is_some());

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 1);

        // Verify status reset to 'tracked' and deferred_until cleared
        let dir_after = db
            .get_directory_by_path("/data/deferred")
            .expect("query")
            .expect("exists");
        assert_eq!(dir_after.status, "tracked");
        assert_eq!(dir_after.deferred_until, None);
    }

    #[test]
    fn transition_expired_paths_does_not_reset_active_deferral() {
        let (_temp, db) = temp_database();

        let now = jiff::Timestamp::now();
        let next_week = now
            .checked_add(jiff::SignedDuration::from_secs(7 * SECS_PER_DAY))
            .unwrap();

        // Insert a directory with deferred status
        let id = db
            .insert_or_update_directory("/data/deferred", 1024, 5, None, now.as_second())
            .expect("insert directory");

        // Set status to 'deferred' with deferred_until in the future
        db.conn()
            .execute(
                "UPDATE directories SET status = 'deferred', deferred_until = ?1 WHERE id = ?2",
                (next_week.as_second(), id),
            )
            .expect("set deferred status");

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let dir = db
            .get_directory_by_path("/data/deferred")
            .expect("query")
            .expect("exists");
        assert_eq!(dir.status, "deferred");
        assert_eq!(dir.deferred_until, Some(next_week.as_second()));
    }

    #[test]
    fn transition_expired_paths_ignores_ignored_status() {
        let (_temp, db) = temp_database();

        // Insert a directory with old mtime but 'ignored' status
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .unwrap();

        let id = db
            .insert_or_update_directory(
                "/data/ignored",
                1024,
                5,
                Some(hundred_days_ago.as_second()),
                now.as_second(),
            )
            .expect("insert directory");

        // Set status to 'ignored'
        db.update_directory_status(id, "ignored")
            .expect("update status");

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let dir = db
            .get_directory_by_path("/data/ignored")
            .expect("query")
            .expect("exists");
        assert_eq!(dir.status, "ignored");
    }

    #[test]
    fn transition_expired_paths_handles_multiple_directories() {
        let (_temp, db) = temp_database();

        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .unwrap();
        let ten_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(10 * SECS_PER_DAY))
            .unwrap();
        let yesterday = now
            .checked_sub(jiff::SignedDuration::from_secs(SECS_PER_DAY))
            .unwrap();

        // Expired tracked directory
        db.insert_or_update_directory(
            "/data/expired1",
            1024,
            5,
            Some(hundred_days_ago.as_second()),
            now.as_second(),
        )
        .expect("insert");

        // Another expired tracked directory
        db.insert_or_update_directory(
            "/data/expired2",
            2048,
            10,
            Some(hundred_days_ago.as_second()),
            now.as_second(),
        )
        .expect("insert");

        // Non-expired tracked directory
        db.insert_or_update_directory(
            "/data/recent",
            512,
            2,
            Some(ten_days_ago.as_second()),
            now.as_second(),
        )
        .expect("insert");

        // Expired deferral
        let deferred_id = db
            .insert_or_update_directory("/data/deferred", 256, 1, None, now.as_second())
            .expect("insert");
        db.conn()
            .execute(
                "UPDATE directories SET status = 'deferred', deferred_until = ?1 WHERE id = ?2",
                (yesterday.as_second(), deferred_id),
            )
            .expect("set deferred");

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        assert_eq!(summary.expired_to_pending, 2);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 1);

        // Verify each directory
        assert_eq!(
            db.get_directory_by_path("/data/expired1")
                .unwrap()
                .unwrap()
                .status,
            "pending"
        );
        assert_eq!(
            db.get_directory_by_path("/data/expired2")
                .unwrap()
                .unwrap()
                .status,
            "pending"
        );
        assert_eq!(
            db.get_directory_by_path("/data/recent")
                .unwrap()
                .unwrap()
                .status,
            "tracked"
        );
        assert_eq!(
            db.get_directory_by_path("/data/deferred")
                .unwrap()
                .unwrap()
                .status,
            "tracked"
        );
    }

    #[test]
    fn transition_expired_paths_handles_directory_without_mtime() {
        let (_temp, db) = temp_database();

        let now = jiff::Timestamp::now();

        // Directory with no oldest_mtime (empty directory)
        db.insert_or_update_directory("/data/empty", 0, 0, None, now.as_second())
            .expect("insert directory");

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        // Should not transition directories without mtime
        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let dir = db
            .get_directory_by_path("/data/empty")
            .expect("query")
            .expect("exists");
        assert_eq!(dir.status, "tracked");
    }

    // === Additional High-Priority Tests from testing-guru Review ===

    #[test]
    fn transition_expired_paths_ignores_pending_approved_removed_blocked() {
        let (_temp, db) = temp_database();
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .unwrap();

        // Create directories with various statuses that should NOT be transitioned
        for (path, status) in [
            ("/data/pending", "pending"),
            ("/data/approved", "approved"),
            ("/data/removed", "removed"),
            ("/data/blocked", "blocked"),
        ] {
            let id = db
                .insert_or_update_directory(
                    path,
                    1024,
                    5,
                    Some(hundred_days_ago.as_second()),
                    now.as_second(),
                )
                .expect("insert");
            db.update_directory_status(id, status).expect("set status");
        }

        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify statuses unchanged
        for (path, expected_status) in [
            ("/data/pending", "pending"),
            ("/data/approved", "approved"),
            ("/data/removed", "removed"),
            ("/data/blocked", "blocked"),
        ] {
            let dir = db.get_directory_by_path(path).unwrap().unwrap();
            assert_eq!(
                dir.status, expected_status,
                "Status for {path} should be unchanged"
            );
        }
    }

    #[test]
    fn transition_expired_paths_handles_deferred_with_null_deferred_until() {
        let (_temp, db) = temp_database();
        let now = jiff::Timestamp::now();

        let id = db
            .insert_or_update_directory("/data/deferred-null", 1024, 5, None, now.as_second())
            .expect("insert");

        // Set status to deferred but leave deferred_until as NULL
        db.conn()
            .execute(
                "UPDATE directories SET status = 'deferred' WHERE id = ?1",
                (id,),
            )
            .expect("set deferred status without deferred_until");

        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        // Should NOT reset because deferred_until is None
        assert_eq!(summary.deferred_reset, 0);

        let dir = db
            .get_directory_by_path("/data/deferred-null")
            .unwrap()
            .unwrap();
        assert_eq!(dir.status, "deferred");
    }

    #[test]
    fn transition_expired_paths_handles_empty_database() {
        let (_temp, db) = temp_database();

        let summary = super::transition_expired_paths(&db, 90, false).expect("transition");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);
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
        let summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
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
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
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
        let _summary = scan_and_persist(&db, &scanner, &[project_dir], 90, 14)
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
        let _summary = scan_and_persist(&db, &scanner, &[project_dir], 90, 14)
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
        let summary = scan_and_persist(&db, &scanner, &[dir1.clone(), dir2.clone()], 90, 14)
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
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
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
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
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

    // === Stats Update Tests ===

    #[tokio::test]
    async fn stats_update_calculates_paths_within_warning() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create a directory with a file that's within the warning period
        // For 90-day expiration and 14-day warning: file should be 77-89 days old to be in warning
        let warning_dir = root.join("warning");
        fs::create_dir(&warning_dir).unwrap();
        File::create(warning_dir.join("old.txt"))
            .unwrap()
            .write_all(&[0u8; 100])
            .unwrap();

        // Set the file's mtime to 80 days ago (within warning period)
        let now = jiff::Timestamp::now();
        let eighty_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(80 * SECS_PER_DAY))
            .unwrap();
        let old_time = FileTime::from_unix_time(eighty_days_ago.as_second(), 0);
        set_file_mtime(warning_dir.join("old.txt"), old_time).unwrap();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[warning_dir], 90, 14)
            .await
            .unwrap();

        // Verify stats were calculated correctly
        let stats = db.get_stats().unwrap();
        assert_eq!(stats.total_tracked_paths, 1);
        assert_eq!(
            stats.paths_within_warning, 1,
            "Directory with file 80 days old should be in warning period (10 days remaining, within 14-day warning)"
        );
        assert_eq!(stats.paths_pending_approval, 0);
        assert_eq!(stats.paths_overdue, 0);
    }

    #[tokio::test]
    async fn stats_update_calculates_paths_pending_approval() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create a directory and manually set status to 'pending'
        let pending_dir = root.join("pending");
        fs::create_dir(&pending_dir).unwrap();
        File::create(pending_dir.join("file.txt"))
            .unwrap()
            .write_all(&[0u8; 50])
            .unwrap();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&pending_dir), 90, 14)
            .await
            .unwrap();

        // Manually set status to 'pending'
        let dir = db
            .get_directory_by_path(&pending_dir.to_string_lossy())
            .unwrap()
            .unwrap();
        db.update_directory_status(dir.id, "pending").unwrap();

        // Scan again to update stats
        let _summary = scan_and_persist(&db, &scanner, &[pending_dir], 90, 14)
            .await
            .unwrap();

        // Verify stats
        let stats = db.get_stats().unwrap();
        assert_eq!(stats.paths_pending_approval, 1);
    }

    #[tokio::test]
    async fn stats_update_calculates_paths_overdue() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create a directory with an old file (expired)
        let overdue_dir = root.join("overdue");
        fs::create_dir(&overdue_dir).unwrap();
        File::create(overdue_dir.join("ancient.txt"))
            .unwrap()
            .write_all(&[0u8; 100])
            .unwrap();

        // Set file mtime to 100 days ago (overdue for 90-day expiration)
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .unwrap();
        let old_time = FileTime::from_unix_time(hundred_days_ago.as_second(), 0);
        set_file_mtime(overdue_dir.join("ancient.txt"), old_time).unwrap();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[overdue_dir], 90, 14)
            .await
            .unwrap();

        // Verify stats - should have 1 overdue path (status is still 'tracked')
        let stats = db.get_stats().unwrap();
        assert_eq!(stats.total_tracked_paths, 1);
        assert_eq!(
            stats.paths_overdue, 1,
            "Directory with 100-day-old file should be overdue"
        );
        assert_eq!(stats.paths_pending_approval, 0);
        assert_eq!(stats.paths_within_warning, 0);
    }

    #[tokio::test]
    async fn stats_update_handles_mixed_scenarios() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let now = jiff::Timestamp::now();

        // Create three directories with different scenarios
        // 1. Recent file (safe)
        let safe_dir = root.join("safe");
        fs::create_dir(&safe_dir).unwrap();
        File::create(safe_dir.join("recent.txt"))
            .unwrap()
            .write_all(&[0u8; 50])
            .unwrap();

        // 2. File 80 days old (warning period)
        let warning_dir = root.join("warning");
        fs::create_dir(&warning_dir).unwrap();
        File::create(warning_dir.join("warning.txt"))
            .unwrap()
            .write_all(&[0u8; 50])
            .unwrap();
        let eighty_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(80 * SECS_PER_DAY))
            .unwrap();
        set_file_mtime(
            warning_dir.join("warning.txt"),
            FileTime::from_unix_time(eighty_days_ago.as_second(), 0),
        )
        .unwrap();

        // 3. File 100 days old (overdue)
        let overdue_dir = root.join("overdue");
        fs::create_dir(&overdue_dir).unwrap();
        File::create(overdue_dir.join("overdue.txt"))
            .unwrap()
            .write_all(&[0u8; 50])
            .unwrap();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .unwrap();
        set_file_mtime(
            overdue_dir.join("overdue.txt"),
            FileTime::from_unix_time(hundred_days_ago.as_second(), 0),
        )
        .unwrap();

        // Create database and scan all three
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(
            &db,
            &scanner,
            &[safe_dir, warning_dir.clone(), overdue_dir],
            90,
            14,
        )
        .await
        .unwrap();

        // Mark warning_dir as 'pending'
        let dir = db
            .get_directory_by_path(&warning_dir.to_string_lossy())
            .unwrap()
            .unwrap();
        db.update_directory_status(dir.id, "pending").unwrap();

        // Scan again to update stats
        let _summary = scan_and_persist(
            &db,
            &scanner,
            &[root.join("safe"), warning_dir, root.join("overdue")],
            90,
            14,
        )
        .await
        .unwrap();

        // Verify stats
        let stats = db.get_stats().unwrap();
        assert_eq!(stats.total_tracked_paths, 3);
        assert_eq!(stats.paths_overdue, 1, "One overdue directory");
        assert_eq!(stats.paths_pending_approval, 1, "One pending directory");
        // Note: warning_dir is now 'pending', so paths_within_warning should be 0
        assert_eq!(
            stats.paths_within_warning, 0,
            "Warning path was marked pending"
        );
    }

    #[tokio::test]
    async fn stats_update_excludes_ignored_from_overdue_count() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create directory with old file
        let ignored_dir = root.join("ignored");
        fs::create_dir(&ignored_dir).unwrap();
        File::create(ignored_dir.join("old.txt"))
            .unwrap()
            .write_all(&[0u8; 50])
            .unwrap();

        // Set file to 100 days old
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .unwrap();
        set_file_mtime(
            ignored_dir.join("old.txt"),
            FileTime::from_unix_time(hundred_days_ago.as_second(), 0),
        )
        .unwrap();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&ignored_dir), 90, 14)
            .await
            .unwrap();

        // Mark as ignored
        let dir = db
            .get_directory_by_path(&ignored_dir.to_string_lossy())
            .unwrap()
            .unwrap();
        db.update_directory_status(dir.id, "ignored").unwrap();

        // Scan again to update stats
        let _summary = scan_and_persist(&db, &scanner, &[ignored_dir], 90, 14)
            .await
            .unwrap();

        // Verify stats - ignored directory should NOT be counted as overdue
        let stats = db.get_stats().unwrap();
        assert_eq!(
            stats.paths_overdue, 0,
            "Ignored paths should not be counted as overdue"
        );
    }

    #[tokio::test]
    async fn stats_update_custom_expiration_warning_periods() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create directory with file 25 days old
        let dir = root.join("test");
        fs::create_dir(&dir).unwrap();
        File::create(dir.join("file.txt"))
            .unwrap()
            .write_all(&[0u8; 50])
            .unwrap();

        let now = jiff::Timestamp::now();
        let twentyfive_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(25 * SECS_PER_DAY))
            .unwrap();
        set_file_mtime(
            dir.join("file.txt"),
            FileTime::from_unix_time(twentyfive_days_ago.as_second(), 0),
        )
        .unwrap();

        // Create database and scan with custom periods:
        // expiration_days = 30, warning_days = 7
        // File is 25 days old, so 5 days remaining - within 7-day warning period
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[dir], 30, 7)
            .await
            .unwrap();

        // Verify stats
        let stats = db.get_stats().unwrap();
        assert_eq!(stats.total_tracked_paths, 1);
        assert_eq!(
            stats.paths_within_warning, 1,
            "With 30-day expiration and 7-day warning, 25-day-old file (5 days remaining) should be in warning"
        );
        assert_eq!(stats.paths_overdue, 0);
    }

    #[tokio::test]
    async fn stats_update_handles_directories_without_mtime() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create empty directory (no files, so no oldest_mtime)
        let empty_dir = root.join("empty");
        fs::create_dir(&empty_dir).unwrap();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[empty_dir], 90, 14)
            .await
            .unwrap();

        // Verify stats - directories without mtime should not be counted in warning/overdue
        let stats = db.get_stats().unwrap();
        // Note: empty directories don't get inserted by scan_and_persist
        // because scan_directory_tree only aggregates directories with files
        assert_eq!(stats.total_tracked_paths, 0);
        assert_eq!(stats.paths_within_warning, 0);
        assert_eq!(stats.paths_overdue, 0);
    }

    #[tokio::test]
    async fn stats_update_sets_last_scan_completed_timestamp() {
        use crate::db::Database;

        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        // Create a directory with a file
        let dir = root.join("test");
        fs::create_dir(&dir).unwrap();
        File::create(dir.join("file.txt"))
            .unwrap()
            .write_all(&[0u8; 50])
            .unwrap();

        // Record current time before scan
        let before_scan = jiff::Timestamp::now().as_second();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).unwrap();
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[dir], 90, 14)
            .await
            .unwrap();

        // Record current time after scan
        let after_scan = jiff::Timestamp::now().as_second();

        // Verify last_scan_completed was set and is within reasonable range
        let stats = db.get_stats().unwrap();
        assert!(
            stats.last_scan_completed.is_some(),
            "last_scan_completed should be set"
        );
        let last_scan = stats.last_scan_completed.unwrap();
        assert!(
            last_scan >= before_scan && last_scan <= after_scan,
            "last_scan_completed ({last_scan}) should be between {before_scan} and {after_scan}"
        );
    }
}
