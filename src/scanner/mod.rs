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

            // Recalculate effective oldest timestamp using max(mtime, tracked_since)
            // This ensures newly-tracked old files get a full expiration period
            recalculate_directory_oldest_mtime(db, dir_id)?;

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

/// Recalculate a directory's effective oldest timestamp.
///
/// After file upserts, we need to recalculate the directory's `oldest_mtime` field
/// to reflect the effective expiration time based on `max(mtime, tracked_since)` for
/// each file. This ensures newly-tracked old files don't appear overdue.
///
/// For backward compatibility, files with `NULL` `tracked_since` default to their mtime.
///
/// # Arguments
///
/// * `db` - Database connection
/// * `directory_id` - ID of the directory to recalculate
///
/// # Errors
///
/// Returns an error if the database query fails.
///
/// # Visibility
///
/// This function is public for testing purposes but is not part of the stable API.
#[doc(hidden)]
pub fn recalculate_directory_oldest_mtime(db: &Database, directory_id: i64) -> Result<()> {
    debug_assert!(directory_id > 0, "directory_id must be positive");

    let rows_affected = db.conn().execute(
        "UPDATE directories
         SET oldest_mtime = (
             SELECT MIN(MAX(mtime, COALESCE(tracked_since, mtime)))
             FROM files
             WHERE directory_id = ?1
         )
         WHERE id = ?1",
        [directory_id],
    )?;

    debug_assert!(
        rows_affected <= 1,
        "UPDATE should affect at most one row (got {rows_affected})"
    );

    Ok(())
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
    // Allow: Test code should panic on unexpected errors for fast failure.
    // Using expect() instead of unwrap() in tests adds noise without value.
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
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create a simple directory structure:
        // root/
        //   file1.txt (100 bytes)
        //   subdir/
        //     file2.txt (200 bytes)
        //     file3.txt (300 bytes)

        let mut file1 = File::create(root.join("file1.txt"))
            .expect("failed to create test file - check disk space and permissions");
        file1
            .write_all(&[0u8; 100])
            .expect("failed to write test data to file - disk may be full");
        file1
            .sync_all()
            .expect("failed to sync test file to disk - check filesystem health");

        let subdir = root.join("subdir");
        fs::create_dir(&subdir)
            .expect("failed to create test directory - check disk space and permissions");

        let mut file2 = File::create(subdir.join("file2.txt"))
            .expect("failed to create test file - check disk space and permissions");
        file2
            .write_all(&[0u8; 200])
            .expect("failed to write test data to file - disk may be full");
        file2
            .sync_all()
            .expect("failed to sync test file to disk - check filesystem health");

        let mut file3 = File::create(subdir.join("file3.txt"))
            .expect("failed to create test file - check disk space and permissions");
        file3
            .write_all(&[0u8; 300])
            .expect("failed to write test data to file - disk may be full");
        file3
            .sync_all()
            .expect("failed to sync test file to disk - check filesystem health");

        temp_dir
    }

    #[tokio::test]
    async fn scanner_finds_all_files() {
        let temp_dir = create_test_tree();
        let root = temp_dir.path();

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.expect(
            "scanner should successfully scan test directory - check permissions and disk space",
        );

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
        let result = scanner.scan(root).await.expect(
            "scanner should successfully scan test directory - check permissions and disk space",
        );

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
        let result = scanner.scan(root).await.expect(
            "scanner should successfully scan test directory - check permissions and disk space",
        );

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
        match result.expect_err("expected error result for nonexistent path") {
            Error::PathNotFound(_) => {}
            e => panic!("Expected PathNotFound error, got: {e:?}"),
        }
    }

    #[tokio::test]
    async fn scanner_fails_on_file_path() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let file_path = temp_dir.path().join("file.txt");
        File::create(&file_path)
            .expect("failed to create test file - check disk space and permissions");

        let scanner = Scanner::new();
        let result = scanner.scan(&file_path).await;

        assert!(result.is_err(), "Expected error when scanning a file");
        match result.expect_err("expected error result when scanning a file path") {
            Error::Filesystem { .. } => {}
            e => panic!("Expected Filesystem error about directory, got: {e:?}"),
        }
    }

    #[tokio::test]
    async fn scanner_handles_empty_directory() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );

        let scanner = Scanner::new();
        let result = scanner.scan(temp_dir.path()).await.expect(
            "scanner should successfully scan test directory - check permissions and disk space",
        );

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
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create a file
        let target_file = root.join("target.txt");
        let mut file = File::create(&target_file)
            .expect("failed to create test file - check disk space and permissions");
        file.write_all(&[0u8; 150])
            .expect("failed to write test data to file - disk may be full");
        file.sync_all()
            .expect("failed to sync test file to disk - check filesystem health");

        // Create a symlink to the file
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            symlink(&target_file, root.join("link.txt")).expect(
                "failed to create symlink for test - check filesystem support for symlinks",
            );
        }

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.expect(
            "scanner should successfully scan test directory - check permissions and disk space",
        );

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

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create a valid file
        let mut file = File::create(root.join("valid.txt"))
            .expect("failed to create test file - check disk space and permissions");
        file.write_all(&[0u8; 100])
            .expect("failed to write test data to file - disk may be full");
        file.sync_all()
            .expect("failed to sync test file to disk - check filesystem health");

        // Create a broken symlink pointing to a non-existent target
        symlink("/nonexistent/target", root.join("broken_link"))
            .expect("failed to create symlink for test - check filesystem support for symlinks");

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.expect(
            "scanner should successfully scan test directory - check permissions and disk space",
        );

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
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create two files
        let file1 = root.join("old.txt");
        let file2 = root.join("new.txt");
        File::create(&file1)
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 10])
            .expect("failed to write test data to file - disk may be full");
        File::create(&file2)
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 10])
            .expect("failed to write test data to file - disk may be full");

        // Set file1 to an older time (2001-09-09)
        let old_time = FileTime::from_unix_time(1_000_000_000, 0);
        set_file_mtime(&file1, old_time)
            .expect("failed to set file modification time for test - check filesystem support");

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.expect(
            "scanner should successfully scan test directory - check permissions and disk space",
        );

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
        let expected = jiff::Timestamp::from_second(1_000_000_000)
            .expect("timestamp should be valid for test data - check time value is in valid range");

        // Should be within 1 second (accounting for filesystem timestamp precision)
        let diff_seconds = (oldest.as_second() - expected.as_second()).abs();
        assert!(
            diff_seconds <= 1,
            "oldest_mtime should be close to the old file's mtime (expected ~{expected}, got {oldest}, diff={diff_seconds}s)"
        );
    }

    #[tokio::test]
    async fn scanner_includes_hidden_files() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create a hidden file (starts with dot)
        let mut hidden = File::create(root.join(".hidden"))
            .expect("failed to create test file - check disk space and permissions");
        hidden
            .write_all(&[0u8; 50])
            .expect("failed to write test data to file - disk may be full");
        hidden
            .sync_all()
            .expect("failed to sync test file to disk - check filesystem health");

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.expect(
            "scanner should successfully scan test directory - check permissions and disk space",
        );

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
            .expect("timestamp arithmetic should succeed for test data - check duration values");

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
            .expect("timestamp arithmetic should succeed for test data - check duration values");

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
            .expect("timestamp arithmetic should succeed for test data - check duration values");

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
            .expect("timestamp arithmetic should succeed for test data - check duration values");

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
            .expect("timestamp arithmetic should succeed for test data - check duration values");

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
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        assert_eq!(dir_before.status, "tracked");

        // Run transition with 90-day expiration policy and auto_remove=false
        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 1);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status changed to 'pending'
        let dir_after = db
            .get_directory_by_path("/data/expired")
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        assert_eq!(dir_after.status, "pending");

        // Verify audit entry was created
        let audit = crate::audit::AuditService::new(&db);
        let entries = audit
            .list_by_path("/data/expired")
            .expect("failed to query recent audit entries - database connection may be lost");
        assert_eq!(entries.len(), 1);
        assert!(
            entries[0]
                .details
                .as_ref()
                .expect("audit entry should have details field populated")
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
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        db.insert_or_update_directory(
            "/data/expired",
            1024,
            5,
            Some(hundred_days_ago.as_second()),
            now.as_second(),
        )
        .expect("failed to insert test directory - database connection may be lost");

        // Run transition with auto_remove=true
        let summary = super::transition_expired_paths(&db, 90, true)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 1);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status changed to 'approved'
        let dir = db
            .get_directory_by_path("/data/expired")
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        assert_eq!(dir.status, "approved");
    }

    #[test]
    fn transition_expired_paths_does_not_transition_non_expired() {
        let (_temp, db) = temp_database();

        // Insert a directory with recent mtime (10 days ago)
        let now = jiff::Timestamp::now();
        let ten_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(10 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        db.insert_or_update_directory(
            "/data/recent",
            1024,
            5,
            Some(ten_days_ago.as_second()),
            now.as_second(),
        )
        .expect("failed to insert test directory - database connection may be lost");

        // Run transition with 90-day policy
        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let dir = db
            .get_directory_by_path("/data/recent")
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        assert_eq!(dir.status, "tracked");
    }

    #[test]
    fn transition_expired_paths_resets_expired_deferral() {
        let (_temp, db) = temp_database();

        let now = jiff::Timestamp::now();
        let yesterday = now
            .checked_sub(jiff::SignedDuration::from_secs(SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        // Insert a directory with deferred status
        let id = db
            .insert_or_update_directory("/data/deferred", 1024, 5, None, now.as_second())
            .expect("failed to insert test directory - database connection may be lost");

        // Set status to 'deferred' with deferred_until in the past
        db.conn()
            .execute(
                "UPDATE directories SET status = 'deferred', deferred_until = ?1 WHERE id = ?2",
                (yesterday.as_second(), id),
            )
            .expect("failed to update directory status in test - database connection may be lost");

        // Verify initial state
        let dir_before = db
            .get_directory_by_path("/data/deferred")
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        assert_eq!(dir_before.status, "deferred");
        assert!(dir_before.deferred_until.is_some());

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 1);

        // Verify status reset to 'tracked' and deferred_until cleared
        let dir_after = db
            .get_directory_by_path("/data/deferred")
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        assert_eq!(dir_after.status, "tracked");
        assert_eq!(dir_after.deferred_until, None);
    }

    #[test]
    fn transition_expired_paths_does_not_reset_active_deferral() {
        let (_temp, db) = temp_database();

        let now = jiff::Timestamp::now();
        let next_week = now
            .checked_add(jiff::SignedDuration::from_secs(7 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        // Insert a directory with deferred status
        let id = db
            .insert_or_update_directory("/data/deferred", 1024, 5, None, now.as_second())
            .expect("failed to insert test directory - database connection may be lost");

        // Set status to 'deferred' with deferred_until in the future
        db.conn()
            .execute(
                "UPDATE directories SET status = 'deferred', deferred_until = ?1 WHERE id = ?2",
                (next_week.as_second(), id),
            )
            .expect("failed to update directory status in test - database connection may be lost");

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let dir = db
            .get_directory_by_path("/data/deferred")
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
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
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        let id = db
            .insert_or_update_directory(
                "/data/ignored",
                1024,
                5,
                Some(hundred_days_ago.as_second()),
                now.as_second(),
            )
            .expect("failed to insert test directory - database connection may be lost");

        // Set status to 'ignored'
        db.update_directory_status(id, "ignored")
            .expect("failed to update directory status - database connection may be lost");

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let dir = db
            .get_directory_by_path("/data/ignored")
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        assert_eq!(dir.status, "ignored");
    }

    #[test]
    fn transition_expired_paths_handles_multiple_directories() {
        let (_temp, db) = temp_database();

        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");
        let ten_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(10 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");
        let yesterday = now
            .checked_sub(jiff::SignedDuration::from_secs(SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        // Expired tracked directory
        db.insert_or_update_directory(
            "/data/expired1",
            1024,
            5,
            Some(hundred_days_ago.as_second()),
            now.as_second(),
        )
        .expect("failed to insert test directory - database connection may be lost");

        // Another expired tracked directory
        db.insert_or_update_directory(
            "/data/expired2",
            2048,
            10,
            Some(hundred_days_ago.as_second()),
            now.as_second(),
        )
        .expect("failed to insert test directory - database connection may be lost");

        // Non-expired tracked directory
        db.insert_or_update_directory(
            "/data/recent",
            512,
            2,
            Some(ten_days_ago.as_second()),
            now.as_second(),
        )
        .expect("failed to insert test directory - database connection may be lost");

        // Expired deferral
        let deferred_id = db
            .insert_or_update_directory("/data/deferred", 256, 1, None, now.as_second())
            .expect("failed to insert test directory - database connection may be lost");
        db.conn()
            .execute(
                "UPDATE directories SET status = 'deferred', deferred_until = ?1 WHERE id = ?2",
                (yesterday.as_second(), deferred_id),
            )
            .expect("failed to update directory status in test - database connection may be lost");

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 2);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 1);

        // Verify each directory
        assert_eq!(
            db.get_directory_by_path("/data/expired1")
                .expect("failed to query directory from database - connection may be lost")
                .expect("expected directory to exist after scan - verify scanner persisted data correctly")
                .status,
            "pending"
        );
        assert_eq!(
            db.get_directory_by_path("/data/expired2")
                .expect("failed to query directory from database - connection may be lost")
                .expect("expected directory to exist after scan - verify scanner persisted data correctly")
                .status,
            "pending"
        );
        assert_eq!(
            db.get_directory_by_path("/data/recent")
                .expect("failed to query directory from database - connection may be lost")
                .expect("expected directory to exist after scan - verify scanner persisted data correctly")
                .status,
            "tracked"
        );
        assert_eq!(
            db.get_directory_by_path("/data/deferred")
                .expect("failed to query directory from database - connection may be lost")
                .expect("expected directory to exist after scan - verify scanner persisted data correctly")
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
            .expect("failed to insert test directory - database connection may be lost");

        // Run transition
        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

        // Should not transition directories without mtime
        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let dir = db
            .get_directory_by_path("/data/empty")
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        assert_eq!(dir.status, "tracked");
    }

    // === Additional High-Priority Tests from testing-guru Review ===

    #[test]
    fn transition_expired_paths_ignores_pending_approved_removed_blocked() {
        let (_temp, db) = temp_database();
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

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
                .expect("failed to insert test directory - database connection may be lost");
            db.update_directory_status(id, status)
                .expect("failed to update directory status - database connection may be lost");
        }

        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

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
            let dir = db.get_directory_by_path(path).expect("failed to query directory from database - connection may be lost").expect("expected directory to exist after scan - verify scanner persisted data correctly");
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
            .expect("failed to insert test directory - database connection may be lost");

        // Set status to deferred but leave deferred_until as NULL
        db.conn()
            .execute(
                "UPDATE directories SET status = 'deferred' WHERE id = ?1",
                (id,),
            )
            .expect("failed to update directory status in test - database connection may be lost");

        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

        // Should NOT reset because deferred_until is None
        assert_eq!(summary.deferred_reset, 0);

        let dir = db
            .get_directory_by_path("/data/deferred-null")
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        assert_eq!(dir.status, "deferred");
    }

    #[test]
    fn transition_expired_paths_handles_empty_database() {
        let (_temp, db) = temp_database();

        let summary = super::transition_expired_paths(&db, 90, false)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);
    }

    // === Integration Tests ===

    #[tokio::test]
    async fn scan_and_persist_creates_directory_records() {
        use crate::db::Database;

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create test directory structure
        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create test directory - check disk space and permissions");
        let mut file1 = File::create(project_dir.join("file1.txt"))
            .expect("failed to create test file - check disk space and permissions");
        file1
            .write_all(&[0u8; 100])
            .expect("failed to write test data to file - disk may be full");
        file1
            .sync_all()
            .expect("failed to sync test file to disk - check filesystem health");

        // Create database
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");

        // Run scan_and_persist
        let scanner = Scanner::new();
        let summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Verify summary
        assert_eq!(summary.total_directories, 1);
        assert_eq!(summary.total_files, 1);
        assert_eq!(summary.total_size_bytes, 100);

        // Verify directory was persisted
        let dir = db
            .get_directory_by_path(&project_dir.to_string_lossy())
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );

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

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create test files
        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(project_dir.join("a.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 10])
            .expect("failed to write test data to file - disk may be full");
        File::create(project_dir.join("b.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 20])
            .expect("failed to write test data to file - disk may be full");

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Get directory id
        let dir = db
            .get_directory_by_path(&project_dir.to_string_lossy())
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );

        // Verify files were persisted
        let files = db
            .list_files_by_directory(dir.id)
            .expect("failed to list files from database - connection may be lost");
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

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create test structure
        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(project_dir.join("file.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 500])
            .expect("failed to write test data to file - disk may be full");

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[project_dir], 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Verify stats were updated
        let stats: (i64, i64, Option<i64>) = db
            .conn()
            .query_row(
                "SELECT total_tracked_paths, total_size_bytes, last_scan_completed
                 FROM stats WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("failed to query stats from database - connection may be lost");

        assert_eq!(stats.0, 1, "Expected 1 tracked directory");
        assert_eq!(stats.1, 500, "Expected 500 bytes total");
        assert!(stats.2.is_some(), "Expected last_scan_completed to be set");
    }

    #[tokio::test]
    async fn scan_and_persist_records_audit_entry() {
        use crate::audit::AuditService;
        use crate::db::Database;

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create test structure
        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(project_dir.join("file.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 100])
            .expect("failed to write test data to file - disk may be full");

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[project_dir], 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Verify audit entry was created
        let audit = AuditService::new(&db);
        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");

        assert_eq!(entries.len(), 1, "Expected 1 audit entry");
        assert_eq!(entries[0].action, "scan");
        assert!(entries[0].details.is_some());
        assert!(
            entries[0]
                .details
                .as_ref()
                .expect("audit entry should have details field populated")
                .contains("1 directories"),
            "Expected details to mention directories"
        );
    }

    #[tokio::test]
    async fn scan_and_persist_handles_multiple_paths() {
        use crate::db::Database;

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create two separate directories
        let dir1 = root.join("project1");
        let dir2 = root.join("project2");
        fs::create_dir(&dir1)
            .expect("failed to create test directory - check disk space and permissions");
        fs::create_dir(&dir2)
            .expect("failed to create test directory - check disk space and permissions");

        File::create(dir1.join("file1.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 100])
            .expect("failed to write test data to file - disk may be full");
        File::create(dir2.join("file2.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 200])
            .expect("failed to write test data to file - disk may be full");

        // Create database and scan both paths
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let summary = scan_and_persist(&db, &scanner, &[dir1.clone(), dir2.clone()], 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Verify both directories were scanned
        assert_eq!(summary.total_directories, 2);
        assert_eq!(summary.total_files, 2);
        assert_eq!(summary.total_size_bytes, 300);

        // Verify both are in database
        assert!(
            db.get_directory_by_path(&dir1.to_string_lossy())
                .expect("failed to query directory from database - connection may be lost")
                .is_some()
        );
        assert!(
            db.get_directory_by_path(&dir2.to_string_lossy())
                .expect("failed to query directory from database - connection may be lost")
                .is_some()
        );
    }

    #[tokio::test]
    async fn scan_and_persist_upserts_existing_directories() {
        use crate::db::Database;

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create test directory
        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(project_dir.join("file1.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 100])
            .expect("failed to write test data to file - disk may be full");

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Change directory status manually (simulating user action)
        let dir = db
            .get_directory_by_path(&project_dir.to_string_lossy())
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        db.update_directory_status(dir.id, "approved")
            .expect("failed to update directory status - database connection may be lost");

        // Add a new file
        File::create(project_dir.join("file2.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 50])
            .expect("failed to write test data to file - disk may be full");

        // Scan again
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Verify directory was updated but status preserved
        let updated_dir = db
            .get_directory_by_path(&project_dir.to_string_lossy())
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );

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

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create a directory with a file
        let warning_dir = root.join("warning");
        fs::create_dir(&warning_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(warning_dir.join("old.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 100])
            .expect("failed to write test data to file - disk may be full");

        // Create database and do first scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&warning_dir), 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Manually set tracked_since to 80 days ago and mtime to even older
        // to simulate a file tracked for 80 days. The effective timestamp will be
        // max(mtime, tracked_since) = tracked_since = 80 days ago.
        // This puts it in the warning period (10 days remaining, within 14-day warning)
        let now = jiff::Timestamp::now();
        let eighty_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(80 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed");
        let ninety_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(90 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed");
        db.conn()
            .execute(
                "UPDATE files SET tracked_since = ?1, mtime = ?2",
                (eighty_days_ago.as_second(), ninety_days_ago.as_second()),
            )
            .expect("failed to update tracked_since for test");

        // Recalculate directory oldest_mtime and stats
        let dirs = db
            .list_directories(None)
            .expect("failed to list directories");
        recalculate_directory_oldest_mtime(&db, dirs[0].id).expect("failed to recalculate");
        // Allow: Test uses single directory, will never overflow i64.
        #[allow(clippy::cast_possible_wrap)]
        let total_dirs = dirs.len() as i64;
        let total_size: i64 = dirs.iter().map(|d| d.size_bytes).sum();
        update_stats(&db, total_dirs, total_size, now.as_second(), 90, 14)
            .expect("failed to update stats");

        // Verify stats were calculated correctly
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
        assert_eq!(stats.total_tracked_paths, 1);
        assert_eq!(
            stats.paths_within_warning, 1,
            "Directory with file tracked for 80 days should be in warning period (10 days remaining, within 14-day warning)"
        );
        assert_eq!(stats.paths_pending_approval, 0);
        assert_eq!(stats.paths_overdue, 0);
    }

    #[tokio::test]
    async fn stats_update_calculates_paths_pending_approval() {
        use crate::db::Database;

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create a directory and manually set status to 'pending'
        let pending_dir = root.join("pending");
        fs::create_dir(&pending_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(pending_dir.join("file.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 50])
            .expect("failed to write test data to file - disk may be full");

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&pending_dir), 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Manually set status to 'pending'
        let dir = db
            .get_directory_by_path(&pending_dir.to_string_lossy())
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        db.update_directory_status(dir.id, "pending")
            .expect("failed to update directory status - database connection may be lost");

        // Scan again to update stats
        let _summary = scan_and_persist(&db, &scanner, &[pending_dir], 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Verify stats
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
        assert_eq!(stats.paths_pending_approval, 1);
    }

    #[tokio::test]
    async fn stats_update_calculates_paths_overdue() {
        use crate::db::Database;

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create a directory with a file
        let overdue_dir = root.join("overdue");
        fs::create_dir(&overdue_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(overdue_dir.join("file.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 100])
            .expect("failed to write test data to file - disk may be full");

        // Create database and do first scan (this sets tracked_since to now)
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&overdue_dir), 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Manually set tracked_since to 100 days ago and mtime to even older
        // to simulate a file that's been tracked for a long time.
        // The effective timestamp will be max(mtime, tracked_since) = tracked_since = 100 days ago.
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed");
        let two_hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(200 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed");
        db.conn()
            .execute(
                "UPDATE files SET tracked_since = ?1, mtime = ?2",
                (
                    hundred_days_ago.as_second(),
                    two_hundred_days_ago.as_second(),
                ),
            )
            .expect("failed to update tracked_since for test");

        // Recalculate directory oldest_mtime based on the backdated tracked_since
        let dirs = db
            .list_directories(None)
            .expect("failed to list directories");
        recalculate_directory_oldest_mtime(&db, dirs[0].id).expect("failed to recalculate");

        // Update stats again to reflect the backdated tracked_since
        // Allow: Test uses single directory, will never overflow i64.
        #[allow(clippy::cast_possible_wrap)]
        let total_dirs = dirs.len() as i64;
        let total_size: i64 = dirs.iter().map(|d| d.size_bytes).sum();
        update_stats(&db, total_dirs, total_size, now.as_second(), 90, 14)
            .expect("failed to update stats");

        // Verify stats - should have 1 overdue path (status is still 'tracked')
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
        assert_eq!(stats.total_tracked_paths, 1);
        assert_eq!(
            stats.paths_overdue, 1,
            "Directory with file tracked for 100 days should be overdue"
        );
        assert_eq!(stats.paths_pending_approval, 0);
        assert_eq!(stats.paths_within_warning, 0);
    }

    #[tokio::test]
    async fn stats_update_handles_mixed_scenarios() {
        use crate::db::Database;

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        let now = jiff::Timestamp::now();

        // Create three directories with different scenarios
        // 1. Recent file (safe)
        let safe_dir = root.join("safe");
        fs::create_dir(&safe_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(safe_dir.join("recent.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 50])
            .expect("failed to write test data to file - disk may be full");

        // 2. File 80 days old (warning period)
        let warning_dir = root.join("warning");
        fs::create_dir(&warning_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(warning_dir.join("warning.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 50])
            .expect("failed to write test data to file - disk may be full");
        let eighty_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(80 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");
        set_file_mtime(
            warning_dir.join("warning.txt"),
            FileTime::from_unix_time(eighty_days_ago.as_second(), 0),
        )
        .expect("failed to set file modification time for test - check filesystem support");

        // 3. File 100 days old (overdue)
        let overdue_dir = root.join("overdue");
        fs::create_dir(&overdue_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(overdue_dir.join("overdue.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 50])
            .expect("failed to write test data to file - disk may be full");
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");
        set_file_mtime(
            overdue_dir.join("overdue.txt"),
            FileTime::from_unix_time(hundred_days_ago.as_second(), 0),
        )
        .expect("failed to set file modification time for test - check filesystem support");

        // Create database and scan all three
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(
            &db,
            &scanner,
            &[safe_dir, warning_dir.clone(), overdue_dir],
            90,
            14,
        )
        .await
        .expect(
            "failed to scan and persist directories - check permissions and database connection",
        );

        // Mark warning_dir as 'pending'
        let dir = db
            .get_directory_by_path(&warning_dir.to_string_lossy())
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        db.update_directory_status(dir.id, "pending")
            .expect("failed to update directory status - database connection may be lost");

        // Scan again to update stats
        let _summary = scan_and_persist(
            &db,
            &scanner,
            &[root.join("safe"), warning_dir, root.join("overdue")],
            90,
            14,
        )
        .await
        .expect(
            "failed to scan and persist directories - check permissions and database connection",
        );

        // Verify stats
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
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

        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create directory with old file
        let ignored_dir = root.join("ignored");
        fs::create_dir(&ignored_dir)
            .expect("failed to create test directory - check disk space and permissions");
        File::create(ignored_dir.join("old.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 50])
            .expect("failed to write test data to file - disk may be full");

        // Set file to 100 days old
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");
        set_file_mtime(
            ignored_dir.join("old.txt"),
            FileTime::from_unix_time(hundred_days_ago.as_second(), 0),
        )
        .expect("failed to set file modification time for test - check filesystem support");

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&ignored_dir), 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Mark as ignored
        let dir = db
            .get_directory_by_path(&ignored_dir.to_string_lossy())
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after scan - verify scanner persisted data correctly",
            );
        db.update_directory_status(dir.id, "ignored")
            .expect("failed to update directory status - database connection may be lost");

        // Scan again to update stats
        let _summary = scan_and_persist(&db, &scanner, &[ignored_dir], 90, 14)
            .await
            .expect("failed to scan and persist directories - check permissions and database connection");

        // Verify stats - ignored directory should NOT be counted as overdue
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
        assert_eq!(
            stats.paths_overdue, 0,
            "Ignored paths should not be counted as overdue"
        );
    }

    #[tokio::test]
    async fn stats_update_custom_expiration_warning_periods() {
        use crate::db::Database;

        let temp_dir = TempDir::new().expect("failed to create temp directory for test - check disk space and system temp directory permissions");
        let root = temp_dir.path();

        // Create directory with file
        let dir = root.join("test");
        fs::create_dir(&dir).expect("failed to create test directory - check disk space and write permissions on temp directory");
        File::create(dir.join("file.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 50])
            .expect("failed to write test data - disk may be full or readonly");

        // Create database and scan with custom periods: expiration_days = 30, warning_days = 7
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to initialize database - check disk space and SQLite is functioning");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, std::slice::from_ref(&dir), 30, 7)
            .await
            .expect("scan_and_persist failed - check file permissions and database connection");

        // Manually set tracked_since to 25 days ago and mtime to even older
        // to simulate a file tracked for 25 days. The effective timestamp will be
        // max(mtime, tracked_since) = tracked_since = 25 days ago.
        // This gives it 5 days remaining, which is within the 7-day warning period
        let now = jiff::Timestamp::now();
        let twentyfive_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(25 * SECS_PER_DAY))
            .expect("timestamp arithmetic overflow");
        let thirty_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(30 * SECS_PER_DAY))
            .expect("timestamp arithmetic overflow");
        db.conn()
            .execute(
                "UPDATE files SET tracked_since = ?1, mtime = ?2",
                (twentyfive_days_ago.as_second(), thirty_days_ago.as_second()),
            )
            .expect("failed to update tracked_since for test");

        // Recalculate directory oldest_mtime and stats
        let dirs = db
            .list_directories(None)
            .expect("failed to list directories");
        recalculate_directory_oldest_mtime(&db, dirs[0].id).expect("failed to recalculate");
        // Allow: Test uses single directory, will never overflow i64.
        #[allow(clippy::cast_possible_wrap)]
        let total_dirs = dirs.len() as i64;
        let total_size: i64 = dirs.iter().map(|d| d.size_bytes).sum();
        update_stats(&db, total_dirs, total_size, now.as_second(), 30, 7)
            .expect("failed to update stats");

        // Verify stats
        let stats = db.get_stats().expect(
            "failed to query stats from database - connection may be lost or stats table corrupted",
        );
        assert_eq!(stats.total_tracked_paths, 1);
        assert_eq!(
            stats.paths_within_warning, 1,
            "With 30-day expiration and 7-day warning, file tracked for 25 days (5 days remaining) should be in warning"
        );
        assert_eq!(stats.paths_overdue, 0);
    }

    #[tokio::test]
    async fn stats_update_handles_directories_without_mtime() {
        use crate::db::Database;

        let temp_dir = TempDir::new().expect("failed to create temp directory for test - check disk space and system temp directory permissions");
        let root = temp_dir.path();

        // Create empty directory (no files, so no oldest_mtime)
        let empty_dir = root.join("empty");
        fs::create_dir(&empty_dir).expect(
            "failed to create empty test directory - check disk space and write permissions",
        );

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to initialize database - check disk space and SQLite is functioning");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[empty_dir], 90, 14)
            .await
            .expect("scan_and_persist failed on empty directory - check permissions and database connection");

        // Verify stats - directories without mtime should not be counted in warning/overdue
        let stats = db.get_stats().expect(
            "failed to query stats from database - connection may be lost or stats table corrupted",
        );
        // Note: empty directories don't get inserted by scan_and_persist
        // because scan_directory_tree only aggregates directories with files
        assert_eq!(stats.total_tracked_paths, 0);
        assert_eq!(stats.paths_within_warning, 0);
        assert_eq!(stats.paths_overdue, 0);
    }

    #[tokio::test]
    async fn stats_update_sets_last_scan_completed_timestamp() {
        use crate::db::Database;

        let temp_dir = TempDir::new().expect("failed to create temp directory for test - check disk space and system temp directory permissions");
        let root = temp_dir.path();

        // Create a directory with a file
        let dir = root.join("test");
        fs::create_dir(&dir)
            .expect("failed to create test directory - check disk space and write permissions");
        File::create(dir.join("file.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 50])
            .expect("failed to write test data - disk may be full or readonly");

        // Record current time before scan
        let before_scan = jiff::Timestamp::now().as_second();

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to initialize database - check disk space and SQLite is functioning");
        let scanner = Scanner::new();
        let _summary = scan_and_persist(&db, &scanner, &[dir], 90, 14)
            .await
            .expect("scan_and_persist failed - check file permissions and database connection");

        // Record current time after scan
        let after_scan = jiff::Timestamp::now().as_second();

        // Verify last_scan_completed was set and is within reasonable range
        let stats = db.get_stats().expect(
            "failed to query stats from database - connection may be lost or stats table corrupted",
        );
        assert!(
            stats.last_scan_completed.is_some(),
            "last_scan_completed should be set"
        );
        let last_scan = stats.last_scan_completed.expect("last_scan_completed should be Some after scan, but was None - check scan_and_persist updates stats correctly");
        assert!(
            last_scan >= before_scan && last_scan <= after_scan,
            "last_scan_completed ({last_scan}) should be between {before_scan} and {after_scan}"
        );
    }

    // === tracked_since Tests ===

    #[tokio::test]
    async fn scan_sets_tracked_since_on_first_insert() {
        use crate::db::Database;

        let temp_dir = TempDir::new()
            .expect("failed to create temp directory - check disk space and permissions");
        let root = temp_dir.path();

        // Create an old file (mtime = 100 days ago)
        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create test directory - check disk space and permissions");
        let file_path = project_dir.join("old_file.txt");
        let mut file = File::create(&file_path)
            .expect("failed to create test file - check disk space and permissions");
        file.write_all(b"test content")
            .expect("failed to write test data - disk may be full");
        file.sync_all()
            .expect("failed to sync file - check filesystem health");

        // Set mtime to 100 days ago
        let hundred_days_ago = jiff::Timestamp::now()
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic failed");
        #[cfg(unix)]
        {
            use filetime::FileTime;
            filetime::set_file_mtime(
                &file_path,
                FileTime::from_unix_time(hundred_days_ago.as_second(), 0),
            )
            .expect("failed to set file mtime - check permissions");
        }

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).expect("failed to open database - check permissions");
        let scanner = Scanner::new();
        let before_scan = jiff::Timestamp::now().as_second();

        let _ = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
            .await
            .expect("scan failed - check permissions");

        let after_scan = jiff::Timestamp::now().as_second();

        // Query the directory and file
        let dirs = db
            .list_directories(None)
            .expect("failed to list directories");
        assert_eq!(dirs.len(), 1);
        let files = db
            .list_files_by_directory(dirs[0].id)
            .expect("failed to list files");
        assert_eq!(files.len(), 1);

        // Verify tracked_since was set to current time (not the old mtime)
        let tracked_since = files[0]
            .tracked_since
            .expect("tracked_since should be set on first insert");
        assert!(
            tracked_since >= before_scan && tracked_since <= after_scan,
            "tracked_since should be current time, not old mtime"
        );

        // Verify mtime is still the old value
        #[cfg(unix)]
        {
            assert_eq!(
                files[0].mtime,
                hundred_days_ago.as_second(),
                "mtime should preserve file's actual modification time"
            );
        }
    }

    #[tokio::test]
    async fn scan_preserves_tracked_since_on_update() {
        use crate::db::Database;

        let temp_dir = TempDir::new()
            .expect("failed to create temp directory - check disk space and permissions");
        let root = temp_dir.path();

        // Create file
        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create test directory - check disk space and permissions");
        let file_path = project_dir.join("file.txt");
        let mut file = File::create(&file_path)
            .expect("failed to create test file - check disk space and permissions");
        file.write_all(b"initial content")
            .expect("failed to write test data - disk may be full");
        file.sync_all()
            .expect("failed to sync file - check filesystem health");

        // Create database and do first scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).expect("failed to open database - check permissions");
        let scanner = Scanner::new();

        let _ = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
            .await
            .expect("first scan failed");

        // Get the directory and file from first scan
        let dirs = db
            .list_directories(None)
            .expect("failed to list directories");
        let files_before = db
            .list_files_by_directory(dirs[0].id)
            .expect("failed to list files");
        let tracked_since_original = files_before[0]
            .tracked_since
            .expect("tracked_since should be set after first scan");

        // Wait to ensure timestamp changes
        std::thread::sleep(std::time::Duration::from_secs(1));

        // Modify file (update mtime and size)
        let mut file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&file_path)
            .expect("failed to reopen file for modification");
        file.write_all(b"updated content with more bytes")
            .expect("failed to write updated content");
        file.sync_all().expect("failed to sync updated file");

        // Do second scan
        let _ = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
            .await
            .expect("second scan failed");

        // Verify tracked_since was NOT changed by the update
        let files_after = db
            .list_files_by_directory(dirs[0].id)
            .expect("failed to list files after second scan");
        assert_eq!(
            files_after[0].tracked_since,
            Some(tracked_since_original),
            "tracked_since should be preserved on file updates"
        );

        // Verify mtime and size were updated
        assert_ne!(
            files_after[0].mtime, files_before[0].mtime,
            "mtime should be updated on file modification"
        );
        assert_ne!(
            files_after[0].size_bytes, files_before[0].size_bytes,
            "size_bytes should be updated on file modification"
        );
    }

    #[tokio::test]
    async fn directory_oldest_mtime_uses_effective_timestamp() {
        use crate::db::Database;

        let temp_dir = TempDir::new()
            .expect("failed to create temp directory - check disk space and permissions");
        let root = temp_dir.path();

        // Create directory with an old file
        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create test directory - check disk space and permissions");
        let file_path = project_dir.join("old_file.txt");
        let mut file = File::create(&file_path)
            .expect("failed to create test file - check disk space and permissions");
        file.write_all(b"old file")
            .expect("failed to write test data - disk may be full");
        file.sync_all()
            .expect("failed to sync file - check filesystem health");

        // Set mtime to 200 days ago
        let two_hundred_days_ago = jiff::Timestamp::now()
            .checked_sub(jiff::SignedDuration::from_secs(200 * SECS_PER_DAY))
            .expect("timestamp arithmetic failed");
        #[cfg(unix)]
        {
            use filetime::FileTime;
            filetime::set_file_mtime(
                &file_path,
                FileTime::from_unix_time(two_hundred_days_ago.as_second(), 0),
            )
            .expect("failed to set file mtime - check permissions");
        }

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).expect("failed to open database - check permissions");
        let scanner = Scanner::new();

        #[cfg(not(unix))]
        let before_scan = jiff::Timestamp::now().as_second();

        let _ = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
            .await
            .expect("scan failed - check permissions");

        // Query directory
        let dirs = db
            .list_directories(None)
            .expect("failed to list directories");
        assert_eq!(dirs.len(), 1);

        // Verify directory's oldest_mtime is the effective timestamp (tracked_since, not mtime)
        let oldest_mtime = dirs[0].oldest_mtime.expect("oldest_mtime should be set");

        #[cfg(unix)]
        {
            // oldest_mtime should be close to current time (tracked_since), not 200 days ago (mtime)
            let now = jiff::Timestamp::now().as_second();
            let age_days = (now - oldest_mtime) / SECS_PER_DAY;
            assert!(
                age_days < 1,
                "oldest_mtime should be current time (tracked_since), not old mtime. Age: {age_days} days"
            );
        }

        // On platforms where we can't set mtime, just verify it's close to scan time
        #[cfg(not(unix))]
        {
            let after_scan = jiff::Timestamp::now().as_second();
            assert!(
                oldest_mtime >= before_scan && oldest_mtime <= after_scan,
                "oldest_mtime should be within scan time range"
            );
        }
    }

    #[tokio::test]
    async fn expiration_calculation_gives_full_period_for_old_files() {
        use crate::db::Database;

        let temp_dir = TempDir::new()
            .expect("failed to create temp directory - check disk space and permissions");
        let root = temp_dir.path();

        // Create directory with a very old file (500 days)
        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create test directory - check disk space and permissions");
        let file_path = project_dir.join("ancient_file.txt");
        let mut file = File::create(&file_path)
            .expect("failed to create test file - check disk space and permissions");
        file.write_all(b"ancient data")
            .expect("failed to write test data - disk may be full");
        file.sync_all()
            .expect("failed to sync file - check filesystem health");

        // Set mtime to 500 days ago
        let five_hundred_days_ago = jiff::Timestamp::now()
            .checked_sub(jiff::SignedDuration::from_secs(500 * SECS_PER_DAY))
            .expect("timestamp arithmetic failed");
        #[cfg(unix)]
        {
            use filetime::FileTime;
            filetime::set_file_mtime(
                &file_path,
                FileTime::from_unix_time(five_hundred_days_ago.as_second(), 0),
            )
            .expect("failed to set file mtime - check permissions");
        }

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).expect("failed to open database - check permissions");
        let scanner = Scanner::new();

        let _ = scan_and_persist(&db, &scanner, std::slice::from_ref(&project_dir), 90, 14)
            .await
            .expect("scan failed - check permissions");

        // Query directory
        let dirs = db
            .list_directories(None)
            .expect("failed to list directories");
        assert_eq!(dirs.len(), 1);

        // Calculate expiration
        let oldest_mtime = dirs[0].oldest_mtime.expect("oldest_mtime should be set");
        let days_remaining = calculate_expiration(oldest_mtime, 90);

        // Should have ~90 days remaining (not negative), because expiration is based on
        // tracked_since (current time), not the file's ancient mtime
        #[cfg(unix)]
        {
            assert!(
                (89..=90).contains(&days_remaining),
                "newly tracked old file should have full expiration period ({days_remaining} days remaining)"
            );
        }

        // On non-Unix platforms, just verify it's not overdue
        #[cfg(not(unix))]
        {
            assert!(
                days_remaining > 0,
                "newly tracked file should not be overdue"
            );
        }
    }

    #[test]
    fn recalculate_directory_oldest_mtime_uses_max_of_mtime_and_tracked_since() {
        use crate::db::Database;

        let temp_file =
            tempfile::NamedTempFile::new().expect("failed to create temp file - check disk space");
        let db =
            Database::open(temp_file.path()).expect("failed to open database - check permissions");

        // Create a directory
        let dir_id = db
            .insert_or_update_directory("/test", 100, 2, None, 1_700_000_000)
            .expect("failed to insert directory");

        // Insert two files:
        // File 1: old mtime (100 days ago), recent tracked_since (now)
        // File 2: recent mtime (now), NULL tracked_since (legacy)
        let now = jiff::Timestamp::now().as_second();
        let hundred_days_ago = now - (100 * SECS_PER_DAY);

        // File 1: Manually insert with old mtime but recent tracked_since
        db.conn()
            .execute(
                "INSERT INTO files (directory_id, path, size_bytes, mtime, tracked_since) VALUES (?1, ?2, ?3, ?4, ?5)",
                (dir_id, "/test/old_file.txt", 50, hundred_days_ago, now),
            )
            .expect("failed to insert file 1");

        // File 2: Manually insert with recent mtime but NULL tracked_since (legacy file)
        db.conn()
            .execute(
                "INSERT INTO files (directory_id, path, size_bytes, mtime, tracked_since) VALUES (?1, ?2, ?3, ?4, ?5)",
                (dir_id, "/test/new_file.txt", 50, now, rusqlite::types::Value::Null),
            )
            .expect("failed to insert file 2");

        // Recalculate oldest_mtime
        super::recalculate_directory_oldest_mtime(&db, dir_id)
            .expect("recalculate failed - check database connection");

        // Query the directory
        let dir = db
            .conn()
            .query_row(
                "SELECT oldest_mtime FROM directories WHERE id = ?1",
                [dir_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .expect("failed to query directory - check database connection");

        let oldest_mtime = dir.expect("oldest_mtime should be set");

        // For File 1: max(old_mtime, recent_tracked_since) = recent_tracked_since = now
        // For File 2: max(recent_mtime, NULL) = recent_mtime = now
        // MIN(now, now) = now
        // So oldest_mtime should be approximately now
        let age_seconds = now - oldest_mtime;
        assert!(
            age_seconds < 2,
            "oldest_mtime should be recent (age: {age_seconds} seconds)"
        );
    }

    #[test]
    fn recalculate_directory_oldest_mtime_handles_null_tracked_since() {
        use crate::db::Database;

        let temp_file =
            tempfile::NamedTempFile::new().expect("failed to create temp file - check disk space");
        let db =
            Database::open(temp_file.path()).expect("failed to open database - check permissions");

        // Create a directory
        let dir_id = db
            .insert_or_update_directory("/test", 100, 1, None, 1_700_000_000)
            .expect("failed to insert directory");

        // Insert a legacy file with NULL tracked_since
        let now = jiff::Timestamp::now().as_second();
        let fifty_days_ago = now - (50 * SECS_PER_DAY);

        db.conn()
            .execute(
                "INSERT INTO files (directory_id, path, size_bytes, mtime, tracked_since) VALUES (?1, ?2, ?3, ?4, ?5)",
                (dir_id, "/test/legacy_file.txt", 100, fifty_days_ago, rusqlite::types::Value::Null),
            )
            .expect("failed to insert legacy file");

        // Recalculate oldest_mtime
        super::recalculate_directory_oldest_mtime(&db, dir_id)
            .expect("recalculate failed - check database connection");

        // Query the directory
        let dir = db
            .conn()
            .query_row(
                "SELECT oldest_mtime FROM directories WHERE id = ?1",
                [dir_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .expect("failed to query directory - check database connection");

        let oldest_mtime = dir.expect("oldest_mtime should be set");

        // For legacy file with NULL tracked_since, COALESCE(tracked_since, mtime) = mtime
        // So oldest_mtime should be fifty_days_ago
        assert_eq!(
            oldest_mtime, fifty_days_ago,
            "legacy files should use mtime when tracked_since is NULL"
        );
    }
}
