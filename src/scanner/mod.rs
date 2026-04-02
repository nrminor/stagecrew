//! Filesystem scanning logic.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use jwalk::WalkDir;

use crate::audit::{AuditAction, AuditActorSource, AuditEvent, AuditService};
use crate::config::{AppConfig, Config};
use crate::db::{Database, Root};
use crate::error::{Error, Result};

/// Seconds in a 24-hour day (not calendar-aware).
const SECS_PER_DAY: i64 = 86400;

/// Calculate days remaining until expiration based on countdown start time.
///
/// Calculates the number of days remaining until a file expires, based on its
/// countdown start timestamp and the configured expiration period. Returns a
/// negative value if the file is already expired.
///
/// The countdown start is typically set when a file is first tracked, and can
/// be reset by user action to give files a fresh expiration period.
///
/// # Arguments
///
/// * `countdown_start` - Unix timestamp when the expiration countdown began
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
/// // Countdown started 30 days ago, expires in 90 days
/// const SECS_PER_DAY: i64 = 86400;
/// let now = Timestamp::now();
/// let thirty_days_ago = now.checked_sub(jiff::SignedDuration::from_secs(30 * SECS_PER_DAY)).unwrap();
/// // In real code: let days_remaining = calculate_expiration(thirty_days_ago.as_second(), 90);
/// // assert!(days_remaining > 59 && days_remaining <= 60);
/// ```
#[must_use = "expiration calculation result should be used"]
pub fn calculate_expiration(countdown_start: i64, expiration_days: u32) -> i64 {
    let now = jiff::Timestamp::now();
    let start_ts = jiff::Timestamp::from_second(countdown_start).unwrap_or(now);

    // Calculate expiration timestamp (days as 24-hour periods)
    let expiration_secs = i64::from(expiration_days) * SECS_PER_DAY;
    let expiration_duration = jiff::SignedDuration::from_secs(expiration_secs);
    let expires_at = start_ts.checked_add(expiration_duration).unwrap_or(now);

    // Calculate days remaining (using 86400-second days)
    let duration_remaining = expires_at.duration_since(now);
    duration_remaining.as_secs() / SECS_PER_DAY
}

/// Transition entries based on expiration and deferral status using per-root config.
///
/// This function implements the core business logic for the removal-by-default
/// policy. It processes file entries in the database and transitions them between
/// states based on their expiration status, using per-root configuration for
/// expiration periods and auto-remove settings.
///
/// - **Tracked files**: If expired, transition to `pending` (or `approved` if `auto_remove` is enabled for that root)
/// - **Deferred files**: If the deferral period has ended, reset status to `tracked` and clear `deferred_until`
/// - **Ignored files**: Never transitioned (permanent exemption)
///
/// Note: Only files (not directories) are subject to expiration.
///
/// This function is typically called after a scan to update the workflow state.
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
/// # use stagecrew::config::{AppConfig, Config};
/// // In real code:
/// // let db = Database::open(Path::new("test.db"))?;
/// // let app_config = AppConfig::from_global(Config::default());
/// // let summary = transition_expired_paths(&db, &app_config)?;
/// // println!("Transitioned {} to pending, {} reset from deferred",
/// //          summary.expired_to_pending, summary.deferred_reset);
/// ```
#[must_use = "transition summary should be logged or displayed"]
pub fn transition_expired_paths(
    db: &Database,
    app_config: &AppConfig,
) -> Result<TransitionSummary> {
    let mut expired_to_pending = 0u64;
    let mut expired_to_approved = 0u64;
    let mut deferred_reset = 0u64;

    let now = jiff::Timestamp::now().as_second();

    // Build root_id -> Config lookup
    let roots = db.list_roots()?;
    let root_configs: HashMap<i64, &Config> = roots
        .iter()
        .map(|r| (r.id, app_config.for_root(&r.path)))
        .collect();

    // Get all file entries (is_dir = 0) with status 'tracked' or 'deferred'
    let conn = db.conn();
    let mut stmt = conn.prepare(
        "SELECT id, root_id, path, countdown_start, status, deferred_until
         FROM entries
         WHERE is_dir = 0 AND status IN ('tracked', 'deferred')",
    )?;

    let mut rows = stmt.query([])?;
    let mut transitions = Vec::new();

    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let root_id: i64 = row.get(1)?;
        let path = PathBuf::from(row.get::<_, String>(2)?);
        let countdown_start: Option<i64> = row.get(3)?;
        let status: String = row.get(4)?;
        let deferred_until: Option<i64> = row.get(5)?;

        // Get config for this entry's root (fall back to global if root not found)
        let config = root_configs
            .get(&root_id)
            .copied()
            .unwrap_or(&app_config.global);

        // Handle deferred entries (deferral reset is config-independent)
        if status == "deferred"
            && let Some(deferred_until_ts) = deferred_until
            && now >= deferred_until_ts
        {
            // Deferral period ended, reset to tracked
            transitions.push((id, path, "tracked".to_string(), true));
            deferred_reset += 1;
            continue;
        }

        // Handle tracked entries (check expiration using per-root config)
        if status == "tracked"
            && let Some(countdown_ts) = countdown_start
        {
            let days_remaining = calculate_expiration(countdown_ts, config.expiration_days);

            if days_remaining <= 0 {
                // File has expired
                let new_status = if config.auto_remove {
                    "approved"
                } else {
                    "pending"
                };
                transitions.push((id, path, new_status.to_string(), false));

                if config.auto_remove {
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
        tracing::trace!(
            entry_id = id,
            path = ?path,
            new_status = %new_status,
            is_deferral_reset,
            "Applying scanner transition"
        );

        // Update status
        if is_deferral_reset {
            // Clear deferred_until when resetting to tracked
            conn.execute(
                "UPDATE entries SET status = ?1, deferred_until = NULL, updated_at = strftime('%s', 'now') WHERE id = ?2",
                (&new_status, id),
            )?;
        } else {
            db.update_entry_status(id, &new_status)?;
        }

        record_transition_audit(
            &audit,
            &user,
            id,
            path.as_path(),
            &new_status,
            is_deferral_reset,
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
    /// Number of tracked files transitioned to pending status.
    pub expired_to_pending: u64,
    /// Number of tracked files transitioned to approved status (auto-remove).
    pub expired_to_approved: u64,
    /// Number of deferred files reset to tracked status.
    pub deferred_reset: u64,
}

/// Combined result of a full refresh operation (scan + transition).
///
/// This struct is `#[non_exhaustive]` so new fields can be added in
/// minor versions. Use `..` when destructuring to remain forward-compatible.
#[derive(Debug, Clone, Default)]
#[must_use = "refresh summary should be logged or displayed"]
#[non_exhaustive]
pub struct RefreshSummary {
    /// Results from the filesystem scan phase.
    pub scan: ScanSummary,
    /// Results from the expiration transition phase.
    pub transitions: TransitionSummary,
}

/// Refresh the database by scanning tracked paths and transitioning expired files.
///
/// This is the primary entry point for bringing the database up to date. It
/// composes two operations: scanning the filesystem to discover and upsert
/// entries, then transitioning any expired files to the appropriate status
/// (pending approval or auto-approved, depending on per-root configuration).
///
/// All call sites that need a "full refresh" should use this function rather
/// than calling `scan_and_persist` and `transition_expired_paths` separately,
/// which risks forgetting the transition step.
///
/// # Errors
///
/// Returns an error if either the scan or the transition phase fails.
///
/// # Examples
///
/// ```no_run
/// # use stagecrew::scanner::{refresh, Scanner};
/// # use stagecrew::db::Database;
/// # use stagecrew::config::{AppConfig, Config};
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let db = Database::open(std::path::Path::new("test.db"))?;
/// let scanner = Scanner::new();
/// let app_config = AppConfig::from_global(Config::default());
///
/// let summary = refresh(&db, &scanner, &app_config).await?;
/// # Ok::<(), stagecrew::error::Error>(())
/// # }).unwrap();
/// ```
pub async fn refresh(
    db: &Database,
    scanner: &Scanner,
    app_config: &AppConfig,
) -> Result<RefreshSummary> {
    let scan = scan_and_persist(db, scanner, app_config).await?;
    let transitions = transition_expired_paths(db, app_config)?;
    Ok(RefreshSummary { scan, transitions })
}

/// Scan tracked paths and persist results to the database.
///
/// This function orchestrates the full scan workflow:
/// 1. Seed config baseline paths as roots in the database
/// 2. Query all roots from the database (config baseline + user-added)
/// 3. Scan each root path using the scanner
/// 4. Upsert both directory and file entries into the database
/// 5. Update the stats table with aggregated totals and per-root expiration counts
/// 6. Record the scan action in the audit log
///
/// The database is the source of truth for which paths to scan. Config
/// `tracked_paths` are a baseline that always get seeded as roots, but
/// additional roots added via the CLI or TUI are also scanned.
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
/// # use stagecrew::config::{AppConfig, Config};
/// # use std::path::PathBuf;
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let db = Database::open(std::path::Path::new("test.db"))?;
/// let scanner = Scanner::new();
/// let app_config = AppConfig::from_global(Config::default());
///
/// scan_and_persist(&db, &scanner, &app_config).await?;
/// # Ok::<(), stagecrew::error::Error>(())
/// # }).unwrap();
/// ```
pub async fn scan_and_persist(
    db: &Database,
    scanner: &Scanner,
    app_config: &AppConfig,
) -> Result<ScanSummary> {
    let mut total_directories = 0u64;
    let mut total_files = 0u64;
    let mut total_size_bytes = 0u64;
    let scan_timestamp = jiff::Timestamp::now().as_second();

    // Seed config baseline paths as roots in the database
    for path in &app_config.global.tracked_paths {
        db.insert_root(path)?;
    }

    // Query all roots from the database (config baseline + user-added)
    let roots = db.list_roots()?;

    // Scan each root
    for root in &roots {
        let path = root.path.clone();
        let is_first_scan = root.last_scanned.is_none();
        tracing::info!(?path, is_first_scan, "Scanning path");

        let scan_result = scanner.scan(&path).await?;

        let persisted_directories =
            persist_scan_result_for_root(db, root, &scan_result, scan_timestamp)?;
        total_directories += persisted_directories;

        total_files += scan_result.total_files;
        total_size_bytes += scan_result.total_size_bytes;
    }

    // Update stats table with per-root expiration awareness
    // Allow: Total counts are realistic filesystem statistics that won't exceed i64::MAX.
    #[allow(clippy::cast_possible_wrap)]
    update_stats(db, scan_timestamp, app_config, &roots)?;

    // Record scan in audit log
    let audit = AuditService::new(db);
    let user = AuditService::current_user();
    record_scan_summary_audit(
        &audit,
        &user,
        roots.len(),
        total_directories,
        total_files,
        total_size_bytes,
    )?;

    let deduped_stats = db.get_stats()?;
    let unique_files = u64::try_from(deduped_stats.total_files.max(0))
        .map_err(|_| Error::Config("global stats file count overflow".to_string()))?;
    let unique_size_bytes = u64::try_from(deduped_stats.total_size_bytes.max(0))
        .map_err(|_| Error::Config("global stats byte count overflow".to_string()))?;

    tracing::info!(
        total_directories,
        total_files,
        total_size_bytes,
        unique_files,
        unique_size_bytes,
        "Scan complete"
    );

    Ok(ScanSummary {
        total_directories,
        total_files,
        total_size_bytes,
        unique_files,
        unique_size_bytes,
    })
}

/// Mark entries that no longer exist on disk as removed.
///
/// After a scan upserts all currently-existing entries, this pass finds any
/// active entries under the root whose paths are no longer on the filesystem
/// and sets their status to `removed`. This prevents ghost entries from
/// persisting indefinitely when files are deleted outside of stagecrew.
fn cleanup_missing_entries(
    db: &Database,
    root_id: i64,
    discovered_paths: &HashSet<PathBuf>,
) -> Result<()> {
    let existing_entries = db.list_entries_by_root(root_id)?;
    let mut missing_candidates: Vec<(i64, PathBuf)> = Vec::new();

    for entry in &existing_entries {
        if entry.status == "removed" {
            continue;
        }
        if discovered_paths.contains(&entry.path) {
            continue;
        }
        missing_candidates.push((entry.id, entry.path.clone()));
    }

    let mut cleaned = 0u64;
    for (entry_id, path) in &missing_candidates {
        if !path.exists() {
            tracing::debug!(
                path = ?path,
                entry_id,
                "Entry no longer exists on disk, marking as removed"
            );
            if let Err(e) = db.update_entry_status(*entry_id, "removed") {
                tracing::warn!("Failed to mark missing entry as removed: {e}");
            } else {
                cleaned += 1;
            }
        }
    }
    if cleaned > 0 {
        tracing::info!(root_id, cleaned, "Cleaned up missing entries");
    }
    Ok(())
}

fn persist_scan_result_for_root(
    db: &Database,
    root: &Root,
    scan_result: &ScanResult,
    scan_timestamp: i64,
) -> Result<u64> {
    let root_id = root.id;

    // Batch per-root upserts in one transaction to reduce round-trips.
    with_scan_write_transaction(db, || {
        // Upsert directory entries and file entries captured during scan.
        for dir_info in &scan_result.directories_found {
            // Determine parent_path for this directory
            let parent_path = dir_info
                .path
                .parent()
                .map_or_else(|| root.path.clone(), Path::to_path_buf);

            // Insert directory entry with oldest child mtime so directories
            // sort meaningfully by expiration alongside files.
            let dir_mtime = dir_info.oldest_mtime.map(jiff::Timestamp::as_second);
            #[allow(clippy::cast_possible_wrap)]
            db.upsert_entry_no_return(
                root_id,
                &dir_info.path,
                &parent_path,
                true,
                dir_info.size_bytes as i64,
                dir_mtime,
            )?;
        }

        for file_info in &scan_result.files_found {
            let mtime_unix = file_info.mtime.map(jiff::Timestamp::as_second);

            // Allow: size_bytes is a realistic file size that won't exceed i64::MAX.
            #[allow(clippy::cast_possible_wrap)]
            db.upsert_entry_no_return(
                root_id,
                &file_info.path,
                &file_info.parent_path,
                false,
                file_info.size_bytes as i64,
                mtime_unix,
            )?;
        }

        let ignored_updates = db.enforce_ignored_directory_inheritance(root_id)?;
        tracing::debug!(
            root_id,
            ignored_updates,
            "Applied ignored-directory inheritance during scan persistence"
        );

        Ok(())
    })?;

    cleanup_missing_entries(db, root_id, &scan_result.discovered_paths)?;

    // On first scan of a root, reset all countdowns to give files a fresh start.
    // This prevents old files from immediately appearing overdue when first tracked.
    if root.last_scanned.is_none() {
        let reset_count = db.reset_root_countdowns(root_id)?;
        tracing::info!(
            root_id,
            reset_count,
            "Reset countdowns for newly tracked root"
        );
    }

    // Update root's last_scanned timestamp
    db.update_root_last_scanned(root_id, scan_timestamp)?;

    u64::try_from(scan_result.directories_found.len()).map_err(|_| {
        Error::Config("directory count overflow while persisting scan result".to_string())
    })
}

fn with_scan_write_transaction<T>(db: &Database, f: impl FnOnce() -> Result<T>) -> Result<T> {
    db.conn().execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

    let result = f();
    match result {
        Ok(value) => {
            db.conn().execute_batch("COMMIT")?;
            Ok(value)
        }
        Err(err) => {
            if let Err(rollback_err) = db.conn().execute_batch("ROLLBACK") {
                tracing::warn!(
                    error = %rollback_err,
                    "Rollback failed after scan write transaction error"
                );
            }
            Err(err)
        }
    }
}

fn record_transition_audit(
    audit: &AuditService<'_>,
    user: &str,
    entry_id: i64,
    path: &Path,
    new_status: &str,
    is_deferral_reset: bool,
) -> Result<()> {
    let details = if is_deferral_reset {
        "Deferral period ended, reset to tracked"
    } else if new_status == "approved" {
        "Expired and auto-approved for removal"
    } else {
        "Expired, pending approval for removal"
    };

    audit.record_event(&AuditEvent {
        user,
        actor_source: AuditActorSource::Scanner,
        action: AuditAction::Scan,
        target_path: Some(path),
        details: Some(details),
        entry_id: Some(entry_id),
        root_id: None,
        status_before: Some(if is_deferral_reset {
            "deferred"
        } else {
            "tracked"
        }),
        status_after: Some(new_status),
        outcome: Some(if is_deferral_reset {
            "deferred_reset"
        } else {
            new_status
        }),
    })
}

fn record_scan_summary_audit(
    audit: &AuditService<'_>,
    user: &str,
    root_count: usize,
    total_directories: u64,
    total_files: u64,
    total_size_bytes: u64,
) -> Result<()> {
    let details = format!(
        "Scanned {root_count} paths: {total_directories} directories, {total_files} files, {total_size_bytes} bytes"
    );
    audit.record_event(&AuditEvent {
        user,
        actor_source: AuditActorSource::Scanner,
        action: AuditAction::Scan,
        target_path: None,
        details: Some(&details),
        entry_id: None,
        root_id: None,
        status_before: None,
        status_after: None,
        outcome: Some("completed"),
    })
}

/// Update the stats table with scan results using per-root expiration settings.
///
/// This updates the singleton stats row (id=1) with total counts, warning counts,
/// and timestamps. The stats table is used by the status command for fast queries.
///
/// This function calculates:
/// - `files_within_warning`: files with `days_remaining` <= `warning_days` AND > 0 AND status = 'tracked'
/// - `files_pending_approval`: files with status = 'pending'
/// - `files_overdue`: files with `days_remaining` <= 0 AND status = 'tracked'
///
/// Unlike the previous implementation that used SQL subqueries with a single global
/// expiration period, this version computes stats in Rust to support per-root
/// expiration and warning settings.
///
/// # Errors
///
/// Returns an error if database operations fail.
fn update_stats(
    db: &Database,
    scan_timestamp: i64,
    app_config: &AppConfig,
    roots: &[Root],
) -> Result<()> {
    // Build per-root expiration policy for deduplicated global stats.
    let root_configs = roots
        .iter()
        .map(|r| {
            let cfg = app_config.for_root(&r.path);
            crate::db::RootStatConfig {
                root_id: r.id,
                expiration_days: cfg.expiration_days,
                warning_days: cfg.warning_days,
            }
        })
        .collect::<Vec<_>>();

    let live_stats = db.compute_live_stats_with_root_configs(&root_configs)?;

    db.conn().execute(
        "UPDATE stats SET
            total_files = ?1,
            total_size_bytes = ?2,
            last_scan_completed = ?3,
            files_within_warning = ?4,
            files_pending_approval = ?5,
            files_overdue = ?6
         WHERE id = 1",
        (
            live_stats.total_files,
            live_stats.total_size_bytes,
            scan_timestamp,
            live_stats.files_within_warning,
            live_stats.files_pending_approval,
            live_stats.files_overdue,
        ),
    )?;

    Ok(())
}

/// Summary of a scan-and-persist operation.
// Allow: The `total_` prefix provides clarity that these are aggregate counts
// across the entire scan operation, not per-directory values.
#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_field_names)]
#[must_use = "scan summary should be logged or displayed"]
#[non_exhaustive]
pub struct ScanSummary {
    pub total_directories: u64,
    pub total_files: u64,
    pub total_size_bytes: u64,
    pub unique_files: u64,
    pub unique_size_bytes: u64,
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
    let mut files_found: Vec<ScannedFile> = Vec::new();
    let mut discovered_paths: HashSet<PathBuf> = HashSet::new();

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

        if metadata.is_dir {
            discovered_paths.insert(path.clone());
            dir_map
                .entry(path.clone())
                .or_insert_with(|| DirectoryAggregator {
                    path: path.clone(),
                    size_bytes: 0,
                    file_count: 0,
                    oldest_mtime: None,
                });
            continue;
        }

        // Only process regular files for size and file-count aggregation.
        if !metadata.is_file {
            continue;
        }

        total_files += 1;
        total_size_bytes += metadata.size_bytes;

        // Determine the parent directory for aggregation
        let parent_dir = path
            .parent()
            .map_or_else(|| root.to_path_buf(), std::path::Path::to_path_buf);

        discovered_paths.insert(path.clone());

        files_found.push(ScannedFile {
            path: path.clone(),
            parent_path: parent_dir.clone(),
            size_bytes: metadata.size_bytes,
            mtime: metadata.mtime,
        });

        // Track direct file count for the immediate parent directory.
        let parent_agg = dir_map
            .entry(parent_dir.clone())
            .or_insert_with(|| DirectoryAggregator {
                path: parent_dir.clone(),
                size_bytes: 0,
                file_count: 0,
                oldest_mtime: None,
            });
        parent_agg.file_count += 1;

        accumulate_recursive_dir_stats(
            &mut dir_map,
            &mut discovered_paths,
            parent_dir.as_path(),
            root,
            metadata.size_bytes,
            metadata.mtime,
        );
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
        files_found,
        discovered_paths,
    }
}

fn accumulate_recursive_dir_stats(
    dir_map: &mut HashMap<PathBuf, DirectoryAggregator>,
    discovered_paths: &mut HashSet<PathBuf>,
    parent_dir: &Path,
    root: &Path,
    size_bytes: u64,
    mtime: Option<jiff::Timestamp>,
) {
    let mut current = Some(parent_dir);
    while let Some(dir) = current {
        if !dir.starts_with(root) {
            break;
        }
        discovered_paths.insert(dir.to_path_buf());

        let agg = dir_map
            .entry(dir.to_path_buf())
            .or_insert_with(|| DirectoryAggregator {
                path: dir.to_path_buf(),
                size_bytes: 0,
                file_count: 0,
                oldest_mtime: None,
            });
        agg.size_bytes += size_bytes;
        if let Some(ts) = mtime {
            agg.oldest_mtime = Some(match agg.oldest_mtime {
                Some(existing) if ts < existing => ts,
                Some(existing) => existing,
                None => ts,
            });
        }

        if dir == root {
            break;
        }
        current = dir.parent();
    }
}

/// File metadata collected during scan.
struct FileMetadata {
    size_bytes: u64,
    mtime: Option<jiff::Timestamp>,
    is_file: bool,
    is_dir: bool,
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
    let is_dir = metadata.is_dir();
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
        is_dir,
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
/// total size, per-directory rollups, per-file records, and discovered paths.
#[derive(Debug, Default, Clone)]
#[must_use = "scan results should be processed"]
#[non_exhaustive]
pub struct ScanResult {
    pub total_files: u64,
    pub total_size_bytes: u64,
    pub directories_found: Vec<DirectoryInfo>,
    pub files_found: Vec<ScannedFile>,
    pub discovered_paths: HashSet<PathBuf>,
}

/// Information about a single scanned file.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ScannedFile {
    pub path: PathBuf,
    pub parent_path: PathBuf,
    pub size_bytes: u64,
    pub mtime: Option<jiff::Timestamp>,
}

/// Information about a scanned directory.
///
/// Represents aggregated metadata for all files within a directory. The `oldest_mtime`
/// field tracks the oldest modification time of any file in the directory, which is used
/// for expiration calculations.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[allow(dead_code)]
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
    use crate::config::{AppConfig, Config};
    use crate::db::Database;
    use filetime::{FileTime, set_file_mtime};
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    /// Creates a temporary database for testing.
    fn temp_database() -> (NamedTempFile, Database) {
        let temp_file = NamedTempFile::new().expect("failed to create temp file");
        let db = crate::db::Database::open(temp_file.path()).expect("failed to open database");
        (temp_file, db)
    }

    /// Creates an `AppConfig` for testing with specified expiration settings.
    fn test_app_config(expiration_days: u32, warning_days: u32, auto_remove: bool) -> AppConfig {
        AppConfig::from_global(Config {
            expiration_days,
            warning_days,
            auto_remove,
            ..Config::default()
        })
    }

    /// Creates an `AppConfig` with tracked paths for scan tests.
    fn test_app_config_with_paths(
        paths: Vec<PathBuf>,
        expiration_days: u32,
        warning_days: u32,
    ) -> AppConfig {
        AppConfig::from_global(Config {
            tracked_paths: paths,
            expiration_days,
            warning_days,
            ..Config::default()
        })
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
            result.files_found.len(),
            3,
            "Expected file records to match total files"
        );
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

        assert_eq!(root_agg.file_count, 1, "Root should have 1 direct file");
        assert_eq!(
            root_agg.size_bytes, 600,
            "Root should include recursive bytes from subdirectories"
        );

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
    async fn scanner_includes_empty_directories() {
        let temp_dir = create_test_tree();
        let root = temp_dir.path();
        let empty_dir = root.join("empty");
        fs::create_dir(&empty_dir)
            .expect("failed to create empty test directory - check permissions and disk space");

        let scanner = Scanner::new();
        let result = scanner.scan(root).await.expect(
            "scanner should successfully scan test directory - check permissions and disk space",
        );

        let empty_agg = result
            .directories_found
            .iter()
            .find(|d| d.path == empty_dir)
            .expect("empty directory should be included in scan results");
        assert_eq!(empty_agg.file_count, 0);
        assert_eq!(empty_agg.size_bytes, 0);
        assert!(empty_agg.oldest_mtime.is_none());
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
            1,
            "Expected root directory to be included"
        );

        let root_dir = result
            .directories_found
            .iter()
            .find(|d| d.path == temp_dir.path())
            .expect("Expected root directory aggregation");
        assert_eq!(root_dir.size_bytes, 0);
        assert_eq!(root_dir.file_count, 0);
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

    #[test]
    fn cleanup_missing_entries_keeps_undiscovered_paths_if_they_still_exist() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        let file_path = root.join("existing.txt");
        File::create(&file_path)
            .expect("failed to create test file - check disk space and permissions");

        let (_temp, db) = temp_database();
        let root_id = db.insert_root(root).expect("insert root");
        db.upsert_entry_no_return(root_id, &file_path, root, false, 1, Some(1_700_000_000))
            .expect("upsert entry");

        let discovered_paths = HashSet::new();
        cleanup_missing_entries(&db, root_id, &discovered_paths).expect("cleanup should succeed");

        let entry = db
            .get_entry_by_path(&file_path)
            .expect("query should succeed")
            .expect("entry should exist");
        assert_eq!(entry.status, "tracked");
    }

    #[test]
    fn cleanup_missing_entries_marks_nonexistent_paths_as_removed() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();
        let missing_path = root.join("missing.txt");

        let (_temp, db) = temp_database();
        let root_id = db.insert_root(root).expect("insert root");
        db.upsert_entry_no_return(root_id, &missing_path, root, false, 1, Some(1_700_000_000))
            .expect("upsert entry");

        let discovered_paths = HashSet::new();
        cleanup_missing_entries(&db, root_id, &discovered_paths).expect("cleanup should succeed");

        let entry = db
            .get_entry_by_path(&missing_path)
            .expect("query should succeed")
            .expect("entry should exist");
        assert_eq!(entry.status, "removed");
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

        // Insert a root and a file entry with an old mtime (100 days ago)
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/expired"),
                Path::new("/data"),
                false,
                1024,
                Some(hundred_days_ago.as_second()),
            )
            .expect("insert entry");

        // Backdate countdown_start so the entry is expired (expiration is based on countdown_start)
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE id = ?2",
                (hundred_days_ago.as_second(), entry_id),
            )
            .expect("failed to backdate countdown_start");

        // Verify initial status is 'tracked'
        let entry_before = db
            .get_entry_by_path(Path::new("/data/expired"))
            .expect("failed to query entry from database - connection may be lost")
            .expect(
                "expected entry to exist after insert - verify database persisted data correctly",
            );
        assert_eq!(entry_before.status, "tracked");

        // Run transition with 90-day expiration policy and auto_remove=false
        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 1);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status changed to 'pending'
        let entry_after = db
            .get_entry_by_path(Path::new("/data/expired"))
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after transition - verify scanner persisted data correctly");
        assert_eq!(entry_after.status, "pending");

        // Verify audit entry was created
        let audit = crate::audit::AuditService::new(&db);
        let entries = audit
            .list_by_path(Path::new("/data/expired"))
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

        // Insert a root and file entry with an old mtime
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/expired"),
                Path::new("/data"),
                false,
                1024,
                Some(hundred_days_ago.as_second()),
            )
            .expect("failed to insert test entry - database connection may be lost");

        // Backdate countdown_start so the entry is expired (expiration is based on countdown_start)
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE id = ?2",
                (hundred_days_ago.as_second(), entry_id),
            )
            .expect("failed to backdate countdown_start");

        // Run transition with auto_remove=true
        let app_config = test_app_config(90, 14, true);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 1);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status changed to 'approved'
        let entry = db
            .get_entry_by_path(Path::new("/data/expired"))
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after transition - verify scanner persisted data correctly");
        assert_eq!(entry.status, "approved");
    }

    #[test]
    fn transition_expired_paths_does_not_transition_non_expired() {
        let (_temp, db) = temp_database();

        // Insert a root and file entry with recent mtime (10 days ago)
        let now = jiff::Timestamp::now();
        let ten_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(10 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        db.upsert_entry(
            root_id,
            Path::new("/data/recent"),
            Path::new("/data"),
            false,
            1024,
            Some(ten_days_ago.as_second()),
        )
        .expect("failed to insert test entry - database connection may be lost");

        // Run transition with 90-day policy
        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let entry = db
            .get_entry_by_path(Path::new("/data/recent"))
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after transition - verify scanner persisted data correctly");
        assert_eq!(entry.status, "tracked");
    }

    #[test]
    fn transition_expired_paths_resets_expired_deferral() {
        let (_temp, db) = temp_database();

        let now = jiff::Timestamp::now();
        let yesterday = now
            .checked_sub(jiff::SignedDuration::from_secs(SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        // Insert a root and file entry with deferred status
        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/deferred"),
                Path::new("/data"),
                false,
                1024,
                None,
            )
            .expect("failed to insert test entry - database connection may be lost");

        // Set status to 'deferred' with deferred_until in the past
        db.conn()
            .execute(
                "UPDATE entries SET status = 'deferred', deferred_until = ?1 WHERE id = ?2",
                (yesterday.as_second(), entry_id),
            )
            .expect("failed to update entry status in test - database connection may be lost");

        // Verify initial state
        let entry_before = db
            .get_entry_by_path(Path::new("/data/deferred"))
            .expect("failed to query entry from database - connection may be lost")
            .expect(
                "expected entry to exist after insert - verify scanner persisted data correctly",
            );
        assert_eq!(entry_before.status, "deferred");
        assert!(entry_before.deferred_until.is_some());

        // Run transition
        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 1);

        // Verify status reset to 'tracked' and deferred_until cleared
        let entry_after = db
            .get_entry_by_path(Path::new("/data/deferred"))
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after transition - verify scanner persisted data correctly");
        assert_eq!(entry_after.status, "tracked");
        assert_eq!(entry_after.deferred_until, None);
    }

    #[test]
    fn transition_expired_paths_does_not_reset_active_deferral() {
        let (_temp, db) = temp_database();

        let now = jiff::Timestamp::now();
        let next_week = now
            .checked_add(jiff::SignedDuration::from_secs(7 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        // Insert a root and file entry with deferred status
        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/deferred"),
                Path::new("/data"),
                false,
                1024,
                None,
            )
            .expect("failed to insert test entry - database connection may be lost");

        // Set status to 'deferred' with deferred_until in the future
        db.conn()
            .execute(
                "UPDATE entries SET status = 'deferred', deferred_until = ?1 WHERE id = ?2",
                (next_week.as_second(), entry_id),
            )
            .expect("failed to update entry status in test - database connection may be lost");

        // Run transition
        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let entry = db
            .get_entry_by_path(Path::new("/data/deferred"))
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after transition - verify scanner persisted data correctly");
        assert_eq!(entry.status, "deferred");
        assert_eq!(entry.deferred_until, Some(next_week.as_second()));
    }

    #[test]
    fn transition_expired_paths_ignores_ignored_status() {
        let (_temp, db) = temp_database();

        // Insert a root and file entry with old mtime but 'ignored' status
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/ignored"),
                Path::new("/data"),
                false,
                1024,
                Some(hundred_days_ago.as_second()),
            )
            .expect("failed to insert test entry - database connection may be lost");

        // Set status to 'ignored'
        db.update_entry_status(entry_id, "ignored")
            .expect("failed to update entry status - database connection may be lost");

        // Run transition
        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let entry = db
            .get_entry_by_path(Path::new("/data/ignored"))
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after transition - verify scanner persisted data correctly");
        assert_eq!(entry.status, "ignored");
    }

    #[test]
    fn transition_expired_paths_handles_multiple_entries() {
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

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        // Expired tracked file
        let expired1_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/expired1"),
                Path::new("/data"),
                false,
                1024,
                Some(hundred_days_ago.as_second()),
            )
            .expect("failed to insert test entry - database connection may be lost");

        // Another expired tracked file
        let expired2_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/expired2"),
                Path::new("/data"),
                false,
                2048,
                Some(hundred_days_ago.as_second()),
            )
            .expect("failed to insert test entry - database connection may be lost");

        // Backdate countdown_start for expired entries (expiration is now based on countdown_start)
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE id IN (?2, ?3)",
                (hundred_days_ago.as_second(), expired1_id, expired2_id),
            )
            .expect("failed to backdate countdown_start");

        // Non-expired tracked file
        db.upsert_entry(
            root_id,
            Path::new("/data/recent"),
            Path::new("/data"),
            false,
            512,
            Some(ten_days_ago.as_second()),
        )
        .expect("failed to insert test entry - database connection may be lost");

        // Expired deferral
        let deferred_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/deferred"),
                Path::new("/data"),
                false,
                256,
                None,
            )
            .expect("failed to insert test entry - database connection may be lost");
        db.conn()
            .execute(
                "UPDATE entries SET status = 'deferred', deferred_until = ?1 WHERE id = ?2",
                (yesterday.as_second(), deferred_id),
            )
            .expect("failed to update entry status in test - database connection may be lost");

        // Run transition
        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 2);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 1);

        // Verify each entry
        assert_eq!(
            db.get_entry_by_path(Path::new("/data/expired1"))
                .expect("failed to query entry from database - connection may be lost")
                .expect("expected entry to exist after transition - verify scanner persisted data correctly")
                .status,
            "pending"
        );
        assert_eq!(
            db.get_entry_by_path(Path::new("/data/expired2"))
                .expect("failed to query entry from database - connection may be lost")
                .expect("expected entry to exist after transition - verify scanner persisted data correctly")
                .status,
            "pending"
        );
        assert_eq!(
            db.get_entry_by_path(Path::new("/data/recent"))
                .expect("failed to query entry from database - connection may be lost")
                .expect("expected entry to exist after transition - verify scanner persisted data correctly")
                .status,
            "tracked"
        );
        assert_eq!(
            db.get_entry_by_path(Path::new("/data/deferred"))
                .expect("failed to query entry from database - connection may be lost")
                .expect("expected entry to exist after transition - verify scanner persisted data correctly")
                .status,
            "tracked"
        );
    }

    #[test]
    fn transition_expired_paths_handles_entry_without_mtime() {
        let (_temp, db) = temp_database();

        // File entry with no mtime
        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        db.upsert_entry(
            root_id,
            Path::new("/data/no_mtime"),
            Path::new("/data"),
            false,
            0,
            None,
        )
        .expect("failed to insert test entry - database connection may be lost");

        // Run transition
        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        // Should not transition entries without mtime
        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);

        // Verify status unchanged
        let entry = db
            .get_entry_by_path(Path::new("/data/no_mtime"))
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after transition - verify scanner persisted data correctly");
        assert_eq!(entry.status, "tracked");
    }

    #[test]
    fn transition_expired_paths_ignores_directories() {
        let (_temp, db) = temp_database();

        // Create a directory entry (is_dir=true) - directories should not expire
        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        db.upsert_entry(
            root_id,
            Path::new("/data/subdir"),
            Path::new("/data"),
            true,
            0,
            None,
        )
        .expect("failed to insert test entry - database connection may be lost");

        // Run transition
        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        // Directory should not be transitioned
        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);
    }

    // === Additional High-Priority Tests from testing-guru Review ===

    #[test]
    fn transition_expired_paths_ignores_pending_approved_removed_blocked() {
        let (_temp, db) = temp_database();
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed for test data - check duration values");

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        // Create file entries with various statuses that should NOT be transitioned
        for (path, status) in [
            ("/data/pending", "pending"),
            ("/data/approved", "approved"),
            ("/data/removed", "removed"),
            ("/data/blocked", "blocked"),
        ] {
            let entry_id = db
                .upsert_entry(
                    root_id,
                    Path::new(path),
                    Path::new("/data"),
                    false,
                    1024,
                    Some(hundred_days_ago.as_second()),
                )
                .expect("failed to insert test entry - database connection may be lost");
            db.update_entry_status(entry_id, status)
                .expect("failed to update entry status - database connection may be lost");
        }

        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
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
            let entry = db.get_entry_by_path(Path::new(path)).expect("failed to query entry from database - connection may be lost").expect("expected entry to exist after transition - verify scanner persisted data correctly");
            assert_eq!(
                entry.status, expected_status,
                "Status for {path} should be unchanged"
            );
        }
    }

    #[test]
    fn transition_expired_paths_handles_deferred_with_null_deferred_until() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/deferred-null"),
                Path::new("/data"),
                false,
                1024,
                None,
            )
            .expect("failed to insert test entry - database connection may be lost");

        // Set status to deferred but leave deferred_until as NULL
        db.conn()
            .execute(
                "UPDATE entries SET status = 'deferred' WHERE id = ?1",
                (entry_id,),
            )
            .expect("failed to update entry status in test - database connection may be lost");

        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        // Should NOT reset because deferred_until is None
        assert_eq!(summary.deferred_reset, 0);

        let entry = db
            .get_entry_by_path(Path::new("/data/deferred-null"))
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after transition - verify scanner persisted data correctly");
        assert_eq!(entry.status, "deferred");
    }

    #[test]
    fn transition_expired_paths_handles_empty_database() {
        let (_temp, db) = temp_database();

        let app_config = test_app_config(90, 14, false);
        let summary = super::transition_expired_paths(&db, &app_config)
            .expect("failed to transition expired paths - database connection may be lost");

        assert_eq!(summary.expired_to_pending, 0);
        assert_eq!(summary.expired_to_approved, 0);
        assert_eq!(summary.deferred_reset, 0);
    }

    // === Integration Tests ===

    #[tokio::test]
    async fn scan_and_persist_creates_root_and_entries() {
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
        let app_config = test_app_config_with_paths(vec![project_dir.clone()], 90, 14);
        let summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify summary
        assert_eq!(summary.total_directories, 1);
        assert_eq!(summary.total_files, 1);
        assert_eq!(summary.total_size_bytes, 100);

        // Verify root was created
        let roots = db.list_roots().expect("failed to list roots");
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].path, project_dir);
        assert!(roots[0].last_scanned.is_some());

        // Verify directory entry was created
        let dir_entry = db
            .get_entry_by_path(&project_dir)
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected directory entry to exist after scan");
        assert!(dir_entry.is_dir);
        assert_eq!(dir_entry.size_bytes, 100);
        assert_eq!(dir_entry.status, "tracked");

        // Verify file entry was created
        let file_path = project_dir.join("file1.txt");
        let file_entry = db
            .get_entry_by_path(&file_path)
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected file entry to exist after scan");
        assert!(!file_entry.is_dir);
        assert_eq!(file_entry.size_bytes, 100);
        assert!(file_entry.mtime.is_some());
        assert!(file_entry.tracked_since.is_some());
        assert_eq!(file_entry.status, "tracked");
    }

    #[tokio::test]
    async fn scan_and_persist_creates_file_entries() {
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
        let app_config = test_app_config_with_paths(vec![project_dir.clone()], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify files were persisted as entries
        let entries = db
            .list_entries_by_parent(
                db.get_root_by_path(&project_dir)
                    .expect("failed to query root for project directory")
                    .expect("project directory should be a tracked root")
                    .id,
                &project_dir,
            )
            .expect("failed to list entries from database - connection may be lost");
        assert_eq!(entries.len(), 2, "Expected 2 file entries");

        // Entries should be ordered by path
        assert!(entries[0].path.ends_with("a.txt"));
        assert_eq!(entries[0].size_bytes, 10);
        assert!(!entries[0].is_dir);
        assert!(entries[1].path.ends_with("b.txt"));
        assert_eq!(entries[1].size_bytes, 20);
        assert!(!entries[1].is_dir);
    }

    #[tokio::test]
    async fn scan_and_persist_sets_recursive_directory_sizes() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        // Create a tree where the root directory has no direct files.
        let project_dir = root.join("project");
        let nested_dir = project_dir.join("nested");
        fs::create_dir(&project_dir)
            .expect("failed to create project directory - check disk space and permissions");
        fs::create_dir(&nested_dir)
            .expect("failed to create nested directory - check disk space and permissions");

        File::create(nested_dir.join("a.txt"))
            .expect("failed to create nested file - check disk space and permissions")
            .write_all(&[0u8; 10])
            .expect("failed to write test data to nested file - disk may be full");
        File::create(nested_dir.join("b.txt"))
            .expect("failed to create nested file - check disk space and permissions")
            .write_all(&[0u8; 20])
            .expect("failed to write test data to nested file - disk may be full");

        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let app_config = test_app_config_with_paths(vec![project_dir.clone()], 90, 14);

        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        let project_entry = db
            .get_entry_by_path(&project_dir)
            .expect("failed to query project entry")
            .expect("project directory should be persisted");
        let nested_entry = db
            .get_entry_by_path(&nested_dir)
            .expect("failed to query nested entry")
            .expect("nested directory should be persisted");

        assert!(project_entry.is_dir);
        assert!(nested_entry.is_dir);
        assert_eq!(project_entry.size_bytes, 30);
        assert_eq!(nested_entry.size_bytes, 30);
    }

    #[tokio::test]
    async fn scan_and_persist_persists_empty_subdirectories() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        let project_dir = root.join("project");
        let empty_dir = project_dir.join("empty");
        fs::create_dir(&project_dir)
            .expect("failed to create project directory - check disk space and permissions");
        fs::create_dir(&empty_dir)
            .expect("failed to create empty directory - check disk space and permissions");

        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let app_config = test_app_config_with_paths(vec![project_dir.clone()], 90, 14);

        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        let empty_entry = db
            .get_entry_by_path(&empty_dir)
            .expect("failed to query empty directory entry")
            .expect("empty directory should be persisted");
        assert!(empty_entry.is_dir);
        assert_eq!(empty_entry.size_bytes, 0);
    }

    #[tokio::test]
    async fn scan_and_persist_new_files_in_ignored_directory_stay_ignored() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        let project_dir = root.join("project");
        fs::create_dir(&project_dir)
            .expect("failed to create project directory - check disk space and permissions");

        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let app_config = test_app_config_with_paths(vec![project_dir.clone()], 90, 14);

        // Initial scan persists the directory.
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Mark the directory ignored.
        let project_entry = db
            .get_entry_by_path(&project_dir)
            .expect("failed to query project directory entry")
            .expect("project directory should be persisted");
        db.update_entry_status(project_entry.id, "ignored")
            .expect("failed to mark directory ignored");

        // Add a new file after directory ignore.
        let child_path = project_dir.join("new.txt");
        File::create(&child_path)
            .expect("failed to create child file - check permissions and disk space")
            .write_all(&[0u8; 10])
            .expect("failed to write child file - disk may be full");

        // Rescan should keep new child ignored due to ignored-directory inheritance.
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to rescan and persist - check permissions and database connection");

        let child_entry = db
            .get_entry_by_path(&child_path)
            .expect("failed to query child entry")
            .expect("child file should be persisted");
        assert_eq!(child_entry.status, "ignored");
    }

    #[tokio::test]
    async fn scan_and_persist_updates_stats_table() {
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
        let app_config = test_app_config_with_paths(vec![project_dir], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify stats were updated
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");

        assert_eq!(stats.total_files, 1, "Expected 1 tracked file");
        assert_eq!(stats.total_size_bytes, 500, "Expected 500 bytes total");
        assert!(
            stats.last_scan_completed.is_some(),
            "Expected last_scan_completed to be set"
        );
    }

    #[tokio::test]
    async fn scan_and_persist_records_audit_entry() {
        use crate::audit::AuditService;

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
        let app_config = test_app_config_with_paths(vec![project_dir], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

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
        let app_config = test_app_config_with_paths(vec![dir1.clone(), dir2.clone()], 90, 14);
        let summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify both directories were scanned
        assert_eq!(summary.total_directories, 2);
        assert_eq!(summary.total_files, 2);
        assert_eq!(summary.total_size_bytes, 300);

        // Verify both roots are in database
        let roots = db.list_roots().expect("failed to list roots");
        assert_eq!(roots.len(), 2);

        // Verify entries exist for both
        assert!(
            db.get_entry_by_path(&dir1)
                .expect("failed to query entry from database - connection may be lost")
                .is_some()
        );
        assert!(
            db.get_entry_by_path(&dir2)
                .expect("failed to query entry from database - connection may be lost")
                .is_some()
        );
    }

    #[tokio::test]
    async fn scan_and_persist_preserves_overlapping_entries_for_both_roots() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        let parent_root = root.join("staging");
        let nested_root = parent_root.join("projectA");
        fs::create_dir(&parent_root)
            .expect("failed to create parent root directory - check permissions and disk space");
        fs::create_dir(&nested_root)
            .expect("failed to create nested root directory - check permissions and disk space");

        File::create(parent_root.join("top.bin"))
            .expect("failed to create parent-root file - check permissions and disk space")
            .write_all(&[0u8; 10])
            .expect("failed to write parent-root file - disk may be full");
        let shared_file = nested_root.join("shared.bin");
        File::create(&shared_file)
            .expect("failed to create shared file - check permissions and disk space")
            .write_all(&[0u8; 20])
            .expect("failed to write shared file - disk may be full");

        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let app_config =
            test_app_config_with_paths(vec![parent_root.clone(), nested_root.clone()], 90, 14);

        let summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist overlapping roots");

        assert_eq!(
            summary.total_files, 3,
            "scan summary still counts both roots separately"
        );

        let parent_root_id = db
            .get_root_by_path(&parent_root)
            .expect("query parent root")
            .expect("parent root should exist")
            .id;
        let nested_root_id = db
            .get_root_by_path(&nested_root)
            .expect("query nested root")
            .expect("nested root should exist")
            .id;

        let parent_shared = db
            .get_entry_by_root_and_path(parent_root_id, &shared_file)
            .expect("query parent shared entry")
            .expect("parent root should retain overlapping entry");
        let nested_shared = db
            .get_entry_by_root_and_path(nested_root_id, &shared_file)
            .expect("query nested shared entry")
            .expect("nested root should retain overlapping entry");

        assert_eq!(parent_shared.root_id, parent_root_id);
        assert_eq!(nested_shared.root_id, nested_root_id);
        assert_ne!(parent_shared.id, nested_shared.id);

        let parent_entries = db
            .list_entries_by_root(parent_root_id)
            .expect("list parent-root entries");
        let nested_entries = db
            .list_entries_by_root(nested_root_id)
            .expect("list nested-root entries");

        assert!(parent_entries.iter().any(|entry| entry.path == shared_file));
        assert!(nested_entries.iter().any(|entry| entry.path == shared_file));
    }

    #[tokio::test]
    async fn scan_and_persist_marks_removed_overlap_entries_per_root() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        let parent_root = root.join("staging");
        let nested_root = parent_root.join("projectA");
        fs::create_dir(&parent_root)
            .expect("failed to create parent root directory - check permissions and disk space");
        fs::create_dir(&nested_root)
            .expect("failed to create nested root directory - check permissions and disk space");

        let shared_file = nested_root.join("shared.bin");
        File::create(&shared_file)
            .expect("failed to create shared file - check permissions and disk space")
            .write_all(&[0u8; 20])
            .expect("failed to write shared file - disk may be full");

        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let app_config =
            test_app_config_with_paths(vec![parent_root.clone(), nested_root.clone()], 90, 14);

        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to perform initial overlapping scan");

        fs::remove_file(&shared_file).expect("failed to remove shared file");

        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to rescan after removing shared file");

        let parent_root_id = db
            .get_root_by_path(&parent_root)
            .expect("query parent root")
            .expect("parent root should exist")
            .id;
        let nested_root_id = db
            .get_root_by_path(&nested_root)
            .expect("query nested root")
            .expect("nested root should exist")
            .id;

        let parent_shared = db
            .get_entry_by_root_and_path(parent_root_id, &shared_file)
            .expect("query parent shared entry")
            .expect("parent root entry should still exist for auditability");
        let nested_shared = db
            .get_entry_by_root_and_path(nested_root_id, &shared_file)
            .expect("query nested shared entry")
            .expect("nested root entry should still exist for auditability");

        assert_eq!(parent_shared.status, "removed");
        assert_eq!(nested_shared.status, "removed");
    }

    #[tokio::test]
    async fn scan_and_persist_updates_global_stats_with_deduped_overlap_totals() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        let parent_root = root.join("staging");
        let nested_root = parent_root.join("projectA");
        fs::create_dir(&parent_root)
            .expect("failed to create parent root directory - check permissions and disk space");
        fs::create_dir(&nested_root)
            .expect("failed to create nested root directory - check permissions and disk space");

        File::create(parent_root.join("top.bin"))
            .expect("failed to create parent-root file - check permissions and disk space")
            .write_all(&[0u8; 10])
            .expect("failed to write parent-root file - disk may be full");
        File::create(nested_root.join("shared.bin"))
            .expect("failed to create shared file - check permissions and disk space")
            .write_all(&[0u8; 20])
            .expect("failed to write shared file - disk may be full");

        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let app_config =
            test_app_config_with_paths(vec![parent_root.clone(), nested_root.clone()], 90, 14);

        let summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist overlapping roots");
        assert_eq!(
            summary.total_files, 3,
            "scan summary remains root-summed for now"
        );

        let stats = db
            .get_stats()
            .expect("failed to query deduped stats after overlapping scan");
        assert_eq!(
            stats.total_files, 2,
            "global stats should dedupe overlapping files"
        );
        assert_eq!(
            stats.total_size_bytes, 30,
            "global bytes should dedupe overlapping files"
        );
    }

    #[tokio::test]
    async fn scan_and_persist_ignored_inheritance_stays_root_local_for_overlap() {
        let temp_dir = TempDir::new().expect(
            "failed to create temp directory for scanner test - check disk space and permissions",
        );
        let root = temp_dir.path();

        let parent_root = root.join("staging");
        let nested_root = parent_root.join("projectA");
        fs::create_dir(&parent_root)
            .expect("failed to create parent root directory - check permissions and disk space");
        fs::create_dir(&nested_root)
            .expect("failed to create nested root directory - check permissions and disk space");

        let shared_file = nested_root.join("shared.bin");
        File::create(&shared_file)
            .expect("failed to create shared file - check permissions and disk space")
            .write_all(&[0u8; 20])
            .expect("failed to write shared file - disk may be full");

        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        let scanner = Scanner::new();
        let app_config =
            test_app_config_with_paths(vec![parent_root.clone(), nested_root.clone()], 90, 14);

        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to perform initial overlapping scan");

        let nested_root_id = db
            .get_root_by_path(&nested_root)
            .expect("query nested root")
            .expect("nested root should exist")
            .id;
        let parent_root_id = db
            .get_root_by_path(&parent_root)
            .expect("query parent root")
            .expect("parent root should exist")
            .id;

        let nested_dir_entry = db
            .get_entry_by_root_and_path(nested_root_id, &nested_root)
            .expect("query nested root directory entry")
            .expect("nested root directory should be persisted");
        db.update_entry_status(nested_dir_entry.id, "ignored")
            .expect("mark nested root directory ignored");

        let new_child = nested_root.join("new.bin");
        File::create(&new_child)
            .expect("failed to create new overlapping child file")
            .write_all(&[0u8; 5])
            .expect("failed to write new overlapping child file");

        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to rescan overlapping roots after ignore");

        let nested_child = db
            .get_entry_by_root_and_path(nested_root_id, &new_child)
            .expect("query nested child entry")
            .expect("nested child should exist");
        let parent_child = db
            .get_entry_by_root_and_path(parent_root_id, &new_child)
            .expect("query parent child entry")
            .expect("parent child should exist");

        assert_eq!(nested_child.status, "ignored");
        assert_eq!(parent_child.status, "tracked");
    }

    #[tokio::test]
    async fn scan_and_persist_upserts_existing_entries() {
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
        let app_config = test_app_config_with_paths(vec![project_dir.clone()], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Change entry status manually (simulating user action)
        let file_path = project_dir.join("file1.txt");
        let entry = db
            .get_entry_by_path(&file_path)
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after scan");
        db.update_entry_status(entry.id, "approved")
            .expect("failed to update entry status - database connection may be lost");

        // Add a new file
        File::create(project_dir.join("file2.txt"))
            .expect("failed to create test file - check disk space and permissions")
            .write_all(&[0u8; 50])
            .expect("failed to write test data to file - disk may be full");

        // Scan again
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify entry was updated but status preserved
        let updated_entry = db
            .get_entry_by_path(&file_path)
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after scan");

        assert_eq!(updated_entry.id, entry.id, "ID should not change");
        assert_eq!(
            updated_entry.status, "approved",
            "Status should be preserved"
        );
    }

    // === Stats Update Tests ===

    #[tokio::test]
    async fn stats_update_calculates_files_within_warning() {
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
        let app_config = test_app_config_with_paths(vec![warning_dir.clone()], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Manually set countdown_start to 80 days ago to simulate a file
        // that has been counting down for 80 days. This puts it in the warning
        // period (10 days remaining, within 14-day warning).
        let now = jiff::Timestamp::now();
        let eighty_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(80 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed");
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE is_dir = 0",
                (eighty_days_ago.as_second(),),
            )
            .expect("failed to update countdown_start for test");

        // Scan again to update stats with the modified countdown_start
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify stats were calculated correctly
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
        assert_eq!(stats.total_files, 1);
        assert_eq!(
            stats.files_within_warning, 1,
            "File tracked for 80 days should be in warning period (10 days remaining, within 14-day warning)"
        );
        assert_eq!(stats.files_pending_approval, 0);
        assert_eq!(stats.files_overdue, 0);
    }

    #[tokio::test]
    async fn stats_update_calculates_files_pending_approval() {
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
        let app_config = test_app_config_with_paths(vec![pending_dir.clone()], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Manually set status to 'pending'
        let file_path = pending_dir.join("file.txt");
        let entry = db
            .get_entry_by_path(&file_path)
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after scan");
        db.update_entry_status(entry.id, "pending")
            .expect("failed to update entry status - database connection may be lost");

        // Scan again to update stats
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify stats
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
        assert_eq!(stats.files_pending_approval, 1);
    }

    #[tokio::test]
    async fn stats_update_calculates_files_overdue() {
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
        let app_config = test_app_config_with_paths(vec![overdue_dir.clone()], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Manually set countdown_start to 100 days ago to simulate a file
        // that has been counting down for 100 days (past the 90-day expiration).
        let now = jiff::Timestamp::now();
        let hundred_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(100 * SECS_PER_DAY))
            .expect("timestamp arithmetic should succeed");
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE is_dir = 0",
                (hundred_days_ago.as_second(),),
            )
            .expect("failed to update countdown_start for test");

        // Scan again to update stats with the modified countdown_start
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify stats - should have 1 overdue file (status is still 'tracked')
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
        assert_eq!(stats.total_files, 1);
        assert_eq!(
            stats.files_overdue, 1,
            "File tracked for 100 days should be overdue"
        );
        assert_eq!(stats.files_pending_approval, 0);
        assert_eq!(stats.files_within_warning, 0);
    }

    #[tokio::test]
    async fn stats_update_handles_mixed_scenarios() {
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
        let app_config = test_app_config_with_paths(
            vec![safe_dir.clone(), warning_dir.clone(), overdue_dir.clone()],
            90,
            14,
        );
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Backdate countdown_start for the overdue file to simulate
        // a file that has been counting down for 100 days (past the 90-day expiration).
        let overdue_file_path = overdue_dir.join("overdue.txt");
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE path = ?2",
                (
                    hundred_days_ago.as_second(),
                    overdue_file_path.to_string_lossy().as_ref(),
                ),
            )
            .expect("failed to backdate countdown_start");

        // Mark warning file as 'pending'
        let warning_file_path = warning_dir.join("warning.txt");
        let entry = db
            .get_entry_by_path(&warning_file_path)
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after scan");
        db.update_entry_status(entry.id, "pending")
            .expect("failed to update entry status - database connection may be lost");

        // Scan again to update stats
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify stats
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
        assert_eq!(stats.total_files, 3);
        assert_eq!(stats.files_overdue, 1, "One overdue file");
        assert_eq!(stats.files_pending_approval, 1, "One pending file");
        // Note: warning file is now 'pending', so files_within_warning should be 0
        assert_eq!(
            stats.files_within_warning, 0,
            "Warning file was marked pending"
        );
    }

    #[tokio::test]
    async fn stats_update_excludes_ignored_from_overdue_count() {
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
        let app_config = test_app_config_with_paths(vec![ignored_dir.clone()], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Mark as ignored
        let file_path = ignored_dir.join("old.txt");
        let entry = db
            .get_entry_by_path(&file_path)
            .expect("failed to query entry from database - connection may be lost")
            .expect("expected entry to exist after scan");
        db.update_entry_status(entry.id, "ignored")
            .expect("failed to update entry status - database connection may be lost");

        // Scan again to update stats
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("failed to scan and persist - check permissions and database connection");

        // Verify stats - ignored file should NOT be counted as overdue
        let stats = db
            .get_stats()
            .expect("failed to query stats from database - connection may be lost");
        assert_eq!(
            stats.files_overdue, 0,
            "Ignored files should not be counted as overdue"
        );
    }

    #[tokio::test]
    async fn stats_update_custom_expiration_warning_periods() {
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
        let app_config = test_app_config_with_paths(vec![dir.clone()], 30, 7);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("scan_and_persist failed - check file permissions and database connection");

        // Manually set countdown_start to 25 days ago to simulate a file
        // that has been counting down for 25 days. This gives it 5 days remaining,
        // which is within the 7-day warning period.
        let now = jiff::Timestamp::now();
        let twentyfive_days_ago = now
            .checked_sub(jiff::SignedDuration::from_secs(25 * SECS_PER_DAY))
            .expect("timestamp arithmetic overflow");
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE is_dir = 0",
                (twentyfive_days_ago.as_second(),),
            )
            .expect("failed to update countdown_start for test");

        // Scan again to update stats with the modified countdown_start
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("scan_and_persist failed - check file permissions and database connection");

        // Verify stats
        let stats = db.get_stats().expect(
            "failed to query stats from database - connection may be lost or stats table corrupted",
        );
        assert_eq!(stats.total_files, 1);
        assert_eq!(
            stats.files_within_warning, 1,
            "With 30-day expiration and 7-day warning, file tracked for 25 days (5 days remaining) should be in warning"
        );
        assert_eq!(stats.files_overdue, 0);
    }

    #[tokio::test]
    async fn stats_update_handles_entries_without_mtime() {
        let temp_dir = TempDir::new().expect("failed to create temp directory for test - check disk space and system temp directory permissions");
        let root = temp_dir.path();

        // Create empty directory (no files, so no entries with mtime)
        let empty_dir = root.join("empty");
        fs::create_dir(&empty_dir).expect(
            "failed to create empty test directory - check disk space and write permissions",
        );

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to initialize database - check disk space and SQLite is functioning");
        let scanner = Scanner::new();
        let app_config = test_app_config_with_paths(vec![empty_dir], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("scan_and_persist failed on empty directory - check permissions and database connection");

        // Verify stats - entries without mtime should not be counted in warning/overdue
        let stats = db.get_stats().expect(
            "failed to query stats from database - connection may be lost or stats table corrupted",
        );
        // Note: empty directories don't get file entries
        assert_eq!(stats.total_files, 0);
        assert_eq!(stats.files_within_warning, 0);
        assert_eq!(stats.files_overdue, 0);
    }

    #[tokio::test]
    async fn stats_update_sets_last_scan_completed_timestamp() {
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
        let app_config = test_app_config_with_paths(vec![dir], 90, 14);
        let _summary = scan_and_persist(&db, &scanner, &app_config)
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
            filetime::set_file_mtime(
                &file_path,
                filetime::FileTime::from_unix_time(hundred_days_ago.as_second(), 0),
            )
            .expect("failed to set file mtime - check permissions");
        }

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).expect("failed to open database - check permissions");
        let scanner = Scanner::new();
        let app_config = test_app_config_with_paths(vec![project_dir.clone()], 90, 14);
        let before_scan = jiff::Timestamp::now().as_second();

        let _ = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("scan failed - check permissions");

        let after_scan = jiff::Timestamp::now().as_second();

        // Query the file entry
        let entry = db
            .get_entry_by_path(&file_path)
            .expect("failed to query entry")
            .expect("entry should exist");

        // Verify tracked_since was set to current time (not the old mtime)
        let tracked_since = entry
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
                entry.mtime,
                Some(hundred_days_ago.as_second()),
                "mtime should preserve file's actual modification time"
            );
        }
    }

    #[tokio::test]
    async fn scan_preserves_tracked_since_on_update() {
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
        let app_config = test_app_config_with_paths(vec![project_dir.clone()], 90, 14);

        let _ = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("first scan failed");

        // Get the entry from first scan
        let entry_before = db
            .get_entry_by_path(&file_path)
            .expect("failed to query entry")
            .expect("entry should exist");
        let tracked_since_original = entry_before
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
        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("second scan failed");

        // Verify tracked_since was NOT changed by the update
        let entry_after = db
            .get_entry_by_path(&file_path)
            .expect("failed to query entry after second scan")
            .expect("entry should exist");
        assert_eq!(
            entry_after.tracked_since,
            Some(tracked_since_original),
            "tracked_since should be preserved on file updates"
        );

        // Verify mtime and size were updated
        assert_ne!(
            entry_after.mtime, entry_before.mtime,
            "mtime should be updated on file modification"
        );
        assert_ne!(
            entry_after.size_bytes, entry_before.size_bytes,
            "size_bytes should be updated on file modification"
        );
    }

    #[tokio::test]
    async fn expiration_uses_effective_timestamp() {
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
            filetime::set_file_mtime(
                &file_path,
                filetime::FileTime::from_unix_time(two_hundred_days_ago.as_second(), 0),
            )
            .expect("failed to set file mtime - check permissions");
        }

        // Create database and scan
        let db_path = root.join("test.db");
        let db = Database::open(&db_path).expect("failed to open database - check permissions");
        let scanner = Scanner::new();
        let app_config = test_app_config_with_paths(vec![project_dir.clone()], 90, 14);

        let _summary = scan_and_persist(&db, &scanner, &app_config)
            .await
            .expect("scan failed - check permissions");

        // Query entry
        let entry = db
            .get_entry_by_path(&file_path)
            .expect("failed to query entry")
            .expect("entry should exist");

        // Calculate effective mtime (max of mtime and tracked_since)
        let mtime = entry.mtime.expect("mtime should be set");
        let tracked_since = entry.tracked_since.expect("tracked_since should be set");
        let effective_mtime = std::cmp::max(mtime, tracked_since);

        // Calculate expiration using effective mtime
        let days_remaining = calculate_expiration(effective_mtime, 90);

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
}
