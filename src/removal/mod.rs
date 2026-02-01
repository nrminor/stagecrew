//! File removal logic and approval workflow.

use std::path::Path;

use crate::audit::{AuditAction, AuditService};
use crate::db::Database;
use crate::error::{Error, Result};

/// Handles file and directory removal with safety checks.
pub struct RemovalService {
    dry_run: bool,
}

impl RemovalService {
    /// Create a new removal service.
    #[must_use]
    pub fn new(dry_run: bool) -> Self {
        Self { dry_run }
    }

    /// Attempt to remove a path (file or directory).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Permission is denied
    /// - Path doesn't exist
    /// - Other filesystem errors occur
    pub fn remove(&self, path: &Path) -> Result<RemovalOutcome> {
        if !path.exists() {
            return Err(Error::PathNotFound(path.to_path_buf()));
        }

        if self.dry_run {
            tracing::info!(?path, "Dry run: would remove");
            return Ok(RemovalOutcome::DryRun);
        }

        let result = if path.is_dir() {
            std::fs::remove_dir_all(path)
        } else {
            std::fs::remove_file(path)
        };

        match result {
            Ok(()) => {
                tracing::info!(?path, "Removed successfully");
                Ok(RemovalOutcome::Removed)
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                tracing::warn!(?path, "Permission denied");
                Err(Error::PermissionDenied(path.to_path_buf()))
            }
            Err(e) => Err(Error::Filesystem {
                path: path.to_path_buf(),
                source: e,
            }),
        }
    }
}

/// Outcome of a removal attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "removal outcome should be checked"]
#[non_exhaustive]
pub enum RemovalOutcome {
    /// File was removed.
    Removed,
    /// Dry run mode - no actual removal.
    DryRun,
}

/// Process all approved directories for removal.
///
/// Queries the database for directories with status='approved', attempts to
/// remove each one, and updates the database with the outcome. On success,
/// the directory status is set to 'removed'. On error (permission denied or
/// other filesystem failure), the status is set to 'blocked' with details
/// logged in the audit trail.
///
/// This function never uses elevated permissions (no sudo). It operates with
/// the current user's permissions and handles errors gracefully.
///
/// # Returns
///
/// A summary containing the number of successfully removed directories,
/// blocked directories (due to errors), and total bytes freed.
///
/// # Errors
///
/// Returns an error if database operations fail. Individual removal failures
/// are handled gracefully and recorded in the summary (not propagated).
pub fn remove_approved(db: &Database) -> Result<RemovalSummary> {
    let audit = AuditService::new(db);
    let user = AuditService::current_user();
    let service = RemovalService::new(false);

    // Query all approved directories
    let approved_dirs = db.list_directories(Some("approved"))?;

    let mut removed_count = 0;
    let mut blocked_count = 0;
    let mut total_bytes_freed = 0i64;

    for dir in approved_dirs {
        let path = std::path::PathBuf::from(&dir.path);

        tracing::info!(path = ?dir.path, "Processing approved directory for removal");

        match service.remove(&path) {
            Ok(RemovalOutcome::Removed) => {
                // Success: Update status to removed
                db.update_directory_status(dir.id, "removed")?;
                removed_count += 1;
                total_bytes_freed += dir.size_bytes;

                tracing::info!(path = ?dir.path, bytes = dir.size_bytes, "Directory removed successfully");

                // Record audit entry
                audit.record(
                    &user,
                    AuditAction::Remove,
                    Some(&dir.path),
                    Some(&format!("Removed {} bytes", dir.size_bytes)),
                    Some(dir.id),
                )?;
            }
            Ok(RemovalOutcome::DryRun) => {
                // This shouldn't happen in production (dry_run=false above)
                tracing::warn!(path = ?dir.path, "Unexpected dry run outcome");
            }
            Err(Error::PermissionDenied(_)) => {
                // Permission error: Update status to blocked
                db.update_directory_status(dir.id, "blocked")?;
                blocked_count += 1;

                tracing::warn!(path = ?dir.path, "Removal blocked: permission denied");

                // Record audit entry with error details
                audit.record(
                    &user,
                    AuditAction::Remove,
                    Some(&dir.path),
                    Some("Blocked: permission denied"),
                    Some(dir.id),
                )?;
            }
            Err(e) => {
                // Other error: Update status to blocked
                db.update_directory_status(dir.id, "blocked")?;
                blocked_count += 1;

                tracing::warn!(path = ?dir.path, error = %e, "Removal blocked: filesystem error");

                // Record audit entry with error details
                audit.record(
                    &user,
                    AuditAction::Remove,
                    Some(&dir.path),
                    Some(&format!("Blocked: {e}")),
                    Some(dir.id),
                )?;
            }
        }
    }

    Ok(RemovalSummary {
        removed_count,
        blocked_count,
        total_bytes_freed,
    })
}

/// Summary of removal operations.
///
/// This struct provides read-only access to removal statistics. It cannot be
/// constructed directly by external code; instances are returned by
/// [`remove_approved`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
#[non_exhaustive]
pub struct RemovalSummary {
    removed_count: usize,
    blocked_count: usize,
    total_bytes_freed: i64,
}

impl RemovalSummary {
    /// Number of directories successfully removed.
    #[must_use]
    pub const fn removed_count(&self) -> usize {
        self.removed_count
    }

    /// Number of directories that could not be removed (blocked).
    #[must_use]
    pub const fn blocked_count(&self) -> usize {
        self.blocked_count
    }

    /// Total bytes freed from successful removals.
    #[must_use]
    pub const fn total_bytes_freed(&self) -> i64 {
        self.total_bytes_freed
    }

    /// Create an empty summary with all counts at zero.
    ///
    /// Primarily useful for testing or initialization.
    // Allow: Used in tests for creating empty summaries. Part of the public API
    // for testing and initialization scenarios.
    #[allow(dead_code)]
    pub const fn empty() -> Self {
        Self {
            removed_count: 0,
            blocked_count: 0,
            total_bytes_freed: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::AuditService;
    use crate::db::Database;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    /// Helper to create a temporary database for testing.
    fn temp_database() -> (Database, TempDir) {
        let temp_dir = TempDir::with_prefix("stagecrew-removal-test-").expect(
            "failed to create temp directory for removal test - check disk space and permissions",
        );
        let db_path = temp_dir.path().join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
        (db, temp_dir)
    }

    /// Helper to create a directory with test files.
    fn create_test_directory(root: &Path, name: &str, file_count: usize) -> (String, i64) {
        let dir_path = root.join(name);
        fs::create_dir(&dir_path)
            .expect("failed to create test directory structure - check disk space and permissions");

        let mut total_size = 0i64;
        for i in 0..file_count {
            let file_path = dir_path.join(format!("file{i}.txt"));
            let content = format!("Test content {i}");
            fs::write(&file_path, &content)
                .expect("failed to write test data to file - disk may be full");
            // Allow: content.len() is small test data, will never exceed i64::MAX.
            // In production, file sizes come from fs::metadata which returns u64.
            #[allow(clippy::cast_possible_wrap)]
            {
                total_size += content.len() as i64;
            }
        }

        (dir_path.to_string_lossy().to_string(), total_size)
    }

    #[test]
    fn remove_approved_processes_approved_directories() {
        let (db, _temp_dir) = temp_database();
        let test_root = TempDir::with_prefix("stagecrew-removal-files-").expect(
            "failed to create temp directory for removal test - check disk space and permissions",
        );

        // Create two test directories with files
        let (dir1_path, dir1_size) = create_test_directory(test_root.path(), "dir1", 3);
        let (dir2_path, dir2_size) = create_test_directory(test_root.path(), "dir2", 2);

        // Insert directories into database
        let now = jiff::Timestamp::now().as_second();
        let dir1_id = db
            .insert_or_update_directory(&dir1_path, dir1_size, 3, Some(now), now)
            .expect("failed to insert test directory - database connection may be lost");
        let dir2_id = db
            .insert_or_update_directory(&dir2_path, dir2_size, 2, Some(now), now)
            .expect("failed to insert test directory - database connection may be lost");

        // Approve both directories
        db.update_directory_status(dir1_id, "approved")
            .expect("failed to update directory status - database connection may be lost");
        db.update_directory_status(dir2_id, "approved")
            .expect("failed to update directory status - database connection may be lost");

        // Verify directories exist
        assert!(std::path::Path::new(&dir1_path).exists());
        assert!(std::path::Path::new(&dir2_path).exists());

        // Remove approved directories
        let summary = remove_approved(&db)
            .expect("failed to remove approved directories - check permissions and disk space");

        // Verify summary
        assert_eq!(summary.removed_count(), 2, "Expected 2 directories removed");
        assert_eq!(
            summary.blocked_count(),
            0,
            "Expected no blocked directories"
        );
        assert_eq!(
            summary.total_bytes_freed(),
            dir1_size + dir2_size,
            "Expected total bytes freed to match sum of directory sizes"
        );

        // Verify directories are gone
        assert!(
            !std::path::Path::new(&dir1_path).exists(),
            "Directory should be removed"
        );
        assert!(
            !std::path::Path::new(&dir2_path).exists(),
            "Directory should be removed"
        );

        // Verify database status updated
        let dir1 = db
            .get_directory_by_path(&dir1_path)
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after insertion - verify test database is working",
            );
        assert_eq!(dir1.status, "removed", "Status should be 'removed'");

        let dir2 = db
            .get_directory_by_path(&dir2_path)
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after insertion - verify test database is working",
            );
        assert_eq!(dir2.status, "removed", "Status should be 'removed'");

        // Verify audit entries
        let audit = AuditService::new(&db);
        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
        assert_eq!(entries.len(), 2, "Expected 2 audit entries");

        for entry in &entries {
            assert_eq!(entry.action, "remove");
            assert!(entry.details.is_some());
            assert!(
                entry
                    .details
                    .as_ref()
                    .expect("expected audit entry to have details - verify audit trail is working")
                    .contains("Removed")
            );
        }
    }

    #[test]
    fn remove_approved_handles_permission_denied() {
        let (db, _temp_dir) = temp_database();
        let test_root = TempDir::with_prefix("stagecrew-removal-files-").expect(
            "failed to create temp directory for removal test - check disk space and permissions",
        );

        // Create test directory with files
        let (dir_path, dir_size) = create_test_directory(test_root.path(), "protected", 2);

        // Make directory read-only to trigger permission error
        let path = std::path::Path::new(&dir_path);
        let mut perms = fs::metadata(path)
            .expect("failed to read file permissions - check file exists and is accessible")
            .permissions();
        perms.set_mode(0o444); // Read-only
        fs::set_permissions(path, perms)
            .expect("failed to set file permissions for test - check filesystem support");

        // Insert directory into database and approve
        let now = jiff::Timestamp::now().as_second();
        let dir_id = db
            .insert_or_update_directory(&dir_path, dir_size, 2, Some(now), now)
            .expect("failed to insert test directory - database connection may be lost");
        db.update_directory_status(dir_id, "approved")
            .expect("failed to update directory status - database connection may be lost");

        // Attempt removal
        let summary = remove_approved(&db)
            .expect("failed to remove approved directories - check permissions and disk space");

        // Verify summary
        assert_eq!(
            summary.removed_count(),
            0,
            "Expected no directories removed"
        );
        assert_eq!(summary.blocked_count(), 1, "Expected 1 blocked directory");
        assert_eq!(summary.total_bytes_freed(), 0, "Expected no bytes freed");

        // Verify directory still exists
        assert!(path.exists(), "Directory should still exist");

        // Verify database status updated to blocked
        let dir = db
            .get_directory_by_path(&dir_path)
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after insertion - verify test database is working",
            );
        assert_eq!(dir.status, "blocked", "Status should be 'blocked'");

        // Verify audit entry
        let audit = AuditService::new(&db);
        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
        assert_eq!(entries.len(), 1, "Expected 1 audit entry");
        assert_eq!(entries[0].action, "remove");
        assert!(
            entries[0]
                .details
                .as_ref()
                .expect("expected audit entry to have details - verify audit trail is working")
                .contains("permission denied")
        );

        // Cleanup: restore permissions so tempdir can be removed
        let mut perms = fs::metadata(path)
            .expect("failed to read file permissions - check file exists and is accessible")
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)
            .expect("failed to set file permissions for test - check filesystem support");
    }

    #[test]
    fn remove_approved_handles_nonexistent_path() {
        let (db, _temp_dir) = temp_database();

        // Insert directory that doesn't exist on filesystem
        let dir_path = "/nonexistent/path/to/directory";
        let now = jiff::Timestamp::now().as_second();
        let dir_id = db
            .insert_or_update_directory(dir_path, 1024, 5, Some(now), now)
            .expect("failed to insert test directory - database connection may be lost");
        db.update_directory_status(dir_id, "approved")
            .expect("failed to update directory status - database connection may be lost");

        // Attempt removal
        let summary = remove_approved(&db)
            .expect("failed to remove approved directories - check permissions and disk space");

        // Verify summary
        assert_eq!(
            summary.removed_count(),
            0,
            "Expected no directories removed"
        );
        assert_eq!(summary.blocked_count(), 1, "Expected 1 blocked directory");
        assert_eq!(summary.total_bytes_freed(), 0, "Expected no bytes freed");

        // Verify database status updated to blocked
        let dir = db
            .get_directory_by_path(dir_path)
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after insertion - verify test database is working",
            );
        assert_eq!(dir.status, "blocked", "Status should be 'blocked'");

        // Verify audit entry
        let audit = AuditService::new(&db);
        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
        assert_eq!(entries.len(), 1, "Expected 1 audit entry");
        assert_eq!(entries[0].action, "remove");
        assert!(entries[0].details.is_some());
    }

    #[test]
    fn remove_approved_handles_mixed_success_and_failure() {
        let (db, _temp_dir) = temp_database();
        let test_root = TempDir::with_prefix("stagecrew-removal-files-").expect(
            "failed to create temp directory for removal test - check disk space and permissions",
        );

        // Create two directories: one normal, one protected
        let (dir1_path, dir1_size) = create_test_directory(test_root.path(), "normal", 2);
        let (dir2_path, dir2_size) = create_test_directory(test_root.path(), "protected", 2);

        // Make second directory read-only
        let path2 = std::path::Path::new(&dir2_path);
        let mut perms = fs::metadata(path2)
            .expect("failed to read file permissions - check file exists and is accessible")
            .permissions();
        perms.set_mode(0o444);
        fs::set_permissions(path2, perms)
            .expect("failed to set file permissions for test - check filesystem support");

        // Insert and approve both directories
        let now = jiff::Timestamp::now().as_second();
        let dir1_id = db
            .insert_or_update_directory(&dir1_path, dir1_size, 2, Some(now), now)
            .expect("failed to insert test directory - database connection may be lost");
        let dir2_id = db
            .insert_or_update_directory(&dir2_path, dir2_size, 2, Some(now), now)
            .expect("failed to insert test directory - database connection may be lost");
        db.update_directory_status(dir1_id, "approved")
            .expect("failed to update directory status - database connection may be lost");
        db.update_directory_status(dir2_id, "approved")
            .expect("failed to update directory status - database connection may be lost");

        // Attempt removal
        let summary = remove_approved(&db)
            .expect("failed to remove approved directories - check permissions and disk space");

        // Verify summary
        assert_eq!(summary.removed_count(), 1, "Expected 1 directory removed");
        assert_eq!(summary.blocked_count(), 1, "Expected 1 blocked directory");
        assert_eq!(
            summary.total_bytes_freed(),
            dir1_size,
            "Expected bytes freed from successful removal only"
        );

        // Verify first directory removed, second still exists
        assert!(!std::path::Path::new(&dir1_path).exists());
        assert!(path2.exists());

        // Verify database statuses
        let dir1 = db
            .get_directory_by_path(&dir1_path)
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after insertion - verify test database is working",
            );
        assert_eq!(dir1.status, "removed");

        let dir2 = db
            .get_directory_by_path(&dir2_path)
            .expect("failed to query directory from database - connection may be lost")
            .expect(
                "expected directory to exist after insertion - verify test database is working",
            );
        assert_eq!(dir2.status, "blocked");

        // Cleanup
        let mut perms = fs::metadata(path2)
            .expect("failed to read file permissions - check file exists and is accessible")
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path2, perms)
            .expect("failed to set file permissions for test - check filesystem support");
    }

    #[test]
    fn remove_approved_returns_empty_summary_when_no_approved() {
        let (db, _temp_dir) = temp_database();

        // Insert directories with non-approved statuses
        let now = jiff::Timestamp::now().as_second();
        let dir1_id = db
            .insert_or_update_directory("/path1", 1024, 5, Some(now), now)
            .expect("failed to insert test directory - database connection may be lost");
        let dir2_id = db
            .insert_or_update_directory("/path2", 2048, 10, Some(now), now)
            .expect("failed to insert test directory - database connection may be lost");

        db.update_directory_status(dir1_id, "tracked")
            .expect("failed to update directory status - database connection may be lost");
        db.update_directory_status(dir2_id, "pending")
            .expect("failed to update directory status - database connection may be lost");

        // Attempt removal
        let summary = remove_approved(&db)
            .expect("failed to remove approved directories - check permissions and disk space");

        // Verify empty summary
        assert_eq!(summary.removed_count(), 0);
        assert_eq!(summary.blocked_count(), 0);
        assert_eq!(summary.total_bytes_freed(), 0);

        // Verify no audit entries
        let audit = AuditService::new(&db);
        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
        assert_eq!(entries.len(), 0, "Expected no audit entries");
    }

    #[test]
    fn remove_approved_records_audit_entries_with_directory_id() {
        let (db, _temp_dir) = temp_database();
        let test_root = TempDir::with_prefix("stagecrew-removal-files-").expect(
            "failed to create temp directory for removal test - check disk space and permissions",
        );

        let (dir_path, dir_size) = create_test_directory(test_root.path(), "dir", 2);
        let now = jiff::Timestamp::now().as_second();
        let dir_id = db
            .insert_or_update_directory(&dir_path, dir_size, 2, Some(now), now)
            .expect("failed to insert test directory - database connection may be lost");
        db.update_directory_status(dir_id, "approved")
            .expect("failed to update directory status - database connection may be lost");

        let _summary = remove_approved(&db)
            .expect("failed to remove approved directories - check permissions and disk space");

        // Verify audit entry has directory_id
        let audit = AuditService::new(&db);
        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].directory_id, Some(dir_id));
        assert_eq!(entries[0].target_path, Some(dir_path));
    }
}
