//! Database schema, queries, and migrations.

use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::error::{Error, Result};
use crate::removal::RemovalMethod;

/// A user-configured tracked root path.
///
/// Roots are the top-level directories that users add to stagecrew for monitoring.
/// They appear in the TUI sidebar and serve as entry points for filesystem browsing.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Root {
    /// Unique database identifier.
    pub id: i64,
    /// Absolute path to the root directory.
    pub path: PathBuf,
    /// Unix timestamp when the root was added.
    pub added_at: i64,
    /// Unix timestamp of the last completed scan, or None if never scanned.
    pub last_scanned: Option<i64>,
    /// Optional byte quota target for this root.
    pub target_bytes: Option<i64>,
}

/// A filesystem entry (file or directory) within a tracked root.
///
/// Entries are discovered during scans and represent the actual filesystem contents.
/// Both files and directories are stored in the same table, distinguished by `is_dir`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Unique database identifier.
    pub id: i64,
    /// Foreign key to the parent root.
    pub root_id: i64,
    /// Absolute path to the entry.
    pub path: PathBuf,
    /// Path of the parent directory (for efficient listing).
    pub parent_path: PathBuf,
    /// True if this entry is a directory, false if it's a file.
    pub is_dir: bool,
    /// Size in bytes (0 for directories).
    pub size_bytes: i64,
    /// Unix timestamp of the file's modification time, or None for directories.
    pub mtime: Option<i64>,
    /// Unix timestamp when entry was first tracked by stagecrew.
    pub tracked_since: Option<i64>,
    /// Unix timestamp when the expiration countdown started.
    ///
    /// This is the anchor for expiration calculation: the file expires at
    /// `countdown_start + expiration_days`. Unlike `mtime`, this can be reset
    /// by user action to give files a fresh expiration period.
    pub countdown_start: Option<i64>,
    /// Current status in the lifecycle.
    pub status: String,
    /// Unix timestamp when deferral expires, or None if not deferred.
    pub deferred_until: Option<i64>,
    /// Unix timestamp when record was created.
    pub created_at: i64,
    /// Unix timestamp when record was last updated.
    pub updated_at: i64,
}

/// Pre-computed statistics for shell hooks and status display.
///
/// Stored in the singleton `stats` table row, updated after each scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Total number of tracked files.
    pub total_files: i64,
    /// Total size of all tracked files in bytes.
    pub total_size_bytes: i64,
    /// Number of files within the warning period.
    pub files_within_warning: i64,
    /// Number of files pending approval for removal.
    pub files_pending_approval: i64,
    /// Number of files that are overdue (past expiration).
    pub files_overdue: i64,
    /// Unix timestamp of the last completed scan.
    pub last_scan_completed: Option<i64>,
    /// Number of healthy files (tracked, not in warning or overdue).
    pub files_healthy: i64,
    /// Total bytes of healthy files.
    pub bytes_healthy: i64,
    /// Total bytes of files within the warning period.
    pub bytes_within_warning: i64,
    /// Total bytes of files pending approval.
    pub bytes_pending_approval: i64,
    /// Total bytes of overdue files.
    pub bytes_overdue: i64,
    /// Number of ignored files.
    pub files_ignored: i64,
    /// Total bytes of ignored files.
    pub bytes_ignored: i64,
}

/// Database handle for stagecrew state.
///
/// Manages the `SQLite` database that stores tracked roots, entries,
/// audit logs, and pre-computed statistics. Uses WAL mode for concurrent
/// access by multiple users on shared filesystems.
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Open or create the database at the given path.
    ///
    /// Creates the database file if it doesn't exist, enables WAL mode
    /// and foreign key constraints, and runs the schema initialization.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database cannot be opened (e.g., invalid path, permissions)
    /// - WAL mode cannot be enabled (e.g., unsupported filesystem)
    /// - Schema initialization fails
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        let db = Self { conn };
        db.initialize()?;
        Ok(db)
    }

    /// Initialize database schema and pragmas.
    ///
    /// Enables WAL mode for concurrent access and foreign key constraints,
    /// then creates all tables and indexes if they don't exist.
    fn initialize(&self) -> Result<()> {
        // Enable WAL mode for concurrent access across multiple users.
        // This must be done before any other operations.
        self.conn.pragma_update(None, "journal_mode", "WAL")?;

        // Verify WAL mode is active. This can fail on certain filesystems
        // (e.g., some network mounts) and would cause silent data corruption
        // in multi-user scenarios if not caught.
        let journal_mode: String = self
            .conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Config(format!(
                "Failed to enable WAL mode (got '{journal_mode}'). \
                 The database may be on a filesystem that doesn't support it."
            )));
        }

        // Enable foreign key constraint enforcement.
        self.conn.pragma_update(None, "foreign_keys", "ON")?;

        // Run schema creation (idempotent with IF NOT EXISTS).
        self.conn.execute_batch(include_str!("schema.sql"))?;

        Ok(())
    }

    /// Get a reference to the underlying connection.
    ///
    /// # Stability Note
    ///
    /// This exposes `rusqlite::Connection` directly. The database schema
    /// is not part of the stable API and may change between versions.
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    // =========================================================================
    // Root operations
    // =========================================================================

    /// Insert a new root path into the database.
    ///
    /// If the path already exists, returns the existing root's ID without
    /// modifying it.
    ///
    /// # Returns
    ///
    /// The database row ID of the inserted or existing root.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn insert_root(&self, path: &Path) -> Result<i64> {
        let path_str = path.to_string_lossy();
        self.conn.execute(
            "INSERT OR IGNORE INTO roots (path) VALUES (?1)",
            [&*path_str],
        )?;

        let id: i64 = self.conn.query_row(
            "SELECT id FROM roots WHERE path = ?1",
            [&*path_str],
            |row| row.get(0),
        )?;
        Ok(id)
    }

    /// Look up a root by its database ID.
    ///
    /// # Returns
    ///
    /// `Some(Root)` if found, `None` if no root has the given ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // TODO(cleanup): Remove allow once TuiContext::config() is wired into the event loop.
    #[allow(dead_code)]
    pub fn get_root(&self, id: i64) -> Result<Option<Root>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, path, added_at, last_scanned, target_bytes FROM roots WHERE id = ?1",
        )?;

        let mut rows = stmt.query([id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Root {
                id: row.get(0)?,
                path: PathBuf::from(row.get::<_, String>(1)?),
                added_at: row.get(2)?,
                last_scanned: row.get(3)?,
                target_bytes: row.get(4)?,
            }))
        } else {
            Ok(None)
        }
    }

    /// Get a root by its path.
    ///
    /// # Returns
    ///
    /// `Some(Root)` if found, `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // TODO(cleanup): Will be used by CLI `status` command to show root details.
    #[allow(dead_code)]
    pub fn get_root_by_path(&self, path: &Path) -> Result<Option<Root>> {
        let path_str = path.to_string_lossy();
        let mut stmt = self.conn.prepare(
            "SELECT id, path, added_at, last_scanned, target_bytes FROM roots WHERE path = ?1",
        )?;

        let mut rows = stmt.query([&*path_str])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Root {
                id: row.get(0)?,
                path: PathBuf::from(row.get::<_, String>(1)?),
                added_at: row.get(2)?,
                last_scanned: row.get(3)?,
                target_bytes: row.get(4)?,
            }))
        } else {
            Ok(None)
        }
    }

    /// List all tracked roots.
    ///
    /// # Returns
    ///
    /// A vector of all roots, ordered by path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    pub fn list_roots(&self) -> Result<Vec<Root>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, path, added_at, last_scanned, target_bytes FROM roots ORDER BY path",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(Root {
                id: row.get(0)?,
                path: PathBuf::from(row.get::<_, String>(1)?),
                added_at: row.get(2)?,
                last_scanned: row.get(3)?,
                target_bytes: row.get(4)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Delete a root and all its entries (via CASCADE).
    ///
    /// # Errors
    ///
    /// Returns an error if the root doesn't exist or the operation fails.
    // TODO(cleanup): Will be used by TUI `Remove Root` functionality.
    #[allow(dead_code)]
    pub fn delete_root(&self, root_id: i64) -> Result<()> {
        let rows_affected = self
            .conn
            .execute("DELETE FROM roots WHERE id = ?1", [root_id])?;

        if rows_affected == 0 {
            return Err(Error::Config(format!("Root with id {root_id} not found")));
        }

        Ok(())
    }

    /// Update the `last_scanned` timestamp for a root.
    ///
    /// # Errors
    ///
    /// Returns an error if the root doesn't exist or the operation fails.
    pub fn update_root_last_scanned(&self, root_id: i64, timestamp: i64) -> Result<()> {
        let rows_affected = self.conn.execute(
            "UPDATE roots SET last_scanned = ?1 WHERE id = ?2",
            (timestamp, root_id),
        )?;

        if rows_affected == 0 {
            return Err(Error::Config(format!("Root with id {root_id} not found")));
        }

        Ok(())
    }

    /// Set or clear the byte quota target for a root.
    ///
    /// Pass `Some(bytes)` to set a target, or `None` to clear it.
    /// A value of 0 is treated as clearing the target.
    ///
    /// # Errors
    ///
    /// Returns an error if the root doesn't exist or the operation fails.
    // TODO(cleanup): Remove allow once TUI quota target dialog is implemented.
    #[allow(dead_code)]
    pub fn set_root_target_bytes(&self, root_id: i64, target: Option<i64>) -> Result<()> {
        // Treat 0 as "no target" for convenience
        let target = target.filter(|&t| t > 0);

        let rows_affected = self.conn.execute(
            "UPDATE roots SET target_bytes = ?1 WHERE id = ?2",
            (target, root_id),
        )?;

        if rows_affected == 0 {
            return Err(Error::Config(format!("Root with id {root_id} not found")));
        }

        Ok(())
    }

    // =========================================================================
    // Entry operations
    // =========================================================================

    /// Insert or update an entry in the database.
    ///
    /// Uses UPSERT to either create a new entry or update an existing one
    /// based on the path. On first insert, sets `tracked_since` and `countdown_start`
    /// to the current timestamp. On update, preserves the existing values for both.
    ///
    /// # Arguments
    ///
    /// * `root_id` - Foreign key to the parent root
    /// * `path` - Absolute path to the entry
    /// * `parent_path` - Path of the parent directory
    /// * `is_dir` - True if this is a directory, false if a file
    /// * `size_bytes` - Size in bytes (0 for directories)
    /// * `mtime` - Unix timestamp of modification time, or None for directories
    ///
    /// # Returns
    ///
    /// The database row ID of the inserted or updated entry.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `root_id` does not exist (foreign key constraint)
    /// - The database operation fails
    pub fn upsert_entry(
        &self,
        root_id: i64,
        path: &Path,
        parent_path: &Path,
        is_dir: bool,
        size_bytes: i64,
        mtime: Option<i64>,
    ) -> Result<i64> {
        let path_str = path.to_string_lossy();
        let parent_path_str = parent_path.to_string_lossy();
        self.conn.execute(
            "INSERT INTO entries (root_id, path, parent_path, is_dir, size_bytes, mtime, tracked_since, countdown_start)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, strftime('%s', 'now'), strftime('%s', 'now'))
             ON CONFLICT(path) DO UPDATE SET
                 root_id = excluded.root_id,
                 parent_path = excluded.parent_path,
                 is_dir = excluded.is_dir,
                 size_bytes = excluded.size_bytes,
                 mtime = excluded.mtime,
                 updated_at = strftime('%s', 'now')",
            (root_id, &*path_str, &*parent_path_str, is_dir, size_bytes, mtime),
        )?;

        let id: i64 = self.conn.query_row(
            "SELECT id FROM entries WHERE path = ?1",
            [&*path_str],
            |row| row.get(0),
        )?;
        Ok(id)
    }

    /// Get an entry by its path.
    ///
    /// # Returns
    ///
    /// `Some(Entry)` if found, `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // TODO(cleanup): Will be used for entry lookup in CLI commands and TUI navigation.
    #[allow(dead_code)]
    pub fn get_entry_by_path(&self, path: &Path) -> Result<Option<Entry>> {
        let path_str = path.to_string_lossy();
        let mut stmt = self.conn.prepare(
            "SELECT id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                    tracked_since, countdown_start, status, deferred_until, created_at, updated_at
             FROM entries
             WHERE path = ?1",
        )?;

        let mut rows = stmt.query([&*path_str])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Entry {
                id: row.get(0)?,
                root_id: row.get(1)?,
                path: PathBuf::from(row.get::<_, String>(2)?),
                parent_path: PathBuf::from(row.get::<_, String>(3)?),
                is_dir: row.get(4)?,
                size_bytes: row.get(5)?,
                mtime: row.get(6)?,
                tracked_since: row.get(7)?,
                countdown_start: row.get(8)?,
                status: row.get(9)?,
                deferred_until: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            }))
        } else {
            Ok(None)
        }
    }

    /// List all entries with a given parent path.
    ///
    /// This is the primary method for browsing the filesystem tree. Given a
    /// directory path, returns all immediate children (both files and subdirectories).
    ///
    /// # Arguments
    ///
    /// * `parent_path` - The path of the parent directory
    ///
    /// # Returns
    ///
    /// A vector of entries, ordered by path. Excludes entries with status 'removed'.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    pub fn list_entries_by_parent(&self, parent_path: &Path) -> Result<Vec<Entry>> {
        let parent_path_str = parent_path.to_string_lossy();
        let mut stmt = self.conn.prepare(
            "SELECT id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                    tracked_since, countdown_start, status, deferred_until, created_at, updated_at
             FROM entries
             WHERE parent_path = ?1 AND status != 'removed'
             ORDER BY path",
        )?;

        let rows = stmt.query_map([&*parent_path_str], |row| {
            Ok(Entry {
                id: row.get(0)?,
                root_id: row.get(1)?,
                path: PathBuf::from(row.get::<_, String>(2)?),
                parent_path: PathBuf::from(row.get::<_, String>(3)?),
                is_dir: row.get(4)?,
                size_bytes: row.get(5)?,
                mtime: row.get(6)?,
                tracked_since: row.get(7)?,
                countdown_start: row.get(8)?,
                status: row.get(9)?,
                deferred_until: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// List all entries belonging to a specific root.
    ///
    /// Returns all entries (files and directories) under the given root, excluding
    /// removed entries. This is useful for computing aggregate statistics or
    /// visualizations that need the full picture of a root's contents.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    pub fn list_entries_by_root(&self, root_id: i64) -> Result<Vec<Entry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                    tracked_since, countdown_start, status, deferred_until, created_at, updated_at
             FROM entries
             WHERE root_id = ?1 AND status != 'removed'
             ORDER BY path",
        )?;

        let rows = stmt.query_map([root_id], |row| {
            Ok(Entry {
                id: row.get(0)?,
                root_id: row.get(1)?,
                path: PathBuf::from(row.get::<_, String>(2)?),
                parent_path: PathBuf::from(row.get::<_, String>(3)?),
                is_dir: row.get(4)?,
                size_bytes: row.get(5)?,
                mtime: row.get(6)?,
                tracked_since: row.get(7)?,
                countdown_start: row.get(8)?,
                status: row.get(9)?,
                deferred_until: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// List all entries, optionally filtered by status.
    ///
    /// # Arguments
    ///
    /// * `status_filter` - If provided, only return entries with this status
    ///
    /// # Returns
    ///
    /// A vector of entries, ordered by path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    pub fn list_entries(&self, status_filter: Option<&str>) -> Result<Vec<Entry>> {
        let row_to_entry = |row: &rusqlite::Row<'_>| -> rusqlite::Result<Entry> {
            Ok(Entry {
                id: row.get(0)?,
                root_id: row.get(1)?,
                path: PathBuf::from(row.get::<_, String>(2)?),
                parent_path: PathBuf::from(row.get::<_, String>(3)?),
                is_dir: row.get(4)?,
                size_bytes: row.get(5)?,
                mtime: row.get(6)?,
                tracked_since: row.get(7)?,
                countdown_start: row.get(8)?,
                status: row.get(9)?,
                deferred_until: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            })
        };

        let query = if status_filter.is_some() {
            "SELECT id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                    tracked_since, countdown_start, status, deferred_until, created_at, updated_at
             FROM entries
             WHERE status = ?1
             ORDER BY path"
        } else {
            "SELECT id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                    tracked_since, countdown_start, status, deferred_until, created_at, updated_at
             FROM entries
             ORDER BY path"
        };

        let mut stmt = self.conn.prepare(query)?;

        let rows = if let Some(status) = status_filter {
            stmt.query_map([status], row_to_entry)?
        } else {
            stmt.query_map([], row_to_entry)?
        };

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Update the status of an entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the entry doesn't exist or the operation fails.
    pub fn update_entry_status(&self, entry_id: i64, new_status: &str) -> Result<()> {
        let rows_affected = self.conn.execute(
            "UPDATE entries
             SET status = ?1, updated_at = strftime('%s', 'now')
             WHERE id = ?2",
            (new_status, entry_id),
        )?;

        if rows_affected == 0 {
            return Err(Error::Config(format!("Entry with id {entry_id} not found")));
        }

        Ok(())
    }

    /// Update the status of all entries matching a path prefix.
    ///
    /// This enables bulk operations like "ignore everything under /data/archive".
    ///
    /// # Arguments
    ///
    /// * `path_prefix` - The path prefix to match (entries where path starts with this)
    /// * `new_status` - The new status to set
    ///
    /// # Returns
    ///
    /// The number of entries updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn update_entries_by_path_prefix(
        &self,
        path_prefix: &Path,
        new_status: &str,
    ) -> Result<usize> {
        let prefix_str = path_prefix.to_string_lossy();
        // Use path || '/' to match the directory itself and all children
        let rows_affected = self.conn.execute(
            "UPDATE entries
             SET status = ?1, updated_at = strftime('%s', 'now')
             WHERE path = ?2 OR path LIKE ?3",
            (new_status, &*prefix_str, format!("{prefix_str}/%")),
        )?;

        Ok(rows_affected)
    }

    /// Defer an entry until a specified timestamp.
    ///
    /// Sets the entry's status to 'deferred' and records when the deferral expires.
    ///
    /// # Errors
    ///
    /// Returns an error if the entry doesn't exist or the operation fails.
    pub fn defer_entry(&self, entry_id: i64, deferred_until: i64) -> Result<()> {
        let rows_affected = self.conn.execute(
            "UPDATE entries
             SET status = 'deferred', deferred_until = ?1, updated_at = strftime('%s', 'now')
             WHERE id = ?2",
            (deferred_until, entry_id),
        )?;

        if rows_affected == 0 {
            return Err(Error::Config(format!("Entry with id {entry_id} not found")));
        }

        Ok(())
    }

    /// Defer all entries whose path matches or is a child of the given prefix.
    ///
    /// This is the deferral counterpart to [`update_entries_by_path_prefix`](Self::update_entries_by_path_prefix),
    /// setting both `status` and `deferred_until` in a single UPDATE.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn defer_entries_by_path_prefix(
        &self,
        path_prefix: &Path,
        deferred_until: i64,
    ) -> Result<usize> {
        let prefix_str = path_prefix.to_string_lossy();
        let rows_affected = self.conn.execute(
            "UPDATE entries
             SET status = 'deferred', deferred_until = ?1, updated_at = strftime('%s', 'now')
             WHERE path = ?2 OR path LIKE ?3",
            (deferred_until, &*prefix_str, format!("{prefix_str}/%")),
        )?;

        Ok(rows_affected)
    }

    /// Delete an entry from the filesystem and update its status.
    ///
    /// For files: removes the file from disk and marks as 'removed'.
    /// For directories: removes the directory recursively and marks as 'removed'.
    ///
    /// The `method` parameter determines whether to move to trash (recoverable)
    /// or permanently delete (irreversible).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The entry doesn't exist
    /// - The filesystem deletion fails (permission denied, etc.)
    /// - The trash operation fails (on platforms that support it)
    pub fn delete_entry(
        &self,
        entry_id: i64,
        path: &Path,
        is_dir: bool,
        method: RemovalMethod,
    ) -> Result<()> {
        // Attempt filesystem deletion using the specified method
        match method {
            RemovalMethod::Trash => {
                trash::delete(path).map_err(|e| Error::Trash {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })?;
            }
            RemovalMethod::PermanentDelete => {
                let fs_result = if is_dir {
                    std::fs::remove_dir_all(path)
                } else {
                    std::fs::remove_file(path)
                };

                if let Err(e) = fs_result {
                    return Err(match e.kind() {
                        std::io::ErrorKind::PermissionDenied => {
                            Error::PermissionDenied(path.to_path_buf())
                        }
                        std::io::ErrorKind::NotFound => Error::PathNotFound(path.to_path_buf()),
                        _ => Error::Io(e),
                    });
                }
            }
        }

        // Update database status to 'removed'
        self.update_entry_status(entry_id, "removed")?;

        // If it was a directory, also mark all children as removed
        if is_dir {
            self.update_entries_by_path_prefix(path, "removed")?;
        }

        Ok(())
    }

    /// Reset all countdowns for entries in a root.
    ///
    /// This gives all files in the root a fresh expiration period by:
    /// - Setting `countdown_start` to the current timestamp
    /// - Resetting `status` to 'tracked' for any 'pending' or 'approved' entries
    /// - Clearing `deferred_until` for any 'deferred' entries
    ///
    /// Entries with status 'ignored' or 'removed' are not affected.
    ///
    /// # Returns
    ///
    /// The number of entries that were reset.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn reset_root_countdowns(&self, root_id: i64) -> Result<usize> {
        let now = jiff::Timestamp::now().as_second();

        // Reset countdown_start for all non-ignored, non-removed entries
        let rows_affected = self.conn.execute(
            "UPDATE entries
             SET countdown_start = ?1,
                 status = CASE
                     WHEN status IN ('pending', 'approved', 'deferred') THEN 'tracked'
                     ELSE status
                 END,
                 deferred_until = CASE
                     WHEN status = 'deferred' THEN NULL
                     ELSE deferred_until
                 END,
                 updated_at = strftime('%s', 'now')
             WHERE root_id = ?2 AND status NOT IN ('ignored', 'removed')",
            (now, root_id),
        )?;

        Ok(rows_affected)
    }

    // =========================================================================
    // Stats operations
    // =========================================================================

    /// Retrieve pre-computed statistics.
    ///
    /// Returns the singleton stats row, which is guaranteed to exist
    /// after database initialization.
    ///
    /// # Errors
    ///
    /// Returns an error if the stats table cannot be queried or the row is missing.
    pub fn get_stats(&self) -> Result<Stats> {
        self.conn
            .query_row(
                "SELECT total_files, total_size_bytes, files_within_warning,
                        files_pending_approval, files_overdue, last_scan_completed
                 FROM stats WHERE id = 1",
                [],
                |row| {
                    Ok(Stats {
                        total_files: row.get(0)?,
                        total_size_bytes: row.get(1)?,
                        files_within_warning: row.get(2)?,
                        files_pending_approval: row.get(3)?,
                        files_overdue: row.get(4)?,
                        last_scan_completed: row.get(5)?,
                        // get_stats reads from the pre-computed stats table which
                        // doesn't have per-category breakdowns. Default to zero;
                        // callers needing breakdowns should use compute_live_stats.
                        files_healthy: 0,
                        bytes_healthy: 0,
                        bytes_within_warning: 0,
                        bytes_pending_approval: 0,
                        bytes_overdue: 0,
                        files_ignored: 0,
                        bytes_ignored: 0,
                    })
                },
            )
            .map_err(Into::into)
    }

    /// Compute live statistics by querying the entries table directly.
    ///
    /// Unlike [`get_stats`](Self::get_stats), which reads pre-computed values
    /// that are only refreshed during scans, this method always reflects the
    /// current state of the entries table — including status changes from user
    /// actions (ignore, approve, defer, etc.).
    ///
    /// The `last_scan_completed` field is still read from the stats table since
    /// it is only meaningful as a scan-time value.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    pub fn compute_live_stats(&self, expiration_days: u32, warning_days: u32) -> Result<Stats> {
        let now = jiff::Timestamp::now().as_second();
        let expiration_days_i64 = i64::from(expiration_days);
        let warning_days_i64 = i64::from(warning_days);

        self.conn
            .query_row(
                "SELECT
                    -- 0: total files (excludes removed and ignored)
                    (SELECT COUNT(*) FROM entries
                     WHERE is_dir = 0 AND status NOT IN ('removed', 'ignored')),
                    -- 1: total size bytes (excludes removed and ignored)
                    (SELECT COALESCE(SUM(size_bytes), 0) FROM entries
                     WHERE is_dir = 0 AND status NOT IN ('removed', 'ignored')),
                    -- 2: files within warning period
                    (SELECT COUNT(*) FROM entries
                     WHERE is_dir = 0 AND countdown_start IS NOT NULL AND status = 'tracked'
                       AND ((countdown_start + (?1 * 86400) - ?2) / 86400) <= ?3
                       AND ((countdown_start + (?1 * 86400) - ?2) / 86400) > 0),
                    -- 3: files pending approval
                    (SELECT COUNT(*) FROM entries WHERE is_dir = 0 AND status = 'pending'),
                    -- 4: files overdue
                    (SELECT COUNT(*) FROM entries
                     WHERE is_dir = 0 AND countdown_start IS NOT NULL AND status = 'tracked'
                       AND ((countdown_start + (?1 * 86400) - ?2) / 86400) <= 0),
                    -- 5: last scan completed
                    (SELECT last_scan_completed FROM stats WHERE id = 1),
                    -- 6: healthy files (tracked, outside warning period)
                    (SELECT COUNT(*) FROM entries
                     WHERE is_dir = 0 AND countdown_start IS NOT NULL AND status = 'tracked'
                       AND ((countdown_start + (?1 * 86400) - ?2) / 86400) > ?3),
                    -- 7: healthy bytes
                    (SELECT COALESCE(SUM(size_bytes), 0) FROM entries
                     WHERE is_dir = 0 AND countdown_start IS NOT NULL AND status = 'tracked'
                       AND ((countdown_start + (?1 * 86400) - ?2) / 86400) > ?3),
                    -- 8: warning bytes
                    (SELECT COALESCE(SUM(size_bytes), 0) FROM entries
                     WHERE is_dir = 0 AND countdown_start IS NOT NULL AND status = 'tracked'
                       AND ((countdown_start + (?1 * 86400) - ?2) / 86400) <= ?3
                       AND ((countdown_start + (?1 * 86400) - ?2) / 86400) > 0),
                    -- 9: pending bytes
                    (SELECT COALESCE(SUM(size_bytes), 0) FROM entries
                     WHERE is_dir = 0 AND status = 'pending'),
                    -- 10: overdue bytes
                    (SELECT COALESCE(SUM(size_bytes), 0) FROM entries
                     WHERE is_dir = 0 AND countdown_start IS NOT NULL AND status = 'tracked'
                       AND ((countdown_start + (?1 * 86400) - ?2) / 86400) <= 0),
                    -- 11: ignored files
                    (SELECT COUNT(*) FROM entries
                     WHERE is_dir = 0 AND status = 'ignored'),
                    -- 12: ignored bytes
                    (SELECT COALESCE(SUM(size_bytes), 0) FROM entries
                     WHERE is_dir = 0 AND status = 'ignored')",
                (expiration_days_i64, now, warning_days_i64),
                |row| {
                    Ok(Stats {
                        total_files: row.get(0)?,
                        total_size_bytes: row.get(1)?,
                        files_within_warning: row.get(2)?,
                        files_pending_approval: row.get(3)?,
                        files_overdue: row.get(4)?,
                        last_scan_completed: row.get(5)?,
                        files_healthy: row.get(6)?,
                        bytes_healthy: row.get(7)?,
                        bytes_within_warning: row.get(8)?,
                        bytes_pending_approval: row.get(9)?,
                        bytes_overdue: row.get(10)?,
                        files_ignored: row.get(11)?,
                        bytes_ignored: row.get(12)?,
                    })
                },
            )
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::NamedTempFile;

    use super::*;

    /// Creates a temporary database for testing.
    fn temp_database() -> (NamedTempFile, Database) {
        let temp_file = NamedTempFile::new().expect("failed to create temp file");
        let db = Database::open(temp_file.path()).expect("failed to open database");
        (temp_file, db)
    }

    #[test]
    fn database_creates_file_and_schema() {
        let (_temp, db) = temp_database();

        let tables: Vec<String> = db
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .expect("failed to prepare query")
            .query_map([], |row| row.get(0))
            .expect("failed to query")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("failed to collect");

        assert!(
            tables.contains(&"roots".to_string()),
            "missing 'roots' table, found: {tables:?}"
        );
        assert!(
            tables.contains(&"entries".to_string()),
            "missing 'entries' table, found: {tables:?}"
        );
        assert!(
            tables.contains(&"audit_log".to_string()),
            "missing 'audit_log' table, found: {tables:?}"
        );
        assert!(
            tables.contains(&"stats".to_string()),
            "missing 'stats' table, found: {tables:?}"
        );
    }

    #[test]
    fn database_enables_wal_mode() {
        let (_temp, db) = temp_database();

        let journal_mode: String = db
            .conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .expect("failed to query journal_mode");

        assert_eq!(journal_mode.to_lowercase(), "wal");
    }

    #[test]
    fn database_enables_foreign_keys() {
        let (_temp, db) = temp_database();

        let foreign_keys: i32 = db
            .conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .expect("failed to query foreign_keys");

        assert_eq!(foreign_keys, 1);
    }

    #[test]
    fn database_creates_indexes() {
        let (_temp, db) = temp_database();

        let indexes: Vec<String> = db
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
            .expect("failed to prepare query")
            .query_map([], |row| row.get(0))
            .expect("failed to query")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("failed to collect");

        let expected = [
            "idx_entries_root_id",
            "idx_entries_parent_path",
            "idx_entries_status",
            "idx_entries_mtime",
            "idx_audit_log_timestamp",
            "idx_audit_log_action",
        ];
        for idx in expected {
            assert!(
                indexes.contains(&idx.to_string()),
                "missing index '{idx}', found: {indexes:?}"
            );
        }
    }

    #[test]
    fn database_initializes_stats_singleton() {
        let (_temp, db) = temp_database();

        let count: i32 = db
            .conn
            .query_row("SELECT COUNT(*) FROM stats WHERE id = 1", [], |row| {
                row.get(0)
            })
            .expect("failed to query stats");

        assert_eq!(
            count, 1,
            "stats table should have exactly one row with id=1"
        );
    }

    #[test]
    fn database_schema_is_idempotent() {
        let temp_file = NamedTempFile::new().expect("failed to create temp file");

        {
            let db = Database::open(temp_file.path()).expect("first open");
            let root_id = db.insert_root(Path::new("/test")).expect("insert root");
            db.upsert_entry(
                root_id,
                Path::new("/test/file.txt"),
                Path::new("/test"),
                false,
                100,
                Some(1000),
            )
            .expect("insert entry");
        }

        let db = Database::open(temp_file.path()).expect("second open");

        let roots = db.list_roots().expect("list roots");
        assert_eq!(roots.len(), 1, "root should persist across opens");
        assert_eq!(roots[0].path, PathBuf::from("/test"));

        let entries = db
            .list_entries_by_parent(Path::new("/test"))
            .expect("list entries");
        assert_eq!(entries.len(), 1, "entry should persist across opens");

        let stats_count: i32 = db
            .conn
            .query_row("SELECT COUNT(*) FROM stats", [], |row| row.get(0))
            .expect("query stats");
        assert_eq!(
            stats_count, 1,
            "stats table should still have exactly one row"
        );
    }

    #[test]
    fn database_open_fails_on_invalid_path() {
        let result = Database::open(Path::new("/nonexistent/deeply/nested/path/db.sqlite"));
        assert!(result.is_err(), "should fail on invalid path");
    }

    #[test]
    fn foreign_key_constraint_prevents_orphan_entries() {
        let (_temp, db) = temp_database();

        let result = db.conn.execute(
            "INSERT INTO entries (root_id, path, parent_path, size_bytes) VALUES (999, '/orphan', '/', 100)",
            [],
        );

        assert!(
            result.is_err(),
            "foreign key should prevent inserting entry with invalid root_id"
        );
    }

    #[test]
    fn entries_rejects_invalid_status() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/test")).expect("insert root");
        let result = db.conn.execute(
            "INSERT INTO entries (root_id, path, parent_path, status) VALUES (?1, '/test/file', '/test', 'invalid_status')",
            [root_id],
        );

        assert!(
            result.is_err(),
            "CHECK constraint should reject invalid status value"
        );
    }

    #[test]
    fn stats_enforces_singleton_constraint() {
        let (_temp, db) = temp_database();

        let result = db.conn.execute("INSERT INTO stats (id) VALUES (2)", []);

        assert!(
            result.is_err(),
            "CHECK (id = 1) should reject stats row with id != 1"
        );
    }

    // === Root CRUD Tests ===

    #[test]
    fn insert_root_creates_new() {
        let (_temp, db) = temp_database();

        let id = db
            .insert_root(Path::new("/data/project1"))
            .expect("insert should succeed");

        let root = db
            .get_root_by_path(Path::new("/data/project1"))
            .expect("query should succeed")
            .expect("root should exist");

        assert_eq!(root.id, id);
        assert_eq!(root.path, PathBuf::from("/data/project1"));
        assert!(root.added_at > 0);
        assert_eq!(root.last_scanned, None);
    }

    #[test]
    fn insert_root_is_idempotent() {
        let (_temp, db) = temp_database();

        let id1 = db
            .insert_root(Path::new("/data/project1"))
            .expect("first insert");
        let id2 = db
            .insert_root(Path::new("/data/project1"))
            .expect("second insert");

        assert_eq!(id1, id2, "inserting same path should return same ID");

        let roots = db.list_roots().expect("list roots");
        assert_eq!(roots.len(), 1, "should only have one root");
    }

    #[test]
    fn get_root_by_path_returns_none_when_not_found() {
        let (_temp, db) = temp_database();

        let result = db
            .get_root_by_path(Path::new("/nonexistent"))
            .expect("query should succeed");

        assert_eq!(result, None);
    }

    #[test]
    fn list_roots_returns_all_ordered_by_path() {
        let (_temp, db) = temp_database();

        db.insert_root(Path::new("/data/zebra")).expect("insert");
        db.insert_root(Path::new("/data/alpha")).expect("insert");
        db.insert_root(Path::new("/data/middle")).expect("insert");

        let roots = db.list_roots().expect("list roots");

        assert_eq!(roots.len(), 3);
        assert_eq!(roots[0].path, PathBuf::from("/data/alpha"));
        assert_eq!(roots[1].path, PathBuf::from("/data/middle"));
        assert_eq!(roots[2].path, PathBuf::from("/data/zebra"));
    }

    #[test]
    fn delete_root_removes_root() {
        let (_temp, db) = temp_database();

        let id = db.insert_root(Path::new("/data/project1")).expect("insert");
        db.delete_root(id).expect("delete should succeed");

        let root = db
            .get_root_by_path(Path::new("/data/project1"))
            .expect("query");
        assert_eq!(root, None, "root should be deleted");
    }

    #[test]
    fn delete_root_cascades_to_entries() {
        let (_temp, db) = temp_database();

        let root_id = db
            .insert_root(Path::new("/data/project1"))
            .expect("insert root");
        db.upsert_entry(
            root_id,
            Path::new("/data/project1/file.txt"),
            Path::new("/data/project1"),
            false,
            100,
            Some(1000),
        )
        .expect("insert entry");

        let entries_before = db
            .list_entries_by_parent(Path::new("/data/project1"))
            .expect("list");
        assert_eq!(entries_before.len(), 1);

        db.delete_root(root_id).expect("delete root");

        let entries_after = db
            .list_entries_by_parent(Path::new("/data/project1"))
            .expect("list");
        assert_eq!(entries_after.len(), 0, "entries should be cascaded");
    }

    #[test]
    fn delete_root_fails_on_nonexistent() {
        let (_temp, db) = temp_database();

        let result = db.delete_root(999);
        assert!(result.is_err(), "should fail on nonexistent root");
    }

    #[test]
    fn update_root_last_scanned_works() {
        let (_temp, db) = temp_database();

        let id = db.insert_root(Path::new("/data/project1")).expect("insert");

        let root_before = db
            .get_root_by_path(Path::new("/data/project1"))
            .expect("query")
            .expect("root should exist");
        assert_eq!(root_before.last_scanned, None);

        db.update_root_last_scanned(id, 1_700_000_000)
            .expect("update");

        let root_after = db
            .get_root_by_path(Path::new("/data/project1"))
            .expect("query")
            .expect("root should exist");
        assert_eq!(root_after.last_scanned, Some(1_700_000_000));
    }

    // === Entry CRUD Tests ===

    #[test]
    fn upsert_entry_creates_new_file() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/file.txt"),
                Path::new("/data"),
                false,
                512,
                Some(1_700_000_000),
            )
            .expect("insert entry");

        let entry = db
            .get_entry_by_path(Path::new("/data/file.txt"))
            .expect("query")
            .expect("entry should exist");

        assert_eq!(entry.id, entry_id);
        assert_eq!(entry.root_id, root_id);
        assert_eq!(entry.path, PathBuf::from("/data/file.txt"));
        assert_eq!(entry.parent_path, PathBuf::from("/data"));
        assert!(!entry.is_dir);
        assert_eq!(entry.size_bytes, 512);
        assert_eq!(entry.mtime, Some(1_700_000_000));
        assert_eq!(entry.status, "tracked");
        assert!(entry.tracked_since.is_some());
    }

    #[test]
    fn upsert_entry_creates_new_directory() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/subdir"),
                Path::new("/data"),
                true,
                0,
                None,
            )
            .expect("insert entry");

        let entry = db
            .get_entry_by_path(Path::new("/data/subdir"))
            .expect("query")
            .expect("entry should exist");

        assert_eq!(entry.id, entry_id);
        assert!(entry.is_dir);
        assert_eq!(entry.size_bytes, 0);
        assert_eq!(entry.mtime, None);
    }

    #[test]
    fn upsert_entry_updates_existing() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        let id1 = db
            .upsert_entry(
                root_id,
                Path::new("/data/file.txt"),
                Path::new("/data"),
                false,
                512,
                Some(1_700_000_000),
            )
            .expect("first insert");

        let id2 = db
            .upsert_entry(
                root_id,
                Path::new("/data/file.txt"),
                Path::new("/data"),
                false,
                1024,
                Some(1_700_050_000),
            )
            .expect("second insert");

        assert_eq!(id1, id2, "upsert should return same ID");

        let entry = db
            .get_entry_by_path(Path::new("/data/file.txt"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(entry.size_bytes, 1024);
        assert_eq!(entry.mtime, Some(1_700_050_000));
    }

    #[test]
    fn upsert_entry_preserves_tracked_since() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        db.upsert_entry(
            root_id,
            Path::new("/data/file.txt"),
            Path::new("/data"),
            false,
            512,
            Some(1_700_000_000),
        )
        .expect("first insert");

        let entry_before = db
            .get_entry_by_path(Path::new("/data/file.txt"))
            .expect("query")
            .expect("entry should exist");
        let tracked_since = entry_before.tracked_since;

        std::thread::sleep(std::time::Duration::from_secs(1));

        db.upsert_entry(
            root_id,
            Path::new("/data/file.txt"),
            Path::new("/data"),
            false,
            1024,
            Some(1_700_050_000),
        )
        .expect("update");

        let entry_after = db
            .get_entry_by_path(Path::new("/data/file.txt"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(
            entry_after.tracked_since, tracked_since,
            "tracked_since should be preserved"
        );
    }

    #[test]
    fn upsert_entry_fails_with_invalid_root_id() {
        let (_temp, db) = temp_database();

        let result = db.upsert_entry(
            999,
            Path::new("/data/file.txt"),
            Path::new("/data"),
            false,
            512,
            Some(1_700_000_000),
        );
        assert!(result.is_err(), "should fail due to foreign key constraint");
    }

    #[test]
    fn get_entry_by_path_returns_none_when_not_found() {
        let (_temp, db) = temp_database();

        let result = db
            .get_entry_by_path(Path::new("/nonexistent"))
            .expect("query");
        assert_eq!(result, None);
    }

    #[test]
    fn list_entries_by_parent_returns_children() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        db.upsert_entry(
            root_id,
            Path::new("/data/a.txt"),
            Path::new("/data"),
            false,
            100,
            Some(1000),
        )
        .expect("insert");
        db.upsert_entry(
            root_id,
            Path::new("/data/b.txt"),
            Path::new("/data"),
            false,
            200,
            Some(1000),
        )
        .expect("insert");
        db.upsert_entry(
            root_id,
            Path::new("/data/subdir"),
            Path::new("/data"),
            true,
            0,
            None,
        )
        .expect("insert");
        db.upsert_entry(
            root_id,
            Path::new("/data/subdir/nested.txt"),
            Path::new("/data/subdir"),
            false,
            50,
            Some(1000),
        )
        .expect("insert");

        let entries = db.list_entries_by_parent(Path::new("/data")).expect("list");

        assert_eq!(entries.len(), 3, "should return only immediate children");
        assert_eq!(entries[0].path, PathBuf::from("/data/a.txt"));
        assert_eq!(entries[1].path, PathBuf::from("/data/b.txt"));
        assert_eq!(entries[2].path, PathBuf::from("/data/subdir"));
    }

    #[test]
    fn list_entries_by_parent_excludes_removed() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/file.txt"),
                Path::new("/data"),
                false,
                100,
                Some(1000),
            )
            .expect("insert");

        db.update_entry_status(entry_id, "removed")
            .expect("update status");

        let entries = db.list_entries_by_parent(Path::new("/data")).expect("list");
        assert_eq!(entries.len(), 0, "removed entries should be excluded");
    }

    #[test]
    fn list_entries_by_root_returns_all_descendants() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        // Create a nested structure: /data/a.txt, /data/subdir/, /data/subdir/b.txt
        db.upsert_entry(
            root_id,
            Path::new("/data/a.txt"),
            Path::new("/data"),
            false,
            100,
            Some(1000),
        )
        .expect("insert");
        db.upsert_entry(
            root_id,
            Path::new("/data/subdir"),
            Path::new("/data"),
            true,
            0,
            None,
        )
        .expect("insert");
        db.upsert_entry(
            root_id,
            Path::new("/data/subdir/b.txt"),
            Path::new("/data/subdir"),
            false,
            200,
            Some(1000),
        )
        .expect("insert");

        // Create another root with its own entry (should not be returned)
        let other_root_id = db
            .insert_root(Path::new("/other"))
            .expect("insert other root");
        db.upsert_entry(
            other_root_id,
            Path::new("/other/c.txt"),
            Path::new("/other"),
            false,
            300,
            Some(1000),
        )
        .expect("insert");

        let entries = db.list_entries_by_root(root_id).expect("list");

        assert_eq!(entries.len(), 3, "should return all entries for the root");
        assert_eq!(entries[0].path, PathBuf::from("/data/a.txt"));
        assert_eq!(entries[1].path, PathBuf::from("/data/subdir"));
        assert_eq!(entries[2].path, PathBuf::from("/data/subdir/b.txt"));
    }

    #[test]
    fn list_entries_by_root_excludes_removed() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/file.txt"),
                Path::new("/data"),
                false,
                100,
                Some(1000),
            )
            .expect("insert");

        db.update_entry_status(entry_id, "removed")
            .expect("update status");

        let entries = db.list_entries_by_root(root_id).expect("list");
        assert_eq!(entries.len(), 0, "removed entries should be excluded");
    }

    #[test]
    fn list_entries_filters_by_status() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        let id1 = db
            .upsert_entry(
                root_id,
                Path::new("/data/a.txt"),
                Path::new("/data"),
                false,
                100,
                Some(1000),
            )
            .expect("insert");
        let id2 = db
            .upsert_entry(
                root_id,
                Path::new("/data/b.txt"),
                Path::new("/data"),
                false,
                200,
                Some(1000),
            )
            .expect("insert");
        db.upsert_entry(
            root_id,
            Path::new("/data/c.txt"),
            Path::new("/data"),
            false,
            300,
            Some(1000),
        )
        .expect("insert");

        db.update_entry_status(id1, "pending").expect("update");
        db.update_entry_status(id2, "pending").expect("update");

        let pending = db.list_entries(Some("pending")).expect("list");
        assert_eq!(pending.len(), 2);

        let tracked = db.list_entries(Some("tracked")).expect("list");
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].path, PathBuf::from("/data/c.txt"));
    }

    #[test]
    fn update_entry_status_works() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/file.txt"),
                Path::new("/data"),
                false,
                100,
                Some(1000),
            )
            .expect("insert");

        db.update_entry_status(entry_id, "approved")
            .expect("update");

        let entry = db
            .get_entry_by_path(Path::new("/data/file.txt"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(entry.status, "approved");
    }

    #[test]
    fn update_entry_status_fails_on_nonexistent() {
        let (_temp, db) = temp_database();

        let result = db.update_entry_status(999, "approved");
        assert!(result.is_err());
    }

    #[test]
    fn update_entry_status_rejects_invalid_status() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/file.txt"),
                Path::new("/data"),
                false,
                100,
                Some(1000),
            )
            .expect("insert");

        let result = db.update_entry_status(entry_id, "invalid_status");
        assert!(
            result.is_err(),
            "CHECK constraint should reject invalid status"
        );
    }

    #[test]
    fn update_entries_by_path_prefix_updates_all_matching() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        db.upsert_entry(
            root_id,
            Path::new("/data/archive"),
            Path::new("/data"),
            true,
            0,
            None,
        )
        .expect("insert");
        db.upsert_entry(
            root_id,
            Path::new("/data/archive/old.txt"),
            Path::new("/data/archive"),
            false,
            100,
            Some(1000),
        )
        .expect("insert");
        db.upsert_entry(
            root_id,
            Path::new("/data/archive/older.txt"),
            Path::new("/data/archive"),
            false,
            200,
            Some(1000),
        )
        .expect("insert");
        db.upsert_entry(
            root_id,
            Path::new("/data/keep.txt"),
            Path::new("/data"),
            false,
            300,
            Some(1000),
        )
        .expect("insert");

        let count = db
            .update_entries_by_path_prefix(Path::new("/data/archive"), "ignored")
            .expect("update");

        assert_eq!(count, 3, "should update directory and its children");

        let archive = db
            .get_entry_by_path(Path::new("/data/archive"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(archive.status, "ignored");

        let old = db
            .get_entry_by_path(Path::new("/data/archive/old.txt"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(old.status, "ignored");

        let keep = db
            .get_entry_by_path(Path::new("/data/keep.txt"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(
            keep.status, "tracked",
            "unrelated entry should not be affected"
        );
    }

    #[test]
    fn defer_entry_sets_status_and_timestamp() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/file.txt"),
                Path::new("/data"),
                false,
                100,
                Some(1000),
            )
            .expect("insert");

        db.defer_entry(entry_id, 1_700_500_000).expect("defer");

        let entry = db
            .get_entry_by_path(Path::new("/data/file.txt"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(entry.status, "deferred");
        assert_eq!(entry.deferred_until, Some(1_700_500_000));
    }

    #[test]
    fn defer_entry_fails_on_nonexistent() {
        let (_temp, db) = temp_database();

        let result = db.defer_entry(999, 1_700_500_000);
        assert!(result.is_err());
    }

    #[test]
    fn get_stats_returns_defaults() {
        let (_temp, db) = temp_database();

        let stats = db.get_stats().expect("get stats");

        assert_eq!(stats.total_files, 0);
        assert_eq!(stats.total_size_bytes, 0);
        assert_eq!(stats.files_within_warning, 0);
        assert_eq!(stats.files_pending_approval, 0);
        assert_eq!(stats.files_overdue, 0);
        assert_eq!(stats.last_scan_completed, None);
    }

    #[test]
    fn compute_live_stats_counts_entries() {
        let (_temp, db) = temp_database();
        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        // Insert a file entry
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/file1.txt"),
                Path::new("/data"),
                false,
                1024,
                Some(1_700_000_000),
            )
            .expect("upsert");

        // Set countdown_start to an old timestamp (Nov 2023) to make it overdue.
        // Expiration is based on countdown_start, not mtime.
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE id = ?2",
                (1_700_000_000_i64, entry_id),
            )
            .expect("backdate countdown_start");

        let stats = db.compute_live_stats(90, 14).expect("compute_live_stats");
        assert_eq!(stats.total_files, 1, "should count one file");
        assert_eq!(stats.total_size_bytes, 1024, "should sum size");
        // countdown_start is Nov 2023, well past 90-day expiration
        assert_eq!(stats.files_overdue, 1, "old file should be overdue");
    }

    #[test]
    fn compute_live_stats_cross_connection_visibility() {
        let temp_file = NamedTempFile::new().expect("create temp file");
        let path = temp_file.path();

        // Connection 1: the "TUI" connection — opens first, stays open
        let reader = Database::open(path).expect("open reader");

        // Read stats before any data exists
        let before = reader.compute_live_stats(90, 14).expect("stats before");
        assert_eq!(before.total_files, 0);

        // Connection 2: the "scan" connection — opens, writes, closes
        {
            let writer = Database::open(path).expect("open writer");
            let root_id = writer.insert_root(Path::new("/data")).expect("insert root");
            writer
                .upsert_entry(
                    root_id,
                    Path::new("/data/file1.txt"),
                    Path::new("/data"),
                    false,
                    1024,
                    Some(1_700_000_000),
                )
                .expect("upsert");
        } // writer dropped here, connection closed

        // Connection 1 reads again — does it see the new data?
        let after = reader.compute_live_stats(90, 14).expect("stats after");
        eprintln!("AFTER: total_files={}", after.total_files);
        assert_eq!(
            after.total_files, 1,
            "reader should see writer's committed data"
        );
    }
}
