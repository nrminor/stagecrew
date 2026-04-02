//! Database schema, queries, and migrations.

use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::error::{Error, Result};
use crate::removal::RemovalMethod;

/// Current SQLite schema version managed via `PRAGMA user_version`.
const SCHEMA_VERSION: i64 = 2;

/// Expiration policy for a tracked root, used when computing global deduplicated stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootStatConfig {
    pub root_id: i64,
    pub expiration_days: u32,
    pub warning_days: u32,
}

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
    /// Size in bytes.
    ///
    /// For files this is the file size. For directories this stores the
    /// recursive byte total of all descendant files captured during scan.
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

        self.migrate_schema()?;

        Ok(())
    }

    /// Apply ordered schema migrations until the database reaches the current version.
    fn migrate_schema(&self) -> Result<()> {
        let version = self.user_version()?;

        match version {
            0 => {
                self.migrate_v0_to_v1()?;
                self.set_user_version(1)?;
                self.migrate_v1_to_v2()?;
                self.set_user_version(SCHEMA_VERSION)?;
                Ok(())
            }
            1 => {
                self.migrate_v1_to_v2()?;
                self.set_user_version(SCHEMA_VERSION)?;
                Ok(())
            }
            SCHEMA_VERSION => Ok(()),
            other => Err(Error::Config(format!(
                "Unsupported database schema version {other}; expected {SCHEMA_VERSION}"
            ))),
        }
    }

    /// Migrate legacy unversioned databases into the versioned schema flow.
    fn migrate_v0_to_v1(&self) -> Result<()> {
        self.migrate_audit_log_if_needed()
    }

    fn migrate_v1_to_v2(&self) -> Result<()> {
        tracing::info!("Migrating entries table to root-scoped uniqueness");

        let tx = self.conn.unchecked_transaction()?;
        tx.execute_batch(
            "DROP INDEX IF EXISTS idx_entries_root_id;
             DROP INDEX IF EXISTS idx_entries_parent_path;
             DROP INDEX IF EXISTS idx_entries_status;
             DROP INDEX IF EXISTS idx_entries_mtime;

             CREATE TABLE entries_new (
                 id INTEGER PRIMARY KEY,
                 root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
                 path TEXT NOT NULL,
                 parent_path TEXT NOT NULL,
                 is_dir INTEGER NOT NULL DEFAULT 0,
                 size_bytes INTEGER NOT NULL DEFAULT 0,
                 mtime INTEGER,
                 tracked_since INTEGER,
                 countdown_start INTEGER,
                 status TEXT NOT NULL DEFAULT 'tracked'
                     CHECK (status IN ('tracked', 'pending', 'approved', 'deferred', 'ignored', 'removed', 'blocked')),
                 deferred_until INTEGER,
                 created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                 updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                 UNIQUE(root_id, path)
             );

             INSERT INTO entries_new (
                 id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                 tracked_since, countdown_start, status, deferred_until, created_at, updated_at
             )
             SELECT
                 id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                 tracked_since, countdown_start, status, deferred_until, created_at, updated_at
             FROM entries;

             DROP TABLE entries;
             ALTER TABLE entries_new RENAME TO entries;

             CREATE INDEX IF NOT EXISTS idx_entries_root_id ON entries(root_id);
             CREATE INDEX IF NOT EXISTS idx_entries_root_parent_path ON entries(root_id, parent_path);
             CREATE INDEX IF NOT EXISTS idx_entries_root_status ON entries(root_id, status);
             CREATE INDEX IF NOT EXISTS idx_entries_root_is_dir_status ON entries(root_id, is_dir, status);
             CREATE INDEX IF NOT EXISTS idx_entries_mtime ON entries(mtime);
             CREATE INDEX IF NOT EXISTS idx_entries_path ON entries(path);",
        )?;
        tx.commit()?;

        tracing::info!("entries table migration complete");
        Ok(())
    }

    fn user_version(&self) -> Result<i64> {
        self.conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(Into::into)
    }

    fn set_user_version(&self, version: i64) -> Result<()> {
        self.conn.pragma_update(None, "user_version", version)?;
        Ok(())
    }

    /// Migrate the `audit_log` table if it uses the old schema (missing new columns).
    ///
    /// This detects whether the table exists but lacks the `actor_source` column,
    /// then rebuilds it in a transaction to add the new fields while preserving
    /// all existing rows. New columns are filled with NULL for historical data.
    fn migrate_audit_log_if_needed(&self) -> Result<()> {
        // Check whether audit_log exists at all. If not, schema.sql will create it.
        let table_exists: bool = self.conn.query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='audit_log'",
            [],
            |row| row.get(0),
        )?;
        if !table_exists {
            return Ok(());
        }

        // Check whether the new columns already exist.
        let has_actor_source: bool = self.conn.query_row(
            "SELECT COUNT(*) > 0 FROM pragma_table_info('audit_log') WHERE name='actor_source'",
            [],
            |row| row.get(0),
        )?;
        if has_actor_source {
            return Ok(());
        }

        tracing::info!("Migrating audit_log table to expanded schema");

        let tx = self.conn.unchecked_transaction()?;
        tx.execute_batch(
            "CREATE TABLE audit_log_new (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                user TEXT NOT NULL,
                action TEXT NOT NULL
                    CHECK (action IN ('approve', 'unapprove', 'defer', 'ignore', 'unignore', 'remove', 'scan', 'undo', 'config_change')),
                target_path TEXT,
                details TEXT,
                entry_id INTEGER REFERENCES entries(id) ON DELETE SET NULL,
                actor_source TEXT,
                root_id INTEGER,
                outcome TEXT,
                status_before TEXT,
                status_after TEXT
            );

            INSERT INTO audit_log_new (id, timestamp, user, action, target_path, details, entry_id)
                SELECT id, timestamp, user, action, target_path, details, entry_id
                FROM audit_log;

            DROP TABLE audit_log;

            ALTER TABLE audit_log_new RENAME TO audit_log;

            CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp);
            CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);"
        )?;
        tx.commit()?;

        tracing::info!("audit_log migration complete");
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
    /// to the current timestamp. On update, preserves those values unless the existing
    /// row is `removed`, in which case the path is revived as newly `tracked`.
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
    // TODO(cleanup): Remove allow once production code paths need returned entry IDs again.
    // This remains used heavily in tests to create fixture rows with stable IDs.
    #[allow(dead_code)]
    pub fn upsert_entry(
        &self,
        root_id: i64,
        path: &Path,
        parent_path: &Path,
        is_dir: bool,
        size_bytes: i64,
        mtime: Option<i64>,
    ) -> Result<i64> {
        self.upsert_entry_internal(root_id, path, parent_path, is_dir, size_bytes, mtime)?;

        let path_str = path.to_string_lossy();
        let id: i64 = self.conn.query_row(
            "SELECT id FROM entries WHERE root_id = ?1 AND path = ?2",
            rusqlite::params![root_id, &*path_str],
            |row| row.get(0),
        )?;
        Ok(id)
    }

    /// Insert or update an entry in the database without reading the row ID.
    ///
    /// This is useful in scan paths that do not need the entry ID, avoiding an
    /// extra read query per upsert.
    ///
    /// # Errors
    ///
    /// Returns an error if the upsert fails.
    pub fn upsert_entry_no_return(
        &self,
        root_id: i64,
        path: &Path,
        parent_path: &Path,
        is_dir: bool,
        size_bytes: i64,
        mtime: Option<i64>,
    ) -> Result<()> {
        self.upsert_entry_internal(root_id, path, parent_path, is_dir, size_bytes, mtime)
    }

    fn upsert_entry_internal(
        &self,
        root_id: i64,
        path: &Path,
        parent_path: &Path,
        is_dir: bool,
        size_bytes: i64,
        mtime: Option<i64>,
    ) -> Result<()> {
        let path_str = path.to_string_lossy();
        let parent_path_str = parent_path.to_string_lossy();
        self.conn.execute(
            "INSERT INTO entries (root_id, path, parent_path, is_dir, size_bytes, mtime, tracked_since, countdown_start)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, strftime('%s', 'now'), strftime('%s', 'now'))
             ON CONFLICT(root_id, path) DO UPDATE SET
                 parent_path = excluded.parent_path,
                 is_dir = excluded.is_dir,
                 size_bytes = excluded.size_bytes,
                 mtime = excluded.mtime,
                 status = CASE
                     WHEN entries.status = 'removed' THEN 'tracked'
                     ELSE entries.status
                 END,
                 tracked_since = CASE
                     WHEN entries.status = 'removed' THEN strftime('%s', 'now')
                     ELSE entries.tracked_since
                 END,
                 countdown_start = CASE
                     WHEN entries.status = 'removed' THEN strftime('%s', 'now')
                     ELSE entries.countdown_start
                 END,
                 deferred_until = CASE
                     WHEN entries.status = 'removed' THEN NULL
                     ELSE entries.deferred_until
                 END,
                 updated_at = strftime('%s', 'now')",
            (root_id, &*path_str, &*parent_path_str, is_dir, size_bytes, mtime),
        )?;
        Ok(())
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

    /// Get an entry for a specific root by its path.
    ///
    /// # Returns
    ///
    /// `Some(Entry)` if found, `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // TODO(cleanup): Replace remaining test-only path lookups with explicit root-scoped helpers.
    // Overlap-aware production code should prefer get_entry_by_root_and_path.
    #[allow(dead_code)]
    pub fn get_entry_by_root_and_path(&self, root_id: i64, path: &Path) -> Result<Option<Entry>> {
        let path_str = path.to_string_lossy();
        let mut stmt = self.conn.prepare(
            "SELECT id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                    tracked_since, countdown_start, status, deferred_until, created_at, updated_at
             FROM entries
             WHERE root_id = ?1 AND path = ?2",
        )?;

        let mut rows = stmt.query(rusqlite::params![root_id, &*path_str])?;
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
    pub fn list_entries_by_parent(&self, root_id: i64, parent_path: &Path) -> Result<Vec<Entry>> {
        let parent_path_str = parent_path.to_string_lossy();
        let mut stmt = self.conn.prepare(
            "SELECT id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                     tracked_since, countdown_start, status, deferred_until, created_at, updated_at
              FROM entries
             WHERE root_id = ?1 AND parent_path = ?2 AND status != 'removed'
              ORDER BY path",
        )?;

        let rows = stmt.query_map(rusqlite::params![root_id, &*parent_path_str], |row| {
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

    /// List all entries for a specific root with a given status.
    ///
    /// This is the root-scoped counterpart of [`list_entries`](Self::list_entries)
    /// with a mandatory status filter. Used by the dry run preflight check to
    /// query only approved entries for a single root.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    pub fn list_entries_by_root_and_status(
        &self,
        root_id: i64,
        status: &str,
    ) -> Result<Vec<Entry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, root_id, path, parent_path, is_dir, size_bytes, mtime,
                    tracked_since, countdown_start, status, deferred_until, created_at, updated_at
             FROM entries
             WHERE root_id = ?1 AND status = ?2
             ORDER BY path",
        )?;

        let rows = stmt.query_map((root_id, status), |row| {
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

    /// Find the nearest expiration unix timestamp across all active entries.
    ///
    /// For tracked entries, expiration is `countdown_start + expiration_days * 86400`.
    /// For deferred entries, expiration is `deferred_until`.
    /// Ignored, removed, and blocked entries are excluded.
    ///
    /// Returns `None` if no active entries have computable expiration times.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    pub fn nearest_expiration(&self, expiration_days: u32) -> Result<Option<i64>> {
        let expiration_seconds = i64::from(expiration_days) * 86400;
        let result: Option<i64> = self.conn.query_row(
            "SELECT MIN(
                CASE
                    WHEN status = 'deferred' AND deferred_until IS NOT NULL
                        THEN deferred_until
                    WHEN countdown_start IS NOT NULL
                        THEN countdown_start + ?1
                    ELSE NULL
                END
            )
            FROM entries
            WHERE is_dir = 0
              AND status NOT IN ('ignored', 'removed', 'blocked')",
            [expiration_seconds],
            |row| row.get(0),
        )?;
        Ok(result)
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

    /// Restore an entry to a previous state for undo operations.
    ///
    /// Sets the status, `countdown_start`, and `deferred_until` back to the
    /// values captured before the original action was applied.
    ///
    /// # Errors
    ///
    /// Returns an error if the entry doesn't exist or the operation fails.
    pub fn restore_entry_state(
        &self,
        entry_id: i64,
        status: &str,
        countdown_start: Option<i64>,
        deferred_until: Option<i64>,
    ) -> Result<()> {
        let rows = self.conn.execute(
            "UPDATE entries
             SET status = ?1,
                 countdown_start = ?2,
                 deferred_until = ?3,
                 updated_at = strftime('%s', 'now')
             WHERE id = ?4",
            rusqlite::params![status, countdown_start, deferred_until, entry_id],
        )?;
        if rows == 0 {
            return Err(crate::error::Error::Config(format!(
                "Entry {entry_id} not found for undo"
            )));
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
        root_id: i64,
        path_prefix: &Path,
        new_status: &str,
    ) -> Result<usize> {
        let prefix_str = path_prefix.to_string_lossy();
        // Use path || '/' to match the directory itself and all children
        let rows_affected = self.conn.execute(
            "UPDATE entries
              SET status = ?1, updated_at = strftime('%s', 'now')
             WHERE root_id = ?2 AND (path = ?3 OR path LIKE ?4)",
            rusqlite::params![new_status, root_id, &*prefix_str, format!("{prefix_str}/%")],
        )?;

        Ok(rows_affected)
    }

    /// Enforce ignored-directory inheritance for all entries under a root.
    ///
    /// Any non-removed entry whose path is equal to, or a descendant of, an
    /// ignored directory under the same root is set to `ignored`.
    ///
    /// # Returns
    ///
    /// The number of entries updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn enforce_ignored_directory_inheritance(&self, root_id: i64) -> Result<usize> {
        let rows_affected = self.conn.execute(
            "UPDATE entries AS e
             SET status = 'ignored',
                 updated_at = strftime('%s', 'now')
             WHERE e.root_id = ?1
               AND e.status != 'removed'
               AND EXISTS (
                   SELECT 1
                   FROM entries AS d
                   WHERE d.root_id = e.root_id
                     AND d.is_dir = 1
                     AND d.status = 'ignored'
                     AND (e.path = d.path OR e.path LIKE d.path || '/%')
               )",
            [root_id],
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
        root_id: i64,
        path_prefix: &Path,
        deferred_until: i64,
    ) -> Result<usize> {
        let prefix_str = path_prefix.to_string_lossy();
        let rows_affected = self.conn.execute(
            "UPDATE entries
              SET status = 'deferred', deferred_until = ?1, updated_at = strftime('%s', 'now')
             WHERE root_id = ?2 AND (path = ?3 OR path LIKE ?4)",
            rusqlite::params![
                deferred_until,
                root_id,
                &*prefix_str,
                format!("{prefix_str}/%")
            ],
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
            let root_id: i64 = self.conn.query_row(
                "SELECT root_id FROM entries WHERE id = ?1",
                [entry_id],
                |row| row.get(0),
            )?;
            self.update_entries_by_path_prefix(root_id, path, "removed")?;
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
    // TODO(cleanup): Remove once all callers use explicit per-root config maps.
    // This remains useful in tests that exercise the legacy single-config path.
    #[allow(dead_code)]
    pub fn compute_live_stats(&self, expiration_days: u32, warning_days: u32) -> Result<Stats> {
        let root_configs = self
            .list_roots()?
            .into_iter()
            .map(|root| RootStatConfig {
                root_id: root.id,
                expiration_days,
                warning_days,
            })
            .collect::<Vec<_>>();
        self.compute_live_stats_with_root_configs(&root_configs)
    }

    /// Compute live statistics using explicit per-root expiration policies.
    ///
    /// Global totals are deduplicated by file path so overlapping roots do not
    /// double-count bytes or files in the header and shell status surfaces.
    ///
    /// # Errors
    ///
    /// Returns an error if the entries or stats tables cannot be queried.
    pub fn compute_live_stats_with_root_configs(
        &self,
        root_configs: &[RootStatConfig],
    ) -> Result<Stats> {
        let now = jiff::Timestamp::now().as_second();
        let config_map: std::collections::HashMap<i64, (u32, u32)> = root_configs
            .iter()
            .map(|cfg| (cfg.root_id, (cfg.expiration_days, cfg.warning_days)))
            .collect();

        let last_scan_completed: Option<i64> = self.conn.query_row(
            "SELECT last_scan_completed FROM stats WHERE id = 1",
            [],
            |row| row.get(0),
        )?;

        let mut stmt = self.conn.prepare(
            "SELECT path, root_id, size_bytes, countdown_start, deferred_until, status
             FROM entries
             WHERE is_dir = 0
             ORDER BY path, root_id",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(DedupedStatRow {
                path: row.get(0)?,
                root_id: row.get(1)?,
                size_bytes: row.get(2)?,
                countdown_start: row.get(3)?,
                deferred_until: row.get(4)?,
                status: row.get(5)?,
            })
        })?;

        let mut aggregate = DedupedStatAggregate::default();
        let mut current_path: Option<String> = None;
        let mut current_bucket = PathStatBucket::default();

        for row in rows {
            let row = row?;
            if current_path.as_deref() != Some(&row.path) {
                if current_path.is_some() {
                    aggregate.observe(&current_bucket);
                }
                current_path = Some(row.path.clone());
                current_bucket = PathStatBucket::default();
            }

            let (expiration_days, warning_days) =
                config_map.get(&row.root_id).copied().unwrap_or((90, 14));
            current_bucket.observe_row(&row, expiration_days, warning_days, now);
        }

        if current_path.is_some() {
            aggregate.observe(&current_bucket);
        }

        Ok(Stats {
            total_files: aggregate.total_files,
            total_size_bytes: aggregate.total_size_bytes,
            files_within_warning: aggregate.files_within_warning,
            files_pending_approval: aggregate.files_pending_approval,
            files_overdue: aggregate.files_overdue,
            last_scan_completed,
            files_healthy: aggregate.files_healthy,
            bytes_healthy: aggregate.bytes_healthy,
            bytes_within_warning: aggregate.bytes_within_warning,
            bytes_pending_approval: aggregate.bytes_pending_approval,
            bytes_overdue: aggregate.bytes_overdue,
            files_ignored: aggregate.files_ignored,
            bytes_ignored: aggregate.bytes_ignored,
        })
    }
}

#[derive(Debug)]
struct DedupedStatRow {
    path: String,
    root_id: i64,
    size_bytes: i64,
    countdown_start: Option<i64>,
    deferred_until: Option<i64>,
    status: String,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PathUrgency {
    #[default]
    None,
    Healthy,
    Warning,
    Pending,
    Overdue,
}

#[derive(Debug, Default)]
struct PathStatBucket {
    size_bytes: i64,
    has_active: bool,
    has_ignored_only: bool,
    urgency: PathUrgency,
}

impl PathStatBucket {
    fn observe_row(
        &mut self,
        row: &DedupedStatRow,
        expiration_days: u32,
        warning_days: u32,
        now: i64,
    ) {
        self.size_bytes = self.size_bytes.max(row.size_bytes);

        if row.status != "removed" && row.status != "ignored" {
            self.has_active = true;
            self.has_ignored_only = false;
        } else if row.status == "ignored" && !self.has_active {
            self.has_ignored_only = true;
        }

        let days_remaining = if row.status == "deferred" {
            row.deferred_until.map(|until| (until - now) / 86400)
        } else {
            row.countdown_start.map(|countdown_start| {
                let expiration_timestamp = countdown_start + (i64::from(expiration_days) * 86400);
                (expiration_timestamp - now) / 86400
            })
        };

        if row.status == "pending" {
            self.urgency = self.urgency.max(PathUrgency::Pending);
        }

        if row.status == "tracked"
            && let Some(days) = days_remaining
        {
            if days <= 0 {
                self.urgency = self.urgency.max(PathUrgency::Overdue);
            } else if days <= i64::from(warning_days) {
                self.urgency = self.urgency.max(PathUrgency::Warning);
            } else {
                self.urgency = self.urgency.max(PathUrgency::Healthy);
            }
        }

        if (row.status == "approved" || row.status == "pending")
            && let Some(days) = days_remaining
            && days <= 0
        {
            self.urgency = self.urgency.max(PathUrgency::Overdue);
        }
    }
}

#[derive(Debug, Default)]
struct DedupedStatAggregate {
    total_files: i64,
    total_size_bytes: i64,
    files_within_warning: i64,
    files_pending_approval: i64,
    files_overdue: i64,
    files_healthy: i64,
    bytes_healthy: i64,
    bytes_within_warning: i64,
    bytes_pending_approval: i64,
    bytes_overdue: i64,
    files_ignored: i64,
    bytes_ignored: i64,
}

impl DedupedStatAggregate {
    fn observe(&mut self, bucket: &PathStatBucket) {
        if bucket.has_active {
            self.total_files += 1;
            self.total_size_bytes += bucket.size_bytes;

            match bucket.urgency {
                PathUrgency::Overdue => {
                    self.files_overdue += 1;
                    self.bytes_overdue += bucket.size_bytes;
                }
                PathUrgency::Pending => {
                    self.files_pending_approval += 1;
                    self.bytes_pending_approval += bucket.size_bytes;
                }
                PathUrgency::Warning => {
                    self.files_within_warning += 1;
                    self.bytes_within_warning += bucket.size_bytes;
                }
                PathUrgency::Healthy => {
                    self.files_healthy += 1;
                    self.bytes_healthy += bucket.size_bytes;
                }
                PathUrgency::None => {}
            }
        } else if bucket.has_ignored_only {
            self.files_ignored += 1;
            self.bytes_ignored += bucket.size_bytes;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::NamedTempFile;

    use super::*;

    const LEGACY_V1_SCHEMA: &str = "
        CREATE TABLE roots (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE,
            added_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            last_scanned INTEGER,
            target_bytes INTEGER
        );

        CREATE TABLE entries (
            id INTEGER PRIMARY KEY,
            root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
            path TEXT NOT NULL UNIQUE,
            parent_path TEXT NOT NULL,
            is_dir INTEGER NOT NULL DEFAULT 0,
            size_bytes INTEGER NOT NULL DEFAULT 0,
            mtime INTEGER,
            tracked_since INTEGER,
            countdown_start INTEGER,
            status TEXT NOT NULL DEFAULT 'tracked'
                CHECK (status IN ('tracked', 'pending', 'approved', 'deferred', 'ignored', 'removed', 'blocked')),
            deferred_until INTEGER,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        );

        CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            user TEXT NOT NULL,
            action TEXT NOT NULL
                CHECK (action IN ('approve', 'unapprove', 'defer', 'ignore', 'unignore', 'remove', 'scan', 'undo', 'config_change')),
            target_path TEXT,
            details TEXT,
            entry_id INTEGER REFERENCES entries(id) ON DELETE SET NULL,
            actor_source TEXT,
            root_id INTEGER,
            outcome TEXT,
            status_before TEXT,
            status_after TEXT
        );

        CREATE INDEX idx_entries_root_id ON entries(root_id);
        CREATE INDEX idx_entries_parent_path ON entries(parent_path);
        CREATE INDEX idx_entries_status ON entries(status);
        CREATE INDEX idx_entries_mtime ON entries(mtime);
        CREATE INDEX idx_audit_log_timestamp ON audit_log(timestamp);
        CREATE INDEX idx_audit_log_action ON audit_log(action);

        CREATE TABLE stats (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            total_files INTEGER NOT NULL DEFAULT 0,
            total_size_bytes INTEGER NOT NULL DEFAULT 0,
            files_within_warning INTEGER NOT NULL DEFAULT 0,
            files_pending_approval INTEGER NOT NULL DEFAULT 0,
            files_overdue INTEGER NOT NULL DEFAULT 0,
            last_scan_completed INTEGER,
            updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        );

        INSERT OR IGNORE INTO stats (id) VALUES (1);
    ";

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
            "idx_entries_root_parent_path",
            "idx_entries_root_status",
            "idx_entries_root_is_dir_status",
            "idx_entries_mtime",
            "idx_entries_path",
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
    fn database_sets_schema_user_version() {
        let (_temp, db) = temp_database();

        let user_version: i64 = db
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("failed to query user_version");

        assert_eq!(user_version, SCHEMA_VERSION);
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
            .list_entries_by_parent(roots[0].id, Path::new("/test"))
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

        let user_version: i64 = db
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("failed to query user_version after reopen");
        assert_eq!(user_version, SCHEMA_VERSION);
    }

    #[test]
    fn database_open_rejects_future_schema_version() {
        let temp_file = NamedTempFile::new().expect("failed to create temp file");

        {
            let conn = Connection::open(temp_file.path()).expect("open raw sqlite db");
            conn.pragma_update(None, "journal_mode", "WAL")
                .expect("set wal mode");
            conn.pragma_update(None, "foreign_keys", "ON")
                .expect("enable foreign keys");
            conn.execute_batch(include_str!("schema.sql"))
                .expect("create schema");
            conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
                .expect("set future user_version");
        }

        let err = Database::open(temp_file.path())
            .err()
            .expect("future schema version should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("Unsupported database schema version"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn database_open_upgrades_legacy_unversioned_schema() {
        let temp_file = NamedTempFile::new().expect("failed to create temp file");

        {
            let conn = Connection::open(temp_file.path()).expect("open raw sqlite db");
            conn.pragma_update(None, "journal_mode", "WAL")
                .expect("set wal mode");
            conn.pragma_update(None, "foreign_keys", "ON")
                .expect("enable foreign keys");
            conn.execute_batch(LEGACY_V1_SCHEMA)
                .expect("create legacy schema");
            conn.execute("INSERT INTO roots (path) VALUES (?1)", ["/legacy-root"])
                .expect("insert legacy root");
            conn.pragma_update(None, "user_version", 0)
                .expect("clear user_version");
        }

        let db = Database::open(temp_file.path()).expect("open should upgrade legacy schema");
        let roots = db.list_roots().expect("list roots after upgrade");
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].path, PathBuf::from("/legacy-root"));

        let unique_indexes: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM pragma_index_list('entries')
                 WHERE name LIKE 'sqlite_autoindex_entries_%' AND [unique] = 1",
                [],
                |row| row.get(0),
            )
            .expect("query entries unique indexes");
        assert_eq!(
            unique_indexes, 1,
            "entries should have one unique autoindex"
        );

        let user_version: i64 = db
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("failed to query upgraded user_version");
        assert_eq!(user_version, SCHEMA_VERSION);
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
            .list_entries_by_parent(root_id, Path::new("/data/project1"))
            .expect("list");
        assert_eq!(entries_before.len(), 1);

        db.delete_root(root_id).expect("delete root");

        let entries_after = db
            .list_entries_by_parent(root_id, Path::new("/data/project1"))
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
    fn upsert_entry_allows_same_path_in_multiple_roots() {
        let (_temp, db) = temp_database();

        let parent_root_id = db
            .insert_root(Path::new("/data"))
            .expect("insert parent root");
        let nested_root_id = db
            .insert_root(Path::new("/data/project"))
            .expect("insert nested root");

        db.upsert_entry(
            parent_root_id,
            Path::new("/data/project/file.txt"),
            Path::new("/data/project"),
            false,
            100,
            Some(1000),
        )
        .expect("insert parent-root entry");
        db.upsert_entry(
            nested_root_id,
            Path::new("/data/project/file.txt"),
            Path::new("/data/project"),
            false,
            100,
            Some(1000),
        )
        .expect("insert nested-root entry");

        let parent_entry = db
            .get_entry_by_root_and_path(parent_root_id, Path::new("/data/project/file.txt"))
            .expect("query parent-root entry")
            .expect("parent-root entry should exist");
        let nested_entry = db
            .get_entry_by_root_and_path(nested_root_id, Path::new("/data/project/file.txt"))
            .expect("query nested-root entry")
            .expect("nested-root entry should exist");

        assert_eq!(parent_entry.root_id, parent_root_id);
        assert_eq!(nested_entry.root_id, nested_root_id);
        assert_ne!(parent_entry.id, nested_entry.id);
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
    fn upsert_entry_no_return_writes_entry() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        db.upsert_entry_no_return(
            root_id,
            Path::new("/data/file.txt"),
            Path::new("/data"),
            false,
            512,
            Some(1_700_000_000),
        )
        .expect("upsert without ID return should succeed");

        let entry = db
            .get_entry_by_path(Path::new("/data/file.txt"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(entry.root_id, root_id);
        assert_eq!(entry.size_bytes, 512);
        assert_eq!(entry.mtime, Some(1_700_000_000));
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
    fn upsert_entry_revives_removed_path_as_tracked() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/test"),
                Path::new("/data"),
                true,
                0,
                None,
            )
            .expect("first insert");
        db.update_entry_status(entry_id, "removed")
            .expect("mark removed");

        let removed_entry = db
            .get_entry_by_path(Path::new("/data/test"))
            .expect("query")
            .expect("entry should exist");
        let removed_tracked_since = removed_entry.tracked_since;

        std::thread::sleep(std::time::Duration::from_secs(1));

        db.upsert_entry(
            root_id,
            Path::new("/data/test"),
            Path::new("/data"),
            true,
            0,
            None,
        )
        .expect("upsert should revive removed entry");

        let revived_entry = db
            .get_entry_by_path(Path::new("/data/test"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(revived_entry.status, "tracked");
        assert_ne!(revived_entry.tracked_since, removed_tracked_since);
        assert!(revived_entry.countdown_start.is_some());
        assert_eq!(revived_entry.deferred_until, None);
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

        let entries = db
            .list_entries_by_parent(root_id, Path::new("/data"))
            .expect("list");

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

        let entries = db
            .list_entries_by_parent(root_id, Path::new("/data"))
            .expect("list");
        assert_eq!(entries.len(), 0, "removed entries should be excluded");
    }

    #[test]
    fn list_entries_by_parent_is_root_scoped_for_overlapping_paths() {
        let (_temp, db) = temp_database();

        let parent_root_id = db
            .insert_root(Path::new("/data"))
            .expect("insert parent root");
        let nested_root_id = db
            .insert_root(Path::new("/data/project"))
            .expect("insert nested root");

        db.upsert_entry(
            parent_root_id,
            Path::new("/data/project/file.txt"),
            Path::new("/data/project"),
            false,
            100,
            Some(1000),
        )
        .expect("insert parent-root entry");
        db.upsert_entry(
            nested_root_id,
            Path::new("/data/project/file.txt"),
            Path::new("/data/project"),
            false,
            100,
            Some(1000),
        )
        .expect("insert nested-root entry");

        let parent_entries = db
            .list_entries_by_parent(parent_root_id, Path::new("/data/project"))
            .expect("list parent entries");
        let nested_entries = db
            .list_entries_by_parent(nested_root_id, Path::new("/data/project"))
            .expect("list nested entries");

        assert_eq!(parent_entries.len(), 1);
        assert_eq!(nested_entries.len(), 1);
        assert_eq!(parent_entries[0].root_id, parent_root_id);
        assert_eq!(nested_entries[0].root_id, nested_root_id);
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

        let other_root_id = db
            .insert_root(Path::new("/data/archive"))
            .expect("insert overlapping root");
        db.upsert_entry(
            other_root_id,
            Path::new("/data/archive/old.txt"),
            Path::new("/data/archive"),
            false,
            999,
            Some(2000),
        )
        .expect("insert overlapping entry");

        let count = db
            .update_entries_by_path_prefix(root_id, Path::new("/data/archive"), "ignored")
            .expect("update");

        assert_eq!(count, 3, "should update directory and its children");

        let archive = db
            .get_entry_by_root_and_path(root_id, Path::new("/data/archive"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(archive.status, "ignored");

        let old = db
            .get_entry_by_root_and_path(root_id, Path::new("/data/archive/old.txt"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(old.status, "ignored");

        let keep = db
            .get_entry_by_root_and_path(root_id, Path::new("/data/keep.txt"))
            .expect("query")
            .expect("entry should exist");
        assert_eq!(
            keep.status, "tracked",
            "unrelated entry should not be affected"
        );

        let overlapping = db
            .list_entries_by_parent(other_root_id, Path::new("/data/archive"))
            .expect("query overlapping root entries");
        assert_eq!(overlapping.len(), 1);
        assert_eq!(overlapping[0].status, "tracked");
    }

    #[test]
    fn enforce_ignored_directory_inheritance_ignores_descendants() {
        let (_temp, db) = temp_database();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");

        // Ignored directory
        db.upsert_entry(
            root_id,
            Path::new("/data/archive"),
            Path::new("/data"),
            true,
            0,
            None,
        )
        .expect("insert ignored dir");
        let ignored_dir_id = db
            .get_entry_by_path(Path::new("/data/archive"))
            .expect("query ignored dir")
            .expect("ignored dir should exist")
            .id;
        db.update_entry_status(ignored_dir_id, "ignored")
            .expect("mark dir ignored");

        // Descendant file starts tracked.
        db.upsert_entry(
            root_id,
            Path::new("/data/archive/new.txt"),
            Path::new("/data/archive"),
            false,
            100,
            Some(1000),
        )
        .expect("insert descendant file");

        // Unrelated file remains tracked.
        db.upsert_entry(
            root_id,
            Path::new("/data/keep.txt"),
            Path::new("/data"),
            false,
            200,
            Some(1000),
        )
        .expect("insert unrelated file");

        // Removed descendant should remain removed.
        let removed_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/archive/removed.txt"),
                Path::new("/data/archive"),
                false,
                50,
                Some(1000),
            )
            .expect("insert removable file");
        db.update_entry_status(removed_id, "removed")
            .expect("mark removed");

        let updated = db
            .enforce_ignored_directory_inheritance(root_id)
            .expect("enforce inheritance");
        assert!(updated >= 2, "directory and descendant should be ignored");

        let descendant = db
            .get_entry_by_path(Path::new("/data/archive/new.txt"))
            .expect("query descendant")
            .expect("descendant should exist");
        assert_eq!(descendant.status, "ignored");

        let unrelated = db
            .get_entry_by_path(Path::new("/data/keep.txt"))
            .expect("query unrelated")
            .expect("unrelated should exist");
        assert_eq!(unrelated.status, "tracked");

        let removed = db
            .get_entry_by_path(Path::new("/data/archive/removed.txt"))
            .expect("query removed")
            .expect("removed should exist");
        assert_eq!(removed.status, "removed");
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

    #[test]
    fn compute_live_stats_deduplicates_overlapping_paths() {
        let (_temp, db) = temp_database();

        let parent_root_id = db
            .insert_root(Path::new("/data"))
            .expect("insert parent root");
        let nested_root_id = db
            .insert_root(Path::new("/data/project"))
            .expect("insert nested root");

        db.upsert_entry(
            parent_root_id,
            Path::new("/data/top.bin"),
            Path::new("/data"),
            false,
            10,
            Some(1000),
        )
        .expect("insert parent-only file");
        let parent_shared_id = db
            .upsert_entry(
                parent_root_id,
                Path::new("/data/project/shared.bin"),
                Path::new("/data/project"),
                false,
                20,
                Some(1000),
            )
            .expect("insert parent shared file");
        let nested_shared_id = db
            .upsert_entry(
                nested_root_id,
                Path::new("/data/project/shared.bin"),
                Path::new("/data/project"),
                false,
                20,
                Some(1000),
            )
            .expect("insert nested shared file");

        let old_countdown = jiff::Timestamp::now().as_second() - (100 * 86400);
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE id = ?2",
                rusqlite::params![old_countdown, parent_shared_id],
            )
            .expect("backdate parent shared countdown");
        db.conn()
            .execute(
                "UPDATE entries SET countdown_start = ?1 WHERE id = ?2",
                rusqlite::params![old_countdown, nested_shared_id],
            )
            .expect("backdate nested shared countdown");

        let stats = db
            .compute_live_stats_with_root_configs(&[
                RootStatConfig {
                    root_id: parent_root_id,
                    expiration_days: 90,
                    warning_days: 14,
                },
                RootStatConfig {
                    root_id: nested_root_id,
                    expiration_days: 90,
                    warning_days: 14,
                },
            ])
            .expect("compute deduped live stats");

        assert_eq!(
            stats.total_files, 2,
            "shared path should count once globally"
        );
        assert_eq!(
            stats.total_size_bytes, 30,
            "shared bytes should count once globally"
        );
        assert_eq!(
            stats.files_overdue, 1,
            "shared overdue file should count once globally"
        );
    }

    #[test]
    fn compute_live_stats_prefers_active_rows_over_ignored_overlap_rows() {
        let (_temp, db) = temp_database();

        let parent_root_id = db
            .insert_root(Path::new("/data"))
            .expect("insert parent root");
        let nested_root_id = db
            .insert_root(Path::new("/data/project"))
            .expect("insert nested root");

        db.upsert_entry(
            parent_root_id,
            Path::new("/data/project/shared.bin"),
            Path::new("/data/project"),
            false,
            20,
            Some(1000),
        )
        .expect("insert parent shared file");
        let nested_shared_id = db
            .upsert_entry(
                nested_root_id,
                Path::new("/data/project/shared.bin"),
                Path::new("/data/project"),
                false,
                20,
                Some(1000),
            )
            .expect("insert nested shared file");
        db.update_entry_status(nested_shared_id, "ignored")
            .expect("ignore nested shared file");

        let stats = db
            .compute_live_stats_with_root_configs(&[
                RootStatConfig {
                    root_id: parent_root_id,
                    expiration_days: 90,
                    warning_days: 14,
                },
                RootStatConfig {
                    root_id: nested_root_id,
                    expiration_days: 90,
                    warning_days: 14,
                },
            ])
            .expect("compute deduped live stats");

        assert_eq!(
            stats.total_files, 1,
            "active overlapping file should remain globally tracked"
        );
        assert_eq!(
            stats.files_ignored, 0,
            "ignored duplicate should not hide active path"
        );
    }

    #[test]
    fn list_entries_by_root_and_status_filters_correctly() {
        let (_temp, db) = temp_database();
        let now = jiff::Timestamp::now().as_second();

        let root_id = db.insert_root(Path::new("/data")).expect("insert root");
        let other_root_id = db
            .insert_root(Path::new("/other"))
            .expect("insert other root");

        // Create entries under /data with various statuses
        let id1 = db
            .upsert_entry(
                root_id,
                Path::new("/data/approved1.txt"),
                Path::new("/data"),
                false,
                100,
                Some(now),
            )
            .expect("upsert entry");
        let id2 = db
            .upsert_entry(
                root_id,
                Path::new("/data/approved2.txt"),
                Path::new("/data"),
                false,
                200,
                Some(now),
            )
            .expect("upsert entry");
        let id3 = db
            .upsert_entry(
                root_id,
                Path::new("/data/tracked.txt"),
                Path::new("/data"),
                false,
                300,
                Some(now),
            )
            .expect("upsert entry");

        // Create an approved entry under a different root
        let id4 = db
            .upsert_entry(
                other_root_id,
                Path::new("/other/approved.txt"),
                Path::new("/other"),
                false,
                400,
                Some(now),
            )
            .expect("upsert entry");

        db.update_entry_status(id1, "approved")
            .expect("update status");
        db.update_entry_status(id2, "approved")
            .expect("update status");
        // id3 stays as "tracked"
        let _ = id3;
        db.update_entry_status(id4, "approved")
            .expect("update status");

        // Query approved entries for root_id only
        let approved = db
            .list_entries_by_root_and_status(root_id, "approved")
            .expect("query should succeed");
        assert_eq!(
            approved.len(),
            2,
            "Should return only approved entries for the specified root"
        );
        assert!(approved.iter().all(|e| e.status == "approved"));
        assert!(approved.iter().all(|e| e.root_id == root_id));

        // Query tracked entries for root_id
        let tracked = db
            .list_entries_by_root_and_status(root_id, "tracked")
            .expect("query should succeed");
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].path, Path::new("/data/tracked.txt"));

        // Query approved entries for other_root_id
        let other_approved = db
            .list_entries_by_root_and_status(other_root_id, "approved")
            .expect("query should succeed");
        assert_eq!(other_approved.len(), 1);
        assert_eq!(other_approved[0].path, Path::new("/other/approved.txt"));

        // Query with no matching entries
        let empty = db
            .list_entries_by_root_and_status(root_id, "ignored")
            .expect("query should succeed");
        assert!(empty.is_empty(), "No ignored entries should exist");
    }
}
