//! Database schema, queries, and migrations.

use std::path::Path;

use rusqlite::Connection;

use crate::error::{Error, Result};

/// A tracked directory with metadata and status.
///
/// Represents a directory being monitored by stagecrew. Contains aggregated
/// statistics about files within the directory and the current lifecycle status.
// TODO(cleanup): Remove allow once Directory is used by scanner/TUI modules.
#[allow(dead_code)]
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Directory {
    /// Unique database identifier.
    pub id: i64,
    /// Absolute path to the directory.
    pub path: String,
    /// Total size of all files in bytes.
    pub size_bytes: i64,
    /// Number of files in the directory.
    pub file_count: i64,
    /// Unix timestamp of the oldest file's mtime, or None if empty.
    pub oldest_mtime: Option<i64>,
    /// Unix timestamp of the last scan.
    pub last_scanned: Option<i64>,
    /// Current status in the lifecycle.
    pub status: String,
    /// Unix timestamp when deferral expires, or None if not deferred.
    pub deferred_until: Option<i64>,
    /// Unix timestamp when record was created.
    pub created_at: i64,
    /// Unix timestamp when record was last updated.
    pub updated_at: i64,
}

/// A file within a tracked directory.
///
/// Represents an individual file with its metadata. Files are associated with
/// a parent directory via `directory_id`.
// TODO(cleanup): Remove allow once File is used by scanner/TUI modules.
#[allow(dead_code)]
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    /// Unique database identifier.
    pub id: i64,
    /// Foreign key to the parent directory.
    pub directory_id: i64,
    /// Absolute path to the file.
    pub path: String,
    /// File size in bytes.
    pub size_bytes: i64,
    /// Unix timestamp of the file's modification time.
    pub mtime: i64,
    /// Unix timestamp when record was created.
    pub created_at: i64,
}

/// Pre-computed statistics for shell hooks and status display.
///
/// Stored in the singleton `stats` table row, updated after each scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Total number of tracked directories.
    pub total_tracked_paths: i64,
    /// Total size of all tracked files in bytes.
    pub total_size_bytes: i64,
    /// Number of paths within the warning period.
    pub paths_within_warning: i64,
    /// Number of paths pending approval for removal.
    pub paths_pending_approval: i64,
    /// Number of paths that are overdue (past expiration).
    pub paths_overdue: i64,
    /// Unix timestamp of the last completed scan.
    pub last_scan_completed: Option<i64>,
}

/// Database handle for stagecrew state.
///
/// Manages the `SQLite` database that stores tracked directories, files,
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
    // TODO(cleanup): Remove allow once conn() is used by service layer.
    #[allow(dead_code)]
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Insert or update a directory in the database.
    ///
    /// Uses UPSERT (INSERT ... ON CONFLICT) to either create a new directory
    /// record or update an existing one based on the path. The `updated_at`
    /// timestamp is automatically set to the current time on updates.
    ///
    /// # Arguments
    ///
    /// * `path` - Absolute path to the directory
    /// * `size_bytes` - Total size of all files in bytes
    /// * `file_count` - Number of files in the directory
    /// * `oldest_mtime` - Unix timestamp of the oldest file, or None if empty
    /// * `last_scanned` - Unix timestamp of when this scan occurred
    ///
    /// # Returns
    ///
    /// The database row ID of the inserted or updated directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    // TODO(cleanup): Remove allow once used by scanner module.
    #[allow(dead_code)]
    pub fn insert_or_update_directory(
        &self,
        path: &str,
        size_bytes: i64,
        file_count: i64,
        oldest_mtime: Option<i64>,
        last_scanned: i64,
    ) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO directories (path, size_bytes, file_count, oldest_mtime, last_scanned)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(path) DO UPDATE SET
                 size_bytes = excluded.size_bytes,
                 file_count = excluded.file_count,
                 oldest_mtime = excluded.oldest_mtime,
                 last_scanned = excluded.last_scanned,
                 updated_at = strftime('%s', 'now')",
            (path, size_bytes, file_count, oldest_mtime, last_scanned),
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Insert or update a file in the database.
    ///
    /// Uses UPSERT (INSERT ... ON CONFLICT) to either create a new file
    /// record or update an existing one based on the path.
    ///
    /// # Arguments
    ///
    /// * `directory_id` - Foreign key to the parent directory
    /// * `path` - Absolute path to the file
    /// * `size_bytes` - File size in bytes
    /// * `mtime` - Unix timestamp of the file's modification time
    ///
    /// # Returns
    ///
    /// The database row ID of the inserted or updated file.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `directory_id` does not exist (foreign key constraint)
    /// - The database operation fails
    // TODO(cleanup): Remove allow once used by scanner module.
    #[allow(dead_code)]
    pub fn insert_or_update_file(
        &self,
        directory_id: i64,
        path: &str,
        size_bytes: i64,
        mtime: i64,
    ) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO files (directory_id, path, size_bytes, mtime)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(path) DO UPDATE SET
                 directory_id = excluded.directory_id,
                 size_bytes = excluded.size_bytes,
                 mtime = excluded.mtime",
            (directory_id, path, size_bytes, mtime),
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Get a directory by its path.
    ///
    /// # Returns
    ///
    /// `Some(Directory)` if found, `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // TODO(cleanup): Remove allow once used by TUI/daemon modules.
    #[allow(dead_code)]
    pub fn get_directory_by_path(&self, path: &str) -> Result<Option<Directory>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, path, size_bytes, file_count, oldest_mtime, last_scanned,
                    status, deferred_until, created_at, updated_at
             FROM directories
             WHERE path = ?1",
        )?;

        let mut rows = stmt.query([path])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Directory {
                id: row.get(0)?,
                path: row.get(1)?,
                size_bytes: row.get(2)?,
                file_count: row.get(3)?,
                oldest_mtime: row.get(4)?,
                last_scanned: row.get(5)?,
                status: row.get(6)?,
                deferred_until: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
            }))
        } else {
            Ok(None)
        }
    }

    /// List all directories, optionally filtered by status.
    ///
    /// # Arguments
    ///
    /// * `status_filter` - If provided, only return directories with this status
    ///
    /// # Returns
    ///
    /// A vector of all matching directories, ordered by path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // TODO(cleanup): Remove allow once used by TUI module.
    #[allow(dead_code)]
    pub fn list_directories(&self, status_filter: Option<&str>) -> Result<Vec<Directory>> {
        // Helper to map database rows to Directory structs
        let row_to_directory = |row: &rusqlite::Row<'_>| -> rusqlite::Result<Directory> {
            Ok(Directory {
                id: row.get(0)?,
                path: row.get(1)?,
                size_bytes: row.get(2)?,
                file_count: row.get(3)?,
                oldest_mtime: row.get(4)?,
                last_scanned: row.get(5)?,
                status: row.get(6)?,
                deferred_until: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
            })
        };

        let query = if status_filter.is_some() {
            "SELECT id, path, size_bytes, file_count, oldest_mtime, last_scanned,
                    status, deferred_until, created_at, updated_at
             FROM directories
             WHERE status = ?1
             ORDER BY path"
        } else {
            "SELECT id, path, size_bytes, file_count, oldest_mtime, last_scanned,
                    status, deferred_until, created_at, updated_at
             FROM directories
             ORDER BY path"
        };

        let mut stmt = self.conn.prepare(query)?;

        let rows = if let Some(status) = status_filter {
            stmt.query_map([status], row_to_directory)?
        } else {
            stmt.query_map([], row_to_directory)?
        };

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// List all files within a directory.
    ///
    /// # Arguments
    ///
    /// * `directory_id` - The ID of the parent directory
    ///
    /// # Returns
    ///
    /// A vector of all files in the directory, ordered by path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // TODO(cleanup): Remove allow once used by TUI module.
    #[allow(dead_code)]
    pub fn list_files_by_directory(&self, directory_id: i64) -> Result<Vec<File>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, directory_id, path, size_bytes, mtime, created_at
             FROM files
             WHERE directory_id = ?1
             ORDER BY path",
        )?;

        let rows = stmt.query_map([directory_id], |row| {
            Ok(File {
                id: row.get(0)?,
                directory_id: row.get(1)?,
                path: row.get(2)?,
                size_bytes: row.get(3)?,
                mtime: row.get(4)?,
                created_at: row.get(5)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Update the status of a directory.
    ///
    /// Changes the directory's status and automatically updates the `updated_at`
    /// timestamp to the current time.
    ///
    /// # Arguments
    ///
    /// * `directory_id` - The ID of the directory to update
    /// * `new_status` - The new status value
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `directory_id` does not exist
    /// - The `new_status` violates the CHECK constraint
    /// - The database operation fails
    // TODO(cleanup): Remove allow once used by TUI/daemon modules.
    #[allow(dead_code)]
    pub fn update_directory_status(&self, directory_id: i64, new_status: &str) -> Result<()> {
        let rows_affected = self.conn.execute(
            "UPDATE directories
             SET status = ?1, updated_at = strftime('%s', 'now')
             WHERE id = ?2",
            (new_status, directory_id),
        )?;

        if rows_affected == 0 {
            return Err(Error::Config(format!(
                "Directory with id {directory_id} not found"
            )));
        }

        Ok(())
    }

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
                "SELECT total_tracked_paths, total_size_bytes, paths_within_warning,
                        paths_pending_approval, paths_overdue, last_scan_completed
                 FROM stats WHERE id = 1",
                [],
                |row| {
                    Ok(Stats {
                        total_tracked_paths: row.get(0)?,
                        total_size_bytes: row.get(1)?,
                        paths_within_warning: row.get(2)?,
                        paths_pending_approval: row.get(3)?,
                        paths_overdue: row.get(4)?,
                        last_scan_completed: row.get(5)?,
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

        // Verify tables exist by querying sqlite_master
        let tables: Vec<String> = db
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .expect("failed to prepare query")
            .query_map([], |row| row.get(0))
            .expect("failed to query")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("failed to collect");

        assert!(
            tables.contains(&"directories".to_string()),
            "missing 'directories' table, found: {tables:?}"
        );
        assert!(
            tables.contains(&"files".to_string()),
            "missing 'files' table, found: {tables:?}"
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

        // Verify all expected indexes exist
        let expected = [
            "idx_directories_status",
            "idx_directories_oldest_mtime",
            "idx_files_directory_id",
            "idx_files_mtime",
            "idx_audit_log_timestamp",
            "idx_audit_log_user",
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

        // First open: creates schema and inserts test data
        {
            let db = Database::open(temp_file.path()).expect("first open");
            db.conn
                .execute("INSERT INTO directories (path) VALUES ('/test')", [])
                .expect("insert test data");
        }

        // Second open: should not fail or lose data
        let db = Database::open(temp_file.path()).expect("second open");

        // Verify data persisted across opens
        let dir_count: i32 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM directories WHERE path = '/test'",
                [],
                |row| row.get(0),
            )
            .expect("query directories");
        assert_eq!(dir_count, 1, "data should persist across opens");

        // Verify stats singleton unchanged (INSERT OR IGNORE)
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
    fn foreign_key_constraint_prevents_orphan_files() {
        let (_temp, db) = temp_database();

        // Attempt to insert a file referencing non-existent directory
        let result = db.conn.execute(
            "INSERT INTO files (directory_id, path, size_bytes, mtime) VALUES (999, '/orphan', 100, 0)",
            [],
        );

        assert!(
            result.is_err(),
            "foreign key should prevent inserting file with invalid directory_id"
        );
    }

    #[test]
    fn directories_rejects_invalid_status() {
        let (_temp, db) = temp_database();

        let result = db.conn.execute(
            "INSERT INTO directories (path, status) VALUES ('/test', 'invalid_status')",
            [],
        );

        assert!(
            result.is_err(),
            "CHECK constraint should reject invalid status value"
        );
    }

    #[test]
    fn stats_enforces_singleton_constraint() {
        let (_temp, db) = temp_database();

        // Attempt to insert a second row with id != 1
        let result = db.conn.execute("INSERT INTO stats (id) VALUES (2)", []);

        assert!(
            result.is_err(),
            "CHECK (id = 1) should reject stats row with id != 1"
        );
    }

    // === CRUD Operation Tests ===

    #[test]
    fn insert_or_update_directory_creates_new() {
        let (_temp, db) = temp_database();

        let id = db
            .insert_or_update_directory(
                "/data/project1",
                1024,
                5,
                Some(1_700_000_000),
                1_700_100_000,
            )
            .expect("insert should succeed");

        let dir = db
            .get_directory_by_path("/data/project1")
            .expect("query should succeed")
            .expect("directory should exist");

        assert_eq!(dir.id, id);
        assert_eq!(dir.path, "/data/project1");
        assert_eq!(dir.size_bytes, 1024);
        assert_eq!(dir.file_count, 5);
        assert_eq!(dir.oldest_mtime, Some(1_700_000_000));
        assert_eq!(dir.last_scanned, Some(1_700_100_000));
        assert_eq!(dir.status, "tracked");
        assert_eq!(dir.deferred_until, None);
    }

    #[test]
    fn insert_or_update_directory_updates_existing() {
        let (_temp, db) = temp_database();

        // Insert initial directory
        let id1 = db
            .insert_or_update_directory(
                "/data/project1",
                1024,
                5,
                Some(1_700_000_000),
                1_700_100_000,
            )
            .expect("insert should succeed");

        // Update same directory with new data
        let id2 = db
            .insert_or_update_directory(
                "/data/project1",
                2048,
                10,
                Some(1_700_050_000),
                1_700_200_000,
            )
            .expect("update should succeed");

        // ID should be same (upsert)
        assert_eq!(id1, id2);

        let dir = db
            .get_directory_by_path("/data/project1")
            .expect("query should succeed")
            .expect("directory should exist");

        assert_eq!(dir.size_bytes, 2048);
        assert_eq!(dir.file_count, 10);
        assert_eq!(dir.oldest_mtime, Some(1_700_050_000));
        assert_eq!(dir.last_scanned, Some(1_700_200_000));
    }

    #[test]
    fn get_directory_by_path_returns_none_when_not_found() {
        let (_temp, db) = temp_database();

        let result = db
            .get_directory_by_path("/nonexistent")
            .expect("query should succeed");

        assert_eq!(result, None);
    }

    #[test]
    fn list_directories_returns_all() {
        let (_temp, db) = temp_database();

        db.insert_or_update_directory("/data/project1", 1024, 5, None, 1_700_100_000)
            .expect("insert");
        db.insert_or_update_directory("/data/project2", 2048, 10, None, 1_700_100_000)
            .expect("insert");
        db.insert_or_update_directory("/data/project3", 512, 2, None, 1_700_100_000)
            .expect("insert");

        let dirs = db.list_directories(None).expect("query should succeed");

        assert_eq!(dirs.len(), 3);
        // Should be ordered by path
        assert_eq!(dirs[0].path, "/data/project1");
        assert_eq!(dirs[1].path, "/data/project2");
        assert_eq!(dirs[2].path, "/data/project3");
    }

    #[test]
    fn list_directories_filters_by_status() {
        let (_temp, db) = temp_database();

        let id1 = db
            .insert_or_update_directory("/data/project1", 1024, 5, None, 1_700_100_000)
            .expect("insert");
        let id2 = db
            .insert_or_update_directory("/data/project2", 2048, 10, None, 1_700_100_000)
            .expect("insert");
        db.insert_or_update_directory("/data/project3", 512, 2, None, 1_700_100_000)
            .expect("insert");

        // Update status of first two
        db.update_directory_status(id1, "pending").expect("update");
        db.update_directory_status(id2, "pending").expect("update");

        let pending = db
            .list_directories(Some("pending"))
            .expect("query should succeed");

        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].path, "/data/project1");
        assert_eq!(pending[1].path, "/data/project2");

        let tracked = db
            .list_directories(Some("tracked"))
            .expect("query should succeed");

        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].path, "/data/project3");
    }

    #[test]
    fn update_directory_status_changes_status_and_timestamp() {
        let (_temp, db) = temp_database();

        let id = db
            .insert_or_update_directory("/data/project1", 1024, 5, None, 1_700_100_000)
            .expect("insert");

        let dir_before = db
            .get_directory_by_path("/data/project1")
            .expect("query")
            .expect("exists");
        let updated_at_before = dir_before.updated_at;

        // Delay to ensure timestamp changes (SQLite strftime has 1-second resolution)
        std::thread::sleep(std::time::Duration::from_secs(1));

        db.update_directory_status(id, "approved")
            .expect("update should succeed");

        let dir_after = db
            .get_directory_by_path("/data/project1")
            .expect("query")
            .expect("exists");

        assert_eq!(dir_after.status, "approved");
        assert!(
            dir_after.updated_at > updated_at_before,
            "updated_at should have changed"
        );
    }

    #[test]
    fn update_directory_status_fails_on_nonexistent_id() {
        let (_temp, db) = temp_database();

        let result = db.update_directory_status(999, "approved");

        assert!(result.is_err(), "should fail on nonexistent directory");
        match result {
            Err(Error::Config(msg)) => assert!(msg.contains("not found")),
            _ => panic!("expected Config error with 'not found' message"),
        }
    }

    #[test]
    fn update_directory_status_rejects_invalid_status() {
        let (_temp, db) = temp_database();

        let id = db
            .insert_or_update_directory("/data/project1", 1024, 5, None, 1_700_100_000)
            .expect("insert");

        let result = db.update_directory_status(id, "invalid_status");

        assert!(
            result.is_err(),
            "should fail on invalid status via CHECK constraint"
        );
    }

    #[test]
    fn insert_or_update_file_creates_new() {
        let (_temp, db) = temp_database();

        let dir_id = db
            .insert_or_update_directory("/data/project1", 0, 0, None, 1_700_100_000)
            .expect("insert directory");

        let file_id = db
            .insert_or_update_file(dir_id, "/data/project1/file.txt", 512, 1_700_000_000)
            .expect("insert file should succeed");

        let files = db
            .list_files_by_directory(dir_id)
            .expect("query should succeed");

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, file_id);
        assert_eq!(files[0].path, "/data/project1/file.txt");
        assert_eq!(files[0].size_bytes, 512);
        assert_eq!(files[0].mtime, 1_700_000_000);
    }

    #[test]
    fn insert_or_update_file_updates_existing() {
        let (_temp, db) = temp_database();

        let dir_id = db
            .insert_or_update_directory("/data/project1", 0, 0, None, 1_700_100_000)
            .expect("insert directory");

        let file_id1 = db
            .insert_or_update_file(dir_id, "/data/project1/file.txt", 512, 1_700_000_000)
            .expect("insert file");

        let file_id2 = db
            .insert_or_update_file(dir_id, "/data/project1/file.txt", 1024, 1_700_050_000)
            .expect("update file");

        // ID should be same (upsert)
        assert_eq!(file_id1, file_id2);

        let files = db
            .list_files_by_directory(dir_id)
            .expect("query should succeed");

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].size_bytes, 1024);
        assert_eq!(files[0].mtime, 1_700_050_000);
    }

    #[test]
    fn insert_or_update_file_fails_with_invalid_directory_id() {
        let (_temp, db) = temp_database();

        let result = db.insert_or_update_file(999, "/data/project1/file.txt", 512, 1_700_000_000);

        assert!(result.is_err(), "should fail due to foreign key constraint");
    }

    #[test]
    fn list_files_by_directory_returns_empty_for_nonexistent_directory() {
        let (_temp, db) = temp_database();

        let files = db
            .list_files_by_directory(999)
            .expect("query should succeed");

        assert_eq!(files.len(), 0);
    }

    #[test]
    fn list_files_by_directory_returns_all_files() {
        let (_temp, db) = temp_database();

        let dir_id = db
            .insert_or_update_directory("/data/project1", 0, 0, None, 1_700_100_000)
            .expect("insert directory");

        db.insert_or_update_file(dir_id, "/data/project1/a.txt", 100, 1_700_000_000)
            .expect("insert");
        db.insert_or_update_file(dir_id, "/data/project1/b.txt", 200, 1_700_000_000)
            .expect("insert");
        db.insert_or_update_file(dir_id, "/data/project1/c.txt", 300, 1_700_000_000)
            .expect("insert");

        let files = db
            .list_files_by_directory(dir_id)
            .expect("query should succeed");

        assert_eq!(files.len(), 3);
        // Should be ordered by path
        assert_eq!(files[0].path, "/data/project1/a.txt");
        assert_eq!(files[1].path, "/data/project1/b.txt");
        assert_eq!(files[2].path, "/data/project1/c.txt");
    }

    #[test]
    fn cascade_delete_removes_files_when_directory_deleted() {
        let (_temp, db) = temp_database();

        let dir_id = db
            .insert_or_update_directory("/data/project1", 0, 0, None, 1_700_100_000)
            .expect("insert directory");

        db.insert_or_update_file(dir_id, "/data/project1/file.txt", 100, 1_700_000_000)
            .expect("insert file");

        let files_before = db.list_files_by_directory(dir_id).expect("query");
        assert_eq!(files_before.len(), 1);

        // Delete directory
        db.conn
            .execute("DELETE FROM directories WHERE id = ?1", [dir_id])
            .expect("delete directory");

        // Files should be cascaded
        let files_after = db.list_files_by_directory(dir_id).expect("query");
        assert_eq!(files_after.len(), 0);
    }

    #[test]
    fn insert_or_update_directory_preserves_status_on_update() {
        let (_temp, db) = temp_database();

        // Insert directory with default 'tracked' status
        let id = db
            .insert_or_update_directory(
                "/data/project1",
                100,
                1,
                Some(1_700_000_000),
                1_700_100_000,
            )
            .expect("insert");

        // Change status to 'approved'
        db.update_directory_status(id, "approved")
            .expect("update status");

        let dir_before = db
            .get_directory_by_path("/data/project1")
            .expect("query")
            .expect("exists");
        assert_eq!(dir_before.status, "approved");

        // Re-scan with new data (simulates periodic scan updates)
        db.insert_or_update_directory("/data/project1", 200, 2, Some(1_700_050_000), 1_700_200_000)
            .expect("upsert");

        let dir_after = db
            .get_directory_by_path("/data/project1")
            .expect("query")
            .expect("exists");

        // Status should NOT be reset by the upsert
        assert_eq!(
            dir_after.status, "approved",
            "upsert should not reset status to 'tracked'"
        );

        // But scan data should be updated
        assert_eq!(dir_after.size_bytes, 200);
        assert_eq!(dir_after.file_count, 2);
        assert_eq!(dir_after.oldest_mtime, Some(1_700_050_000));
        assert_eq!(dir_after.last_scanned, Some(1_700_200_000));
    }

    #[test]
    fn insert_or_update_directory_preserves_deferred_until_on_update() {
        let (_temp, db) = temp_database();

        let id = db
            .insert_or_update_directory("/data/project1", 100, 1, None, 1_700_100_000)
            .expect("insert");

        // Manually set deferred_until (simulates a deferral action)
        db.conn
            .execute(
                "UPDATE directories SET deferred_until = ?1 WHERE id = ?2",
                [1_700_500_000, id],
            )
            .expect("set deferred_until");

        let dir_before = db
            .get_directory_by_path("/data/project1")
            .expect("query")
            .expect("exists");
        assert_eq!(dir_before.deferred_until, Some(1_700_500_000));

        // Re-scan with new data
        db.insert_or_update_directory("/data/project1", 200, 2, None, 1_700_200_000)
            .expect("upsert");

        let dir_after = db
            .get_directory_by_path("/data/project1")
            .expect("query")
            .expect("exists");

        // deferred_until should NOT be reset by the upsert
        assert_eq!(
            dir_after.deferred_until,
            Some(1_700_500_000),
            "upsert should not clear deferred_until"
        );
    }
}
