//! Database schema, queries, and migrations.

use std::path::Path;

use rusqlite::Connection;

use crate::error::{Error, Result};

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
}
