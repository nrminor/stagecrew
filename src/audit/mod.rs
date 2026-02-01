//! Audit trail logging and queries.

use rusqlite::params;

use crate::db::Database;
use crate::error::Result;

/// Actions that can be recorded in the audit log.
// TODO(cleanup): Remove allow once TUI, daemon, or scanner modules use this type.
// This is part of the public audit API for US-005.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditAction {
    Approve,
    Defer,
    Ignore,
    Remove,
    Scan,
    ConfigChange,
}

impl AuditAction {
    // TODO(cleanup): Remove allow once record() is called by other modules.
    #[allow(dead_code)]
    fn as_str(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Defer => "defer",
            Self::Ignore => "ignore",
            Self::Remove => "remove",
            Self::Scan => "scan",
            Self::ConfigChange => "config_change",
        }
    }
}

/// Service for recording and querying audit events.
// TODO(cleanup): Remove allow once TUI, daemon, or scanner modules instantiate this.
// This is the main service struct for US-005.
#[allow(dead_code)]
pub struct AuditService<'a> {
    db: &'a Database,
}

impl<'a> AuditService<'a> {
    /// Create a new audit service.
    // TODO(cleanup): Remove allow once other modules use this constructor.
    #[allow(dead_code)]
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    /// Record an audit event.
    ///
    /// Inserts a new entry into the audit log with the current timestamp.
    /// This provides an immutable record of who performed what action, when,
    /// and on which path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database insert fails.
    // TODO(cleanup): Remove allow once TUI/daemon record actions.
    #[allow(dead_code)]
    pub fn record(
        &self,
        user: &str,
        action: AuditAction,
        target_path: Option<&str>,
        details: Option<&str>,
        directory_id: Option<i64>,
    ) -> Result<()> {
        self.db.conn().execute(
            "INSERT INTO audit_log (user, action, target_path, details, directory_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![user, action.as_str(), target_path, details, directory_id],
        )?;
        Ok(())
    }

    /// Get the current username from the environment.
    ///
    /// Checks `$USER` and `$LOGNAME` environment variables in that order.
    /// Returns `"unknown"` if neither is set.
    // TODO(cleanup): Remove allow once other modules call this for audit records.
    #[allow(dead_code)]
    #[must_use]
    pub fn current_user() -> String {
        std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .unwrap_or_else(|_| "unknown".to_string())
    }

    /// List the most recent audit entries.
    ///
    /// Returns up to `limit` audit entries, ordered by timestamp descending
    /// (most recent first). Useful for displaying recent activity in the TUI.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // TODO(cleanup): Remove allow once TUI audit log view calls this.
    #[allow(dead_code)]
    pub fn list_recent(&self, limit: usize) -> Result<Vec<AuditEntry>> {
        let mut stmt = self.db.conn().prepare(
            "SELECT id, timestamp, user, action, target_path, details, directory_id
             FROM audit_log
             ORDER BY timestamp DESC
             LIMIT ?1",
        )?;

        // Allow: usize -> i64 cast is safe for realistic limit values.
        // SQLite LIMIT accepts i64 and we won't have limits exceeding i64::MAX.
        #[allow(clippy::cast_possible_wrap)]
        let entries = stmt
            .query_map(params![limit as i64], |row| {
                Ok(AuditEntry {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    user: row.get(2)?,
                    action: row.get(3)?,
                    target_path: row.get(4)?,
                    details: row.get(5)?,
                    directory_id: row.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(entries)
    }

    /// List audit entries for a specific path.
    ///
    /// Returns all audit entries where `target_path` matches the given path,
    /// ordered by timestamp descending. Useful for viewing the history of
    /// actions performed on a particular directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // TODO(cleanup): Remove allow once TUI detail view calls this.
    #[allow(dead_code)]
    pub fn list_by_path(&self, path: &str) -> Result<Vec<AuditEntry>> {
        let mut stmt = self.db.conn().prepare(
            "SELECT id, timestamp, user, action, target_path, details, directory_id
             FROM audit_log
             WHERE target_path = ?1
             ORDER BY timestamp DESC",
        )?;

        let entries = stmt
            .query_map(params![path], |row| {
                Ok(AuditEntry {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    user: row.get(2)?,
                    action: row.get(3)?,
                    target_path: row.get(4)?,
                    details: row.get(5)?,
                    directory_id: row.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(entries)
    }
}

/// A recorded audit event.
///
/// Note: The `action` field is a `String` (the raw database value) rather than
/// `AuditAction` to maintain flexibility. This allows the database to contain
/// historical actions that may not be present in the current enum definition.
// TODO(cleanup): Remove allow once TUI displays audit entries.
#[allow(dead_code)]
#[derive(Debug)]
#[non_exhaustive]
pub struct AuditEntry {
    pub id: i64,
    pub timestamp: i64,
    pub user: String,
    pub action: String,
    pub target_path: Option<String>,
    pub details: Option<String>,
    pub directory_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use tempfile::TempDir;

    /// Helper to create a temporary database for testing.
    fn temp_database() -> (Database, TempDir) {
        let temp_dir = TempDir::with_prefix("stagecrew-audit-test-").unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::open(&db_path).unwrap();
        (db, temp_dir)
    }

    #[test]
    fn audit_service_records_entry() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        audit
            .record(
                "alice",
                AuditAction::Approve,
                Some("/data/test"),
                None,
                None,
            )
            .unwrap();

        // Verify entry was recorded
        let entries = audit.list_recent(10).unwrap();
        assert_eq!(entries.len(), 1, "Expected 1 audit entry");
        assert_eq!(entries[0].user, "alice");
        assert_eq!(entries[0].action, "approve");
        assert_eq!(entries[0].target_path, Some("/data/test".to_string()));
        assert!(entries[0].details.is_none());
        assert!(entries[0].directory_id.is_none());
    }

    #[test]
    fn audit_service_records_entry_without_target_path() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        // System-wide actions like ConfigChange may not have a target path
        audit
            .record(
                "system",
                AuditAction::ConfigChange,
                None,
                Some("Changed expiration to 60 days"),
                None,
            )
            .unwrap();

        let entries = audit.list_recent(10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].user, "system");
        assert_eq!(entries[0].action, "config_change");
        assert!(
            entries[0].target_path.is_none(),
            "Expected no target path for system-wide action"
        );
        assert_eq!(
            entries[0].details,
            Some("Changed expiration to 60 days".to_string())
        );
    }

    #[test]
    fn audit_service_records_all_fields() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        // Create a directory first so we have a valid foreign key
        db.insert_or_update_directory("/data/important", 1024, 5, Some(1000), 1_700_000_000)
            .unwrap();
        let dir = db
            .get_directory_by_path("/data/important")
            .unwrap()
            .unwrap();

        audit
            .record(
                "bob",
                AuditAction::Defer,
                Some("/data/important"),
                Some("Deferred for 30 days"),
                Some(dir.id),
            )
            .unwrap();

        let entries = audit.list_recent(10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].user, "bob");
        assert_eq!(entries[0].action, "defer");
        assert_eq!(entries[0].target_path, Some("/data/important".to_string()));
        assert_eq!(entries[0].details, Some("Deferred for 30 days".to_string()));
        assert_eq!(entries[0].directory_id, Some(dir.id));
    }

    #[test]
    fn audit_service_records_all_action_types() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        let actions = [
            (AuditAction::Approve, "approve"),
            (AuditAction::Defer, "defer"),
            (AuditAction::Ignore, "ignore"),
            (AuditAction::Remove, "remove"),
            (AuditAction::Scan, "scan"),
            (AuditAction::ConfigChange, "config_change"),
        ];

        for (action, _expected_str) in &actions {
            audit
                .record("user", *action, Some("/test"), None, None)
                .unwrap();
        }

        let entries = audit.list_recent(10).unwrap();
        assert_eq!(
            entries.len(),
            actions.len(),
            "Expected {} entries",
            actions.len()
        );

        // Verify each action type was recorded correctly (in reverse order due to DESC)
        for (i, (_, expected_str)) in actions.iter().enumerate().rev() {
            let entry_idx = actions.len() - 1 - i;
            assert_eq!(entries[entry_idx].action, *expected_str);
        }
    }

    #[test]
    fn audit_service_list_recent_on_empty_db() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        let entries = audit.list_recent(10).unwrap();
        assert!(entries.is_empty(), "Expected empty list for empty database");
    }

    #[test]
    fn audit_service_list_recent_with_zero_limit() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        audit
            .record("user", AuditAction::Scan, Some("/test"), None, None)
            .unwrap();

        let entries = audit.list_recent(0).unwrap();
        assert!(entries.is_empty(), "Expected empty list when limit is zero");
    }

    #[test]
    fn audit_service_list_recent_respects_limit() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        // Record 10 entries
        for i in 0..10 {
            audit
                .record(
                    "user",
                    AuditAction::Scan,
                    Some(&format!("/path/{i}")),
                    None,
                    None,
                )
                .unwrap();
        }

        let entries = audit.list_recent(5).unwrap();
        assert_eq!(entries.len(), 5, "Expected limit of 5 to be respected");

        // Should get most recent entries (9, 8, 7, 6, 5)
        assert_eq!(entries[0].target_path, Some("/path/9".to_string()));
        assert_eq!(entries[4].target_path, Some("/path/5".to_string()));
    }

    #[test]
    fn audit_service_list_recent_orders_by_timestamp_desc() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        // Record entries with slight delays to ensure different timestamps
        audit
            .record("user", AuditAction::Scan, Some("/first"), None, None)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));

        audit
            .record("user", AuditAction::Approve, Some("/second"), None, None)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));

        audit
            .record("user", AuditAction::Remove, Some("/third"), None, None)
            .unwrap();

        let entries = audit.list_recent(10).unwrap();
        assert_eq!(entries.len(), 3);

        // Most recent first
        assert_eq!(entries[0].target_path, Some("/third".to_string()));
        assert_eq!(entries[1].target_path, Some("/second".to_string()));
        assert_eq!(entries[2].target_path, Some("/first".to_string()));

        // Verify timestamps are in descending order
        assert!(entries[0].timestamp >= entries[1].timestamp);
        assert!(entries[1].timestamp >= entries[2].timestamp);
    }

    #[test]
    fn audit_service_list_by_path_filters_correctly() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        // Record entries for different paths
        audit
            .record(
                "alice",
                AuditAction::Scan,
                Some("/data/project1"),
                None,
                None,
            )
            .unwrap();
        audit
            .record(
                "bob",
                AuditAction::Approve,
                Some("/data/project2"),
                None,
                None,
            )
            .unwrap();
        audit
            .record(
                "charlie",
                AuditAction::Remove,
                Some("/data/project1"),
                None,
                None,
            )
            .unwrap();
        audit
            .record(
                "dave",
                AuditAction::Defer,
                Some("/data/project1"),
                None,
                None,
            )
            .unwrap();

        let entries = audit.list_by_path("/data/project1").unwrap();
        assert_eq!(entries.len(), 3, "Expected 3 entries for /data/project1");

        // All entries should be for project1
        for entry in &entries {
            assert_eq!(entry.target_path, Some("/data/project1".to_string()));
        }

        // Should be ordered by timestamp desc (dave, charlie, alice)
        assert_eq!(entries[0].user, "dave");
        assert_eq!(entries[1].user, "charlie");
        assert_eq!(entries[2].user, "alice");
    }

    #[test]
    fn audit_service_list_by_path_returns_empty_for_nonexistent() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        audit
            .record("user", AuditAction::Scan, Some("/data/exists"), None, None)
            .unwrap();

        let entries = audit.list_by_path("/data/nonexistent").unwrap();
        assert!(
            entries.is_empty(),
            "Expected no entries for nonexistent path"
        );
    }

    #[test]
    fn audit_service_current_user_reads_environment() {
        // Save original env vars to restore later
        let original_user = std::env::var("USER").ok();
        let original_logname = std::env::var("LOGNAME").ok();

        // SAFETY: Test environment manipulation is isolated and restored after test.
        // This is acceptable in tests that verify environment variable behavior.
        unsafe {
            // Test $USER takes priority
            std::env::set_var("USER", "testuser");
            std::env::set_var("LOGNAME", "fallback");
            assert_eq!(AuditService::current_user(), "testuser");

            // Test $LOGNAME fallback
            std::env::remove_var("USER");
            assert_eq!(AuditService::current_user(), "fallback");

            // Test "unknown" fallback
            std::env::remove_var("LOGNAME");
            assert_eq!(AuditService::current_user(), "unknown");

            // Restore original values
            if let Some(val) = original_user {
                std::env::set_var("USER", val);
            }
            if let Some(val) = original_logname {
                std::env::set_var("LOGNAME", val);
            }
        }
    }

    #[test]
    fn audit_action_as_str_matches_schema_check_constraint() {
        // Verify all action strings match the CHECK constraint in schema.sql
        assert_eq!(AuditAction::Approve.as_str(), "approve");
        assert_eq!(AuditAction::Defer.as_str(), "defer");
        assert_eq!(AuditAction::Ignore.as_str(), "ignore");
        assert_eq!(AuditAction::Remove.as_str(), "remove");
        assert_eq!(AuditAction::Scan.as_str(), "scan");
        assert_eq!(AuditAction::ConfigChange.as_str(), "config_change");
    }
}
