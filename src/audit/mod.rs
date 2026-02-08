//! Audit trail logging and queries.

use std::path::Path;

use rusqlite::params;

use crate::db::Database;
use crate::error::Result;

/// Actions that can be recorded in the audit log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditAction {
    // Allow: Will be used for file-level actions in US-030.
    #[allow(dead_code)]
    Approve,
    // Allow: Will be used for file-level actions in US-030.
    #[allow(dead_code)]
    Defer,
    // Allow: Will be used for file-level actions in US-030.
    #[allow(dead_code)]
    Ignore,
    Unignore,
    Remove,
    Scan,
    // Allow: ConfigChange variant is part of the public API for future config audit logging.
    // Not yet implemented but reserved for tracking configuration changes.
    #[allow(dead_code)]
    ConfigChange,
}

impl AuditAction {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Defer => "defer",
            Self::Ignore => "ignore",
            Self::Unignore => "unignore",
            Self::Remove => "remove",
            Self::Scan => "scan",
            Self::ConfigChange => "config_change",
        }
    }
}

/// Source subsystem that emitted an audit event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditActorSource {
    Tui,
    Daemon,
    Scanner,
}

impl AuditActorSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Tui => "tui",
            Self::Daemon => "daemon",
            Self::Scanner => "scanner",
        }
    }
}

/// Canonical event envelope for audit writes and mirrored disk logs.
#[derive(Debug)]
pub struct AuditEvent<'a> {
    pub user: &'a str,
    pub actor_source: AuditActorSource,
    pub action: AuditAction,
    pub target_path: Option<&'a Path>,
    pub details: Option<&'a str>,
    pub entry_id: Option<i64>,
    pub root_id: Option<i64>,
    pub status_before: Option<&'a str>,
    pub status_after: Option<&'a str>,
    pub outcome: Option<&'a str>,
}

/// Service for recording and querying audit events.
pub struct AuditService<'a> {
    db: &'a Database,
}

impl<'a> AuditService<'a> {
    /// Create a new audit service.
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
    pub fn record(
        &self,
        user: &str,
        action: AuditAction,
        target_path: Option<&Path>,
        details: Option<&str>,
        entry_id: Option<i64>,
    ) -> Result<()> {
        let target_path_str = target_path.map(|p| p.to_string_lossy());
        self.db.conn().execute(
            "INSERT INTO audit_log (user, action, target_path, details, entry_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                user,
                action.as_str(),
                target_path_str.as_deref(),
                details,
                entry_id
            ],
        )?;
        Ok(())
    }

    /// Record an audit event and mirror it to structured tracing output.
    ///
    /// This preserves `SQLite` as the durable source of truth while ensuring
    /// every audit action is also replicated to the on-disk application log
    /// with a consistent, machine-parsable field set.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying audit database insert fails.
    pub fn record_event(&self, event: &AuditEvent<'_>) -> Result<()> {
        self.record(
            event.user,
            event.action,
            event.target_path,
            event.details,
            event.entry_id,
        )?;

        let target_path = event.target_path.map(|p| p.display().to_string());
        if matches!(event.outcome, Some("blocked" | "failed")) {
            tracing::warn!(
                target: "stagecrew::audit",
                audit_action = event.action.as_str(),
                audit_user = event.user,
                audit_actor_source = event.actor_source.as_str(),
                audit_target_path = target_path.as_deref(),
                audit_entry_id = event.entry_id,
                audit_root_id = event.root_id,
                audit_status_before = event.status_before,
                audit_status_after = event.status_after,
                audit_outcome = event.outcome,
                audit_details = event.details,
                "audit_event"
            );
        } else {
            tracing::info!(
                target: "stagecrew::audit",
                audit_action = event.action.as_str(),
                audit_user = event.user,
                audit_actor_source = event.actor_source.as_str(),
                audit_target_path = target_path.as_deref(),
                audit_entry_id = event.entry_id,
                audit_root_id = event.root_id,
                audit_status_before = event.status_before,
                audit_status_after = event.status_after,
                audit_outcome = event.outcome,
                audit_details = event.details,
                "audit_event"
            );
        }

        Ok(())
    }

    /// Get the current username from the environment.
    ///
    /// Checks `$USER` and `$LOGNAME` environment variables in that order.
    /// Returns `"unknown"` if neither is set.
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
    pub fn list_recent(&self, limit: usize) -> Result<Vec<AuditEntry>> {
        let mut stmt = self.db.conn().prepare(
            "SELECT id, timestamp, user, action, target_path, details, entry_id
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
                    entry_id: row.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(entries)
    }

    /// List audit entries for a specific path.
    ///
    /// Returns all audit entries where `target_path` matches the given path,
    /// ordered by timestamp descending. Useful for viewing the history of
    /// actions performed on a particular entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    // Allow: Part of the public audit API. May be used by future TUI detail view
    // to show path-specific history. Currently unused but part of the stable API.
    #[allow(dead_code)]
    pub fn list_by_path(&self, path: &Path) -> Result<Vec<AuditEntry>> {
        let path_str = path.to_string_lossy();
        let mut stmt = self.db.conn().prepare(
            "SELECT id, timestamp, user, action, target_path, details, entry_id
             FROM audit_log
             WHERE target_path = ?1
             ORDER BY timestamp DESC",
        )?;

        let entries = stmt
            .query_map(params![&*path_str], |row| {
                Ok(AuditEntry {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    user: row.get(2)?,
                    action: row.get(3)?,
                    target_path: row.get(4)?,
                    details: row.get(5)?,
                    entry_id: row.get(6)?,
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
#[derive(Debug)]
#[non_exhaustive]
// Allow: Public struct fields are part of the API. Fields like `id`, `details`, and
// `entry_id` are not directly accessed in the current codebase but are available
// for external consumers and future TUI enhancements.
#[allow(dead_code)]
pub struct AuditEntry {
    pub id: i64,
    pub timestamp: i64,
    pub user: String,
    pub action: String,
    pub target_path: Option<String>,
    pub details: Option<String>,
    pub entry_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::db::Database;
    use tempfile::TempDir;

    /// Helper to create a temporary database for testing.
    fn temp_database() -> (Database, TempDir) {
        let temp_dir = TempDir::with_prefix("stagecrew-audit-test-").expect(
            "failed to create temp directory for audit test - check disk space and permissions",
        );
        let db_path = temp_dir.path().join("test.db");
        let db = Database::open(&db_path)
            .expect("failed to open test database - check permissions and disk space");
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
                Some(Path::new("/data/test")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );

        // Verify entry was recorded
        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
        assert_eq!(entries.len(), 1, "Expected 1 audit entry");
        assert_eq!(entries[0].user, "alice");
        assert_eq!(entries[0].action, "approve");
        assert_eq!(entries[0].target_path, Some("/data/test".to_string()));
        assert!(entries[0].details.is_none());
        assert!(entries[0].entry_id.is_none());
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
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );

        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
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

        // Create a root and entry first so we have a valid foreign key
        let root_id = db
            .insert_root(Path::new("/data"))
            .expect("failed to insert root to database - connection may be lost or disk full");
        let entry_id = db
            .upsert_entry(
                root_id,
                Path::new("/data/important"),
                Path::new("/data"),
                false,
                1024,
                Some(1_700_000_000),
            )
            .expect("failed to insert entry to database - connection may be lost or disk full");

        audit
            .record(
                "bob",
                AuditAction::Defer,
                Some(Path::new("/data/important")),
                Some("Deferred for 30 days"),
                Some(entry_id),
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );

        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].user, "bob");
        assert_eq!(entries[0].action, "defer");
        assert_eq!(entries[0].target_path, Some("/data/important".to_string()));
        assert_eq!(entries[0].details, Some("Deferred for 30 days".to_string()));
        assert_eq!(entries[0].entry_id, Some(entry_id));
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
                .record("user", *action, Some(Path::new("/test")), None, None)
                .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );
        }

        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
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

        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
        assert!(entries.is_empty(), "Expected empty list for empty database");
    }

    #[test]
    fn audit_service_list_recent_with_zero_limit() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        audit
            .record(
                "user",
                AuditAction::Scan,
                Some(Path::new("/test")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );

        let entries = audit
            .list_recent(0)
            .expect("failed to query recent audit entries - database connection may be lost");
        assert!(entries.is_empty(), "Expected empty list when limit is zero");
    }

    #[test]
    fn audit_service_list_recent_respects_limit() {
        let (db, _temp_dir) = temp_database();
        let audit = AuditService::new(&db);

        // Record 10 entries
        for i in 0..10 {
            let p = format!("/path/{i}");
            audit
                .record(
                    "user",
                    AuditAction::Scan,
                    Some(Path::new(&p)),
                    None,
                    None,
                )
                .expect("failed to record audit entry to database - connection may be lost or disk full");
        }

        let entries = audit
            .list_recent(5)
            .expect("failed to query recent audit entries - database connection may be lost");
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
            .record(
                "user",
                AuditAction::Scan,
                Some(Path::new("/first")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );
        std::thread::sleep(std::time::Duration::from_millis(10));

        audit
            .record(
                "user",
                AuditAction::Approve,
                Some(Path::new("/second")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );
        std::thread::sleep(std::time::Duration::from_millis(10));

        audit
            .record(
                "user",
                AuditAction::Remove,
                Some(Path::new("/third")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );

        let entries = audit
            .list_recent(10)
            .expect("failed to query recent audit entries - database connection may be lost");
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
                Some(Path::new("/data/project1")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );
        audit
            .record(
                "bob",
                AuditAction::Approve,
                Some(Path::new("/data/project2")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );
        audit
            .record(
                "charlie",
                AuditAction::Remove,
                Some(Path::new("/data/project1")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );
        audit
            .record(
                "dave",
                AuditAction::Defer,
                Some(Path::new("/data/project1")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );

        let entries = audit
            .list_by_path(Path::new("/data/project1"))
            .expect("failed to query audit entries by path - database connection may be lost");
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
            .record(
                "user",
                AuditAction::Scan,
                Some(Path::new("/data/exists")),
                None,
                None,
            )
            .expect(
                "failed to record audit entry to database - connection may be lost or disk full",
            );

        let entries = audit
            .list_by_path(Path::new("/data/nonexistent"))
            .expect("failed to query audit entries by path - database connection may be lost");
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
        assert_eq!(AuditAction::Unignore.as_str(), "unignore");
        assert_eq!(AuditAction::Remove.as_str(), "remove");
        assert_eq!(AuditAction::Scan.as_str(), "scan");
        assert_eq!(AuditAction::ConfigChange.as_str(), "config_change");
    }
}
