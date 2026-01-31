//! Audit trail logging and queries.

// TODO(cleanup): Remove these allows as functionality is implemented and used.
// Tracking issue: These are stub implementations awaiting integration.
#![allow(dead_code, unused)]

use rusqlite::params;

use crate::db::Database;
use crate::error::Result;

/// Actions that can be recorded in the audit log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditAction {
    Approve,
    Defer,
    Ignore,
    Remove,
    Scan,
    ConfigChange,
}

impl AuditAction {
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
pub struct AuditService<'a> {
    db: &'a Database,
}

impl<'a> AuditService<'a> {
    /// Create a new audit service.
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    /// Record an audit event.
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
    pub fn current_user() -> String {
        std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .unwrap_or_else(|_| "unknown".to_string())
    }
}

/// A recorded audit event.
#[derive(Debug)]
pub struct AuditEntry {
    pub id: i64,
    pub timestamp: i64,
    pub user: String,
    pub action: String,
    pub target_path: Option<String>,
    pub details: Option<String>,
}
