//! Async database dispatcher for non-blocking TUI operations.
//!
//! The dispatcher runs a dedicated worker thread with its own SQLite
//! connection, processing read requests from the event loop via mpsc
//! channels. This keeps the event loop free from blocking DB I/O.

use std::path::{Path, PathBuf};

use tokio::sync::mpsc;

use crate::audit::{AuditAction, AuditActorSource, AuditEntry, AuditEvent, AuditService};
use crate::db::{Database, Entry, Root, Stats};
use crate::removal::RemovalMethod;
use crate::scanner::calculate_expiration;

use super::SortMode;
use super::ui::sort_entry_rows;

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// A request from the event loop to the DB worker.
pub(crate) enum DbRequest {
    // -- Reads --
    /// Load all tracked roots.
    Roots,
    /// Load all entries for a specific root.
    RootEntries { root_id: i64 },
    /// Load entries for a directory, compute `days_remaining`, and sort.
    DirEntries {
        root_id: i64,
        parent_path: PathBuf,
        expiration_days: u32,
        sort_mode: SortMode,
    },
    /// Compute live stats and nearest expiration timestamp.
    Stats {
        expiration_days: u32,
        warning_days: u32,
    },
    /// Load recent audit log entries.
    AuditEntries,

    // -- Writes --
    /// Update status for one or more entries (approve, ignore, unignore).
    /// If an entry is a directory, its children are also updated via path
    /// prefix matching.
    UpdateStatus {
        /// Entries to update.
        entries: Vec<WriteEntry>,
        new_status: String,
        audit: WriteAudit,
    },
    /// Defer one or more entries. Directories propagate to children.
    Defer {
        entries: Vec<WriteEntry>,
        deferred_until: i64,
        days: u32,
        audit: WriteAudit,
    },
    /// Delete entries from the filesystem and update DB status.
    Delete {
        entries: Vec<WriteEntry>,
        method: RemovalMethod,
        audit: WriteAudit,
    },
    /// Execute removal of all approved entries under a root.
    ExecuteRemovals { root_id: i64, audit: WriteAudit },
    /// Restore entries to a previous state (undo).
    Undo {
        entries: Vec<UndoWrite>,
        description: String,
        audit: WriteAudit,
    },
    /// Reset all countdowns under a root to now.
    ResetRootTimer { root_id: i64, audit: WriteAudit },
    /// Add a new tracked root path.
    InsertRoot { path: PathBuf },
    /// Remove a tracked root.
    DeleteRoot { root_id: i64 },
    /// Set or clear the quota target for a root.
    SetQuotaTarget {
        root_id: i64,
        target_bytes: Option<i64>,
    },
    /// Export recent audit entries to a file.
    ExportAudit {
        limit: usize,
        format: crate::audit::AuditExportFormat,
        path: PathBuf,
    },
}

/// Identifies an entry being mutated, carrying enough info for the worker
/// to perform the DB write, path-prefix propagation, and audit logging.
pub(crate) struct WriteEntry {
    pub id: i64,
    pub path: PathBuf,
    pub is_dir: bool,
    pub status_before: String,
}

/// Owned audit metadata sent with write requests. The worker uses this to
/// record audit events atomically with the mutation.
pub(crate) struct WriteAudit {
    pub user: String,
    pub root_id: Option<i64>,
}

/// An entry to restore during undo.
pub(crate) struct UndoWrite {
    pub entry_id: i64,
    pub status_before: String,
    pub countdown_start_before: Option<i64>,
    pub deferred_until_before: Option<i64>,
}

/// A response from the DB worker to the event loop.
pub(crate) enum DbResult {
    // -- Read results --
    /// List of tracked roots.
    Roots(Vec<Root>),
    /// Entries for a root (lifecycle, timeline, quota).
    RootEntries(Vec<Entry>),
    /// Sorted directory entries with `days_remaining`.
    DirEntries(Vec<(Entry, i64)>),
    /// Live stats and nearest expiration.
    Stats {
        stats: Stats,
        nearest_expiration: Option<i64>,
    },
    /// Recent audit log entries.
    AuditEntries(Vec<AuditEntry>),

    // -- Write results --
    /// A write completed successfully. The optimistic App state is correct.
    WriteOk,
    /// A write completed but some entries failed. Contains a status message
    /// to display to the user.
    WritePartial { message: String },
    /// A write failed entirely. The optimistic update should be reverted.
    WriteFailed { message: String },

    // -- Errors --
    /// A read request failed. Logged but not fatal.
    Error {
        context: &'static str,
        message: String,
    },
}

// ---------------------------------------------------------------------------
// Dispatcher handle
// ---------------------------------------------------------------------------

/// Handle to send requests to the DB worker.
#[derive(Clone)]
pub(crate) struct DbDispatcher {
    tx: mpsc::Sender<DbRequest>,
    /// When set, requests are processed synchronously on send (for tests).
    #[cfg(test)]
    sync_db_path: Option<PathBuf>,
}

impl DbDispatcher {
    /// Create a no-op dispatcher whose sends are silently dropped.
    /// Used in tests where DB writes are not needed.
    #[cfg(test)]
    pub fn noop() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        Self {
            tx,
            sync_db_path: None,
        }
    }

    /// Create a synchronous dispatcher that processes requests immediately
    /// against the given database. Used in tests that need to verify DB
    /// state after dispatching writes.
    #[cfg(test)]
    pub fn sync_for_db(db_path: &Path) -> Self {
        let (tx, _rx) = mpsc::channel(1);
        Self {
            tx,
            sync_db_path: Some(db_path.to_path_buf()),
        }
    }

    /// Send a request to the DB worker. Non-blocking — drops the request
    /// if the channel is full (back-pressure).
    pub fn send(&self, request: DbRequest) {
        #[cfg(test)]
        if let Some(ref db_path) = self.sync_db_path {
            if let Ok(db) = Database::open(db_path) {
                let _ = process_request(&db, request);
            }
            return;
        }
        if let Err(e) = self.tx.try_send(request) {
            tracing::warn!("DB dispatcher channel full, dropping request: {e}");
        }
    }

    /// Convenience: dispatch all read requests needed to fully refresh
    /// the view. Called after scan completion, user actions, and startup.
    pub fn dispatch_refresh_all(
        &self,
        current_root_id: Option<i64>,
        current_path: &Path,
        expiration_days: u32,
        warning_days: u32,
        sort_mode: SortMode,
    ) {
        self.send(DbRequest::Roots);
        if let Some(root_id) = current_root_id {
            self.send(DbRequest::RootEntries { root_id });
            if !current_path.as_os_str().is_empty() {
                self.send(DbRequest::DirEntries {
                    root_id,
                    parent_path: current_path.to_path_buf(),
                    expiration_days,
                    sort_mode,
                });
            }
        }
        self.send(DbRequest::Stats {
            expiration_days,
            warning_days,
        });
        self.send(DbRequest::AuditEntries);
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

/// Spawn the DB worker thread and return a dispatcher handle + result receiver.
///
/// The worker opens its own `Database` connection (separate from the event
/// loop's connection) and processes requests sequentially. Results are sent
/// back via the returned receiver.
pub(crate) fn spawn_db_worker(db_path: &Path) -> (DbDispatcher, mpsc::Receiver<DbResult>) {
    let (req_tx, mut req_rx) = mpsc::channel::<DbRequest>(64);
    let (res_tx, res_rx) = mpsc::channel::<DbResult>(64);

    let db_path = db_path.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let db = match Database::open(&db_path) {
            Ok(db) => db,
            Err(e) => {
                tracing::error!("DB worker failed to open database: {e}");
                return;
            }
        };

        while let Some(request) = req_rx.blocking_recv() {
            let result = process_request(&db, request);
            if res_tx.blocking_send(result).is_err() {
                break;
            }
        }

        tracing::debug!("DB worker shutting down");
    });

    (
        DbDispatcher {
            tx: req_tx,
            #[cfg(test)]
            sync_db_path: None,
        },
        res_rx,
    )
}

// ---------------------------------------------------------------------------
// Request processing
// ---------------------------------------------------------------------------

/// Process a single DB request and produce a result.
#[allow(clippy::too_many_lines)]
fn process_request(db: &Database, request: DbRequest) -> DbResult {
    match request {
        // -- Reads --
        DbRequest::Roots => match db.list_roots() {
            Ok(roots) => DbResult::Roots(roots),
            Err(e) => DbResult::Error {
                context: "Roots",
                message: e.to_string(),
            },
        },

        DbRequest::RootEntries { root_id } => match db.list_entries_by_root(root_id) {
            Ok(entries) => DbResult::RootEntries(entries),
            Err(e) => DbResult::Error {
                context: "RootEntries",
                message: e.to_string(),
            },
        },

        DbRequest::DirEntries {
            root_id,
            parent_path,
            expiration_days,
            sort_mode,
        } => match db.list_entries_by_parent(root_id, &parent_path) {
            Ok(entries) => {
                let mut rows: Vec<_> = entries
                    .into_iter()
                    .map(|entry| {
                        let days_remaining = entry
                            .countdown_start
                            .map_or(i64::MAX, |cs| calculate_expiration(cs, expiration_days));
                        (entry, days_remaining)
                    })
                    .collect();
                sort_entry_rows(&mut rows, sort_mode);
                DbResult::DirEntries(rows)
            }
            Err(e) => DbResult::Error {
                context: "DirEntries",
                message: e.to_string(),
            },
        },

        DbRequest::Stats {
            expiration_days,
            warning_days,
        } => match db.compute_live_stats(expiration_days, warning_days) {
            Ok(stats) => {
                let nearest = db.nearest_expiration(expiration_days).unwrap_or_else(|e| {
                    tracing::warn!("Failed to query nearest expiration: {e}");
                    None
                });
                DbResult::Stats {
                    stats,
                    nearest_expiration: nearest,
                }
            }
            Err(e) => DbResult::Error {
                context: "Stats",
                message: e.to_string(),
            },
        },

        DbRequest::AuditEntries => match AuditService::new(db).list_recent(1000) {
            Ok(entries) => DbResult::AuditEntries(entries),
            Err(e) => DbResult::Error {
                context: "AuditEntries",
                message: e.to_string(),
            },
        },

        // -- Writes --
        DbRequest::UpdateStatus {
            entries,
            new_status,
            audit,
        } => process_update_status(db, &entries, &new_status, &audit),

        DbRequest::Defer {
            entries,
            deferred_until,
            days,
            audit,
        } => process_defer(db, &entries, deferred_until, days, &audit),

        DbRequest::Delete {
            entries,
            method,
            audit,
        } => process_delete(db, &entries, method, &audit),

        DbRequest::ExecuteRemovals { root_id, audit } => {
            process_execute_removals(db, root_id, &audit)
        }

        DbRequest::Undo {
            entries,
            description,
            audit,
        } => process_undo(db, &entries, &description, &audit),

        DbRequest::ResetRootTimer { root_id, audit } => {
            process_reset_root_timer(db, root_id, &audit)
        }

        DbRequest::InsertRoot { path } => match db.insert_root(&path) {
            Ok(_) => DbResult::WriteOk,
            Err(e) => DbResult::WriteFailed {
                message: format!("Failed to add root: {e}"),
            },
        },

        DbRequest::DeleteRoot { root_id } => match db.delete_root(root_id) {
            Ok(()) => DbResult::WriteOk,
            Err(e) => DbResult::WriteFailed {
                message: format!("Failed to remove root: {e}"),
            },
        },

        DbRequest::SetQuotaTarget {
            root_id,
            target_bytes,
        } => match db.set_root_target_bytes(root_id, target_bytes) {
            Ok(()) => DbResult::WriteOk,
            Err(e) => DbResult::WriteFailed {
                message: format!("Failed to set quota target: {e}"),
            },
        },

        DbRequest::ExportAudit {
            limit,
            format,
            path,
        } => {
            let audit = AuditService::new(db);
            match audit.export_recent_to_path(limit, format, &path) {
                Ok(count) => DbResult::WritePartial {
                    message: format!(
                        "Exported {count} audit entr{} to {} ({})",
                        if count == 1 { "y" } else { "ies" },
                        path.display(),
                        format.label()
                    ),
                },
                Err(e) => DbResult::WriteFailed {
                    message: format!("Audit export failed: {e}"),
                },
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Write processors
// ---------------------------------------------------------------------------

/// Update status for entries, propagating to children for directories.
/// Wraps the batch in a transaction for atomicity and performance (one
/// fsync instead of N).
fn process_update_status(
    db: &Database,
    entries: &[WriteEntry],
    new_status: &str,
    audit: &WriteAudit,
) -> DbResult {
    let tx = db.conn().unchecked_transaction().ok();
    let audit_svc = AuditService::new(db);

    let mut failed = 0;
    for entry in entries {
        if let Err(e) = db.update_entry_status(entry.id, new_status) {
            tracing::warn!("Failed to update entry {}: {e}", entry.id);
            failed += 1;
            continue;
        }
        if entry.is_dir
            && let Some(root_id) = audit.root_id
            && let Err(e) = db.update_entries_by_path_prefix(root_id, &entry.path, new_status)
        {
            tracing::warn!("Failed to propagate status to children: {e}");
        }
        // Derive audit action from context: what status we're moving from/to
        let action = match (entry.status_before.as_str(), new_status) {
            ("approved", "tracked") => AuditAction::Unapprove,
            ("ignored", "tracked") => AuditAction::Unignore,
            (_, "ignored") => AuditAction::Ignore,
            _ => AuditAction::Approve,
        };
        let _ = audit_svc.record_event(&AuditEvent {
            user: &audit.user,
            actor_source: AuditActorSource::Tui,
            action,
            target_path: Some(&entry.path),
            details: None,
            entry_id: Some(entry.id),
            root_id: audit.root_id,
            status_before: Some(&entry.status_before),
            status_after: Some(new_status),
            outcome: Some(new_status),
        });
    }

    if let Some(tx) = tx {
        let _ = tx.commit();
    }

    if failed == 0 {
        DbResult::WriteOk
    } else {
        DbResult::WritePartial {
            message: format!("{failed} of {} entries failed to update", entries.len()),
        }
    }
}

/// Defer entries, propagating to children for directories.
/// Wraps the batch in a transaction for atomicity and performance.
fn process_defer(
    db: &Database,
    entries: &[WriteEntry],
    deferred_until: i64,
    days: u32,
    audit: &WriteAudit,
) -> DbResult {
    let tx = db.conn().unchecked_transaction().ok();
    let audit_svc = AuditService::new(db);
    let detail = format!("Deferred for {days} days");
    let mut failed = 0;

    for entry in entries {
        if let Err(e) = db.defer_entry(entry.id, deferred_until) {
            tracing::warn!("Failed to defer entry {}: {e}", entry.id);
            failed += 1;
            continue;
        }
        if entry.is_dir
            && let Some(root_id) = audit.root_id
            && let Err(e) = db.defer_entries_by_path_prefix(root_id, &entry.path, deferred_until)
        {
            tracing::warn!("Failed to propagate deferral to children: {e}");
        }
        let _ = audit_svc.record_event(&AuditEvent {
            user: &audit.user,
            actor_source: AuditActorSource::Tui,
            action: AuditAction::Defer,
            target_path: Some(&entry.path),
            details: Some(&detail),
            entry_id: Some(entry.id),
            root_id: audit.root_id,
            status_before: Some(&entry.status_before),
            status_after: Some("deferred"),
            outcome: Some("deferred"),
        });
    }

    if let Some(tx) = tx {
        let _ = tx.commit();
    }

    if failed == 0 {
        DbResult::WriteOk
    } else {
        DbResult::WritePartial {
            message: format!("{failed} of {} entries failed to defer", entries.len()),
        }
    }
}

/// Delete entries from disk and update DB status.
fn process_delete(
    db: &Database,
    entries: &[WriteEntry],
    method: RemovalMethod,
    audit: &WriteAudit,
) -> DbResult {
    let tx = db.conn().unchecked_transaction().ok();
    let audit_svc = AuditService::new(db);
    let mut success = 0u32;
    let mut failed = 0u32;

    for entry in entries {
        if let Err(e) = db.delete_entry(entry.id, &entry.path, entry.is_dir, method) {
            tracing::warn!("Failed to delete {}: {e}", entry.path.display());
            failed += 1;
        } else {
            success += 1;
            let detail = match method {
                RemovalMethod::Trash => {
                    if entry.is_dir {
                        "Directory moved to trash by user"
                    } else {
                        "File moved to trash by user"
                    }
                }
                RemovalMethod::PermanentDelete => {
                    if entry.is_dir {
                        "Directory permanently deleted by user"
                    } else {
                        "File permanently deleted by user"
                    }
                }
            };
            let _ = audit_svc.record_event(&AuditEvent {
                user: &audit.user,
                actor_source: AuditActorSource::Tui,
                action: AuditAction::Remove,
                target_path: Some(&entry.path),
                details: Some(detail),
                entry_id: Some(entry.id),
                root_id: audit.root_id,
                status_before: Some("tracked"),
                status_after: Some("removed"),
                outcome: Some("removed"),
            });
        }
    }

    if let Some(tx) = tx {
        let _ = tx.commit();
    }

    let action_past = method.past_tense();
    if failed == 0 {
        DbResult::WritePartial {
            message: format!("{action_past} {success} file(s)"),
        }
    } else if success > 0 {
        DbResult::WritePartial {
            message: format!("{action_past} {success}/{}, {failed} failed", entries.len()),
        }
    } else {
        DbResult::WriteFailed {
            message: format!("{action_past} failed for all {} entries", entries.len()),
        }
    }
}

/// Execute removal of all approved entries under a root.
fn process_execute_removals(db: &Database, root_id: i64, audit: &WriteAudit) -> DbResult {
    let entries = match db.list_entries_by_root_and_status(root_id, "approved") {
        Ok(e) => e,
        Err(e) => {
            return DbResult::WriteFailed {
                message: format!("Failed to list approved entries: {e}"),
            };
        }
    };

    if entries.is_empty() {
        return DbResult::WritePartial {
            message: "No approved entries to remove".to_string(),
        };
    }

    let audit_svc = AuditService::new(db);
    let mut removed = 0u32;
    let mut blocked = 0u32;

    for entry in &entries {
        match crate::removal::remove(&entry.path, RemovalMethod::PermanentDelete) {
            Ok(_) => {
                let _ = db.update_entry_status(entry.id, "removed");
                removed += 1;
                let _ = audit_svc.record_event(&AuditEvent {
                    user: &audit.user,
                    actor_source: AuditActorSource::Tui,
                    action: AuditAction::Remove,
                    target_path: Some(&entry.path),
                    details: Some(&format!("Permanently deleted {} bytes", entry.size_bytes)),
                    entry_id: Some(entry.id),
                    root_id: Some(root_id),
                    status_before: Some("approved"),
                    status_after: Some("removed"),
                    outcome: Some("removed"),
                });
            }
            Err(e) => {
                let _ = db.update_entry_status(entry.id, "blocked");
                blocked += 1;
                let _ = audit_svc.record_event(&AuditEvent {
                    user: &audit.user,
                    actor_source: AuditActorSource::Tui,
                    action: AuditAction::Remove,
                    target_path: Some(&entry.path),
                    details: Some(&format!("Blocked: {e}")),
                    entry_id: Some(entry.id),
                    root_id: Some(root_id),
                    status_before: Some("approved"),
                    status_after: Some("blocked"),
                    outcome: Some("blocked"),
                });
            }
        }
    }

    DbResult::WritePartial {
        message: format!("Removed {removed}, blocked {blocked}"),
    }
}

/// Restore entries to their previous state (undo).
fn process_undo(
    db: &Database,
    entries: &[UndoWrite],
    description: &str,
    audit: &WriteAudit,
) -> DbResult {
    let tx = db.conn().unchecked_transaction().ok();
    let mut failed = 0;
    for entry in entries {
        if let Err(e) = db.restore_entry_state(
            entry.entry_id,
            &entry.status_before,
            entry.countdown_start_before,
            entry.deferred_until_before,
        ) {
            tracing::warn!("Failed to undo entry {}: {e}", entry.entry_id);
            failed += 1;
        }
    }

    let audit_svc = AuditService::new(db);
    let _ = audit_svc.record_event(&AuditEvent {
        user: &audit.user,
        actor_source: AuditActorSource::Tui,
        action: AuditAction::Undo,
        target_path: None,
        details: Some(&format!("Undid: {description}")),
        entry_id: None,
        root_id: audit.root_id,
        status_before: None,
        status_after: None,
        outcome: Some("undone"),
    });

    if let Some(tx) = tx {
        let _ = tx.commit();
    }

    if failed == 0 {
        DbResult::WriteOk
    } else {
        DbResult::WritePartial {
            message: format!("{failed} of {} entries failed to undo", entries.len()),
        }
    }
}

/// Reset all countdowns under a root to now.
fn process_reset_root_timer(db: &Database, root_id: i64, audit: &WriteAudit) -> DbResult {
    match db.reset_root_countdowns(root_id) {
        Ok(count) => {
            let audit_svc = AuditService::new(db);
            let _ = audit_svc.record_event(&AuditEvent {
                user: &audit.user,
                actor_source: AuditActorSource::Tui,
                action: AuditAction::Defer,
                target_path: None,
                details: Some(&format!("Reset countdown for {count} entries under root")),
                entry_id: None,
                root_id: Some(root_id),
                status_before: None,
                status_after: Some("tracked"),
                outcome: Some("reset"),
            });
            DbResult::WriteOk
        }
        Err(e) => DbResult::WriteFailed {
            message: format!("Failed to reset countdowns: {e}"),
        },
    }
}
