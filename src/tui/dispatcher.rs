//! Async database dispatcher for non-blocking TUI operations.
//!
//! The dispatcher runs a dedicated worker thread with its own SQLite
//! connection, processing read requests from the event loop via mpsc
//! channels. This keeps the event loop free from blocking DB I/O.

use std::path::{Path, PathBuf};

use tokio::sync::mpsc;

use crate::audit::{AuditEntry, AuditService};
use crate::db::{Database, Entry, Root, Stats};
use crate::scanner::calculate_expiration;

use super::SortMode;
use super::ui::sort_entry_rows;

/// A request from the event loop to the DB worker.
pub(crate) enum DbRequest {
    /// Load all tracked roots.
    Roots,
    /// Load all entries for a specific root.
    RootEntries { root_id: i64 },
    /// Load entries for a directory, compute `days_remaining`, and sort.
    DirEntries {
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
}

/// A response from the DB worker to the event loop.
pub(crate) enum DbResult {
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
    /// A request failed. The error is logged but not fatal.
    Error {
        context: &'static str,
        message: String,
    },
}

/// Handle to send requests to the DB worker.
#[derive(Clone)]
pub(crate) struct DbDispatcher {
    tx: mpsc::Sender<DbRequest>,
}

impl DbDispatcher {
    /// Create a no-op dispatcher whose sends are silently dropped.
    /// Used in tests where the DB worker is not needed.
    #[cfg(test)]
    pub fn noop() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        Self { tx }
    }

    /// Send a request to the DB worker. Non-blocking — drops the request
    /// if the channel is full (back-pressure).
    pub fn send(&self, request: DbRequest) {
        // Use try_send to avoid blocking the event loop. If the channel
        // is full, the request is dropped — the next user action or timer
        // tick will retry.
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
        }
        if !current_path.as_os_str().is_empty() {
            self.send(DbRequest::DirEntries {
                parent_path: current_path.to_path_buf(),
                expiration_days,
                sort_mode,
            });
        }
        self.send(DbRequest::Stats {
            expiration_days,
            warning_days,
        });
        self.send(DbRequest::AuditEntries);
    }
}

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
                // Event loop dropped the receiver — shut down.
                break;
            }
        }

        tracing::debug!("DB worker shutting down");
    });

    (DbDispatcher { tx: req_tx }, res_rx)
}

/// Process a single DB request and produce a result.
fn process_request(db: &Database, request: DbRequest) -> DbResult {
    match request {
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
            parent_path,
            expiration_days,
            sort_mode,
        } => match db.list_entries_by_parent(&parent_path) {
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
    }
}
