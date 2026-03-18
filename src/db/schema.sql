-- Stagecrew database schema
-- Uses SQLite with WAL mode for concurrent access
--
-- Note: PRAGMA statements (journal_mode, foreign_keys) are set in Rust code
-- via pragma_update() for reliability. execute_batch() does not guarantee
-- PRAGMA execution order or persistence.

-- User-configured tracked roots (what appears in sidebar)
CREATE TABLE IF NOT EXISTS roots (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    added_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    last_scanned INTEGER,
    target_bytes INTEGER
);

-- Unified entries table (files and directories discovered during scan)
CREATE TABLE IF NOT EXISTS entries (
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

-- Audit trail for all actions
CREATE TABLE IF NOT EXISTS audit_log (
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

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_entries_root_id ON entries(root_id);
CREATE INDEX IF NOT EXISTS idx_entries_parent_path ON entries(parent_path);
CREATE INDEX IF NOT EXISTS idx_entries_status ON entries(status);
CREATE INDEX IF NOT EXISTS idx_entries_mtime ON entries(mtime);
CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);

-- Pre-computed stats for shell hook (updated by scanner)
CREATE TABLE IF NOT EXISTS stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_files INTEGER NOT NULL DEFAULT 0,
    total_size_bytes INTEGER NOT NULL DEFAULT 0,
    files_within_warning INTEGER NOT NULL DEFAULT 0,
    files_pending_approval INTEGER NOT NULL DEFAULT 0,
    files_overdue INTEGER NOT NULL DEFAULT 0,
    last_scan_completed INTEGER,
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

-- Initialize stats singleton
INSERT OR IGNORE INTO stats (id) VALUES (1);
