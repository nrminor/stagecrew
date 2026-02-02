-- Stagecrew database schema
-- Uses SQLite with WAL mode for concurrent access
--
-- Note: PRAGMA statements (journal_mode, foreign_keys) are set in Rust code
-- via pragma_update() for reliability. execute_batch() does not guarantee
-- PRAGMA execution order or persistence.

-- Tracked directories (primary tracking unit)
CREATE TABLE IF NOT EXISTS directories (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    file_count INTEGER NOT NULL DEFAULT 0,
    oldest_mtime INTEGER,  -- Unix timestamp of oldest file
    last_scanned INTEGER,  -- Unix timestamp
    status TEXT NOT NULL DEFAULT 'tracked'
        CHECK (status IN ('tracked', 'pending', 'approved', 'deferred', 'ignored', 'removed', 'blocked')),
    deferred_until INTEGER,  -- Unix timestamp, NULL if not deferred
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

-- Individual files within tracked directories
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    directory_id INTEGER NOT NULL REFERENCES directories(id) ON DELETE CASCADE,
    path TEXT NOT NULL UNIQUE,
    size_bytes INTEGER NOT NULL,
    mtime INTEGER NOT NULL,  -- Unix timestamp
    tracked_since INTEGER,  -- Unix timestamp when first added, NULL for legacy files
    status TEXT NOT NULL DEFAULT 'tracked'
        CHECK (status IN ('tracked', 'pending', 'approved', 'deferred', 'ignored', 'removed', 'blocked')),
    deferred_until INTEGER,  -- Unix timestamp, NULL if not deferred
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

-- Audit trail for all actions
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY,
    timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    user TEXT NOT NULL,
    action TEXT NOT NULL
        CHECK (action IN ('approve', 'defer', 'ignore', 'remove', 'scan', 'config_change')),
    target_path TEXT,
    details TEXT,  -- JSON for additional context
    directory_id INTEGER REFERENCES directories(id) ON DELETE SET NULL
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_directories_status ON directories(status);
CREATE INDEX IF NOT EXISTS idx_directories_oldest_mtime ON directories(oldest_mtime);
CREATE INDEX IF NOT EXISTS idx_files_directory_id ON files(directory_id);
CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(mtime);
CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);

-- Pre-computed stats for shell hook (updated by scanner)
CREATE TABLE IF NOT EXISTS stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),  -- Singleton row
    total_tracked_paths INTEGER NOT NULL DEFAULT 0,
    total_size_bytes INTEGER NOT NULL DEFAULT 0,
    paths_within_warning INTEGER NOT NULL DEFAULT 0,
    paths_pending_approval INTEGER NOT NULL DEFAULT 0,
    paths_overdue INTEGER NOT NULL DEFAULT 0,
    last_scan_completed INTEGER,
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

-- Initialize stats singleton
INSERT OR IGNORE INTO stats (id) VALUES (1);
