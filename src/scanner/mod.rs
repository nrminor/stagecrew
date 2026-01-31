//! Filesystem scanning logic.

// TODO(cleanup): Remove these allows as functionality is implemented and used.
// Tracking issue: Scanner results awaiting database integration.
#![allow(dead_code)]

use std::path::Path;

use crate::error::Result;

/// Scanner for walking filesystem trees and collecting metadata.
pub struct Scanner {
    // Configuration will be added here
}

impl Scanner {
    /// Create a new scanner.
    pub fn new() -> Self {
        Self {}
    }

    /// Scan a directory tree and return file metadata.
    ///
    /// This runs in a background thread to avoid blocking the UI.
    // TODO(cleanup): Remove allow once jwalk integration uses spawn_blocking.
    #[allow(clippy::unused_async)]
    pub async fn scan(&self, _root: &Path) -> Result<ScanResult> {
        // TODO: Implement using jwalk for parallel traversal
        Ok(ScanResult::default())
    }
}

impl Default for Scanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a filesystem scan.
#[derive(Debug, Default)]
pub struct ScanResult {
    pub total_files: u64,
    pub total_size_bytes: u64,
    pub directories_found: Vec<DirectoryInfo>,
}

/// Information about a scanned directory.
#[derive(Debug)]
pub struct DirectoryInfo {
    pub path: std::path::PathBuf,
    pub size_bytes: u64,
    pub file_count: u64,
    pub oldest_mtime: Option<jiff::Timestamp>,
}
