//! File removal logic and approval workflow.

// TODO(cleanup): Remove these allows as functionality is implemented and used.
// Tracking issue: RemovalService awaiting daemon integration.
#![allow(dead_code)]

use std::path::Path;

use crate::error::{Error, Result};

/// Handles file and directory removal with safety checks.
pub struct RemovalService {
    dry_run: bool,
}

impl RemovalService {
    /// Create a new removal service.
    pub fn new(dry_run: bool) -> Self {
        Self { dry_run }
    }

    /// Attempt to remove a path (file or directory).
    ///
    /// Returns an error if:
    /// - Permission is denied
    /// - Path doesn't exist
    /// - Other filesystem errors occur
    pub fn remove(&self, path: &Path) -> Result<RemovalOutcome> {
        if !path.exists() {
            return Err(Error::PathNotFound(path.to_path_buf()));
        }

        if self.dry_run {
            tracing::info!(?path, "Dry run: would remove");
            return Ok(RemovalOutcome::DryRun);
        }

        let result = if path.is_dir() {
            std::fs::remove_dir_all(path)
        } else {
            std::fs::remove_file(path)
        };

        match result {
            Ok(()) => {
                tracing::info!(?path, "Removed successfully");
                Ok(RemovalOutcome::Removed)
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                tracing::warn!(?path, "Permission denied");
                Err(Error::PermissionDenied(path.to_path_buf()))
            }
            Err(e) => Err(Error::Filesystem {
                path: path.to_path_buf(),
                source: e,
            }),
        }
    }
}

/// Outcome of a removal attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalOutcome {
    /// File was removed.
    Removed,
    /// Dry run mode - no actual removal.
    DryRun,
}
