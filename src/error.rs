//! Application error types.

use std::path::PathBuf;

use thiserror::Error;

/// Core error type for stagecrew operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("filesystem error at {path}: {source}")]
    Filesystem {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("permission denied: {0}")]
    PermissionDenied(PathBuf),

    #[error("path not found: {0}")]
    PathNotFound(PathBuf),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience type alias for Results using our Error type.
pub type Result<T> = std::result::Result<T, Error>;
