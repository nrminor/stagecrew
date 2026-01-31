//! Database schema, queries, and migrations.

// TODO(cleanup): Remove these allows as functionality is implemented and used.
// Tracking issue: conn() accessor awaiting service layer integration.
#![allow(dead_code)]

use std::path::PathBuf;

use rusqlite::Connection;

use crate::error::Result;

/// Database handle for stagecrew state.
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Open or create the database at the given path.
    pub fn open(path: &PathBuf) -> Result<Self> {
        let conn = Connection::open(path)?;
        let db = Self { conn };
        db.initialize()?;
        Ok(db)
    }

    /// Initialize database schema.
    fn initialize(&self) -> Result<()> {
        self.conn.execute_batch(include_str!("schema.sql"))?;
        Ok(())
    }

    /// Get a reference to the underlying connection.
    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}
