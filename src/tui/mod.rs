//! TUI application state and main loop.

// TODO(cleanup): Remove these allows as TUI is fully implemented.
// Tracking issue: TUI module is stubbed, awaiting full implementation.
#![allow(dead_code, unused)]

mod input;
mod ui;

use crate::config::Config;
use crate::db::Database;
use crate::error::Result;

/// Main TUI application state.
pub struct App {
    /// Whether the app should quit.
    pub should_quit: bool,

    /// Current view mode.
    pub view: View,

    /// Currently selected index in the list.
    pub selected_index: usize,

    /// Current sort mode.
    pub sort_mode: SortMode,

    /// Filter for days until expiration.
    pub filter_days: Option<u32>,
}

/// Available views in the TUI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum View {
    /// Main directory list view.
    #[default]
    DirectoryList,

    /// Detailed view of a single directory's files.
    DirectoryDetail,

    /// Pending approvals view.
    PendingApprovals,

    /// Audit log view.
    AuditLog,

    /// Help/keybindings view.
    Help,
}

/// Sort modes for the directory list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortMode {
    /// Sort by time until expiration (default, most urgent first).
    #[default]
    Expiration,

    /// Sort by size (largest first).
    Size,

    /// Sort by path name.
    Name,
}

impl App {
    /// Create a new TUI application.
    pub fn new() -> Self {
        Self {
            should_quit: false,
            view: View::default(),
            selected_index: 0,
            sort_mode: SortMode::default(),
            filter_days: None,
        }
    }

    /// Run the TUI main loop.
    // TODO(cleanup): Remove allow once event loop uses async crossterm events.
    #[allow(clippy::unused_async)]
    pub async fn run(&mut self, _config: &Config, _db: &Database) -> Result<()> {
        // TODO: Implement TUI main loop with:
        // - Terminal setup/teardown
        // - Event loop
        // - Rendering

        Ok(())
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}
