//! TUI application state and main loop.

// TODO(cleanup): Remove these allows as TUI is fully implemented.
// Tracking issue: TUI module is stubbed, awaiting full implementation.
#![allow(dead_code, unused)]

mod input;
mod ui;

use std::cell::Cell;
use std::io::{self, Stdout};
use std::time::Duration;

use crossterm::ExecutableCommand;
use crossterm::event::{self, DisableMouseCapture, EnableMouseCapture, Event};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::config::Config;
use crate::db::Database;
use crate::error::Result;

use input::InputHandler;

/// State for an in-progress deferral action.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PendingDeferral {
    /// Directory ID being deferred.
    pub directory_id: i64,

    /// Path of the directory being deferred.
    pub path: String,

    /// Accumulated input buffer for days to defer.
    pub input: String,

    /// Default number of days to defer (from config).
    pub default_days: u32,
}

/// Main TUI application state.
pub struct App {
    /// Whether the app should quit.
    pub(crate) should_quit: bool,

    /// Current view mode.
    pub(crate) view: View,

    /// Currently selected index in the list.
    pub(crate) selected_index: usize,

    /// Current sort mode.
    pub(crate) sort_mode: SortMode,

    /// Filter for days until expiration.
    pub(crate) filter_days: Option<u32>,

    /// Length of the current list (updated by render, used for navigation bounds).
    pub(crate) list_len: Cell<usize>,

    /// The directory ID currently being viewed in detail view (None if not in detail view).
    /// Uses Cell for interior mutability since it's updated during rendering.
    pub(crate) current_directory_id: Cell<Option<i64>>,

    /// Pending approval confirmation state.
    /// Contains the directory ID and path awaiting user confirmation for approval.
    pub(crate) pending_approval: Option<(i64, String)>,

    /// Pending deferral input state.
    pub(crate) pending_deferral: Option<PendingDeferral>,
}

impl App {
    /// Get the current view.
    pub fn view(&self) -> View {
        self.view
    }

    /// Get the currently selected index.
    pub fn selected_index(&self) -> usize {
        self.selected_index
    }

    /// Get the current sort mode.
    pub fn sort_mode(&self) -> SortMode {
        self.sort_mode
    }

    /// Get the filter for days until expiration.
    pub fn filter_days(&self) -> Option<u32> {
        self.filter_days
    }

    /// Get the current directory ID being viewed in detail mode.
    pub fn current_directory_id(&self) -> Option<i64> {
        self.current_directory_id.get()
    }

    /// Get the pending approval confirmation state.
    pub fn pending_approval(&self) -> Option<&(i64, String)> {
        self.pending_approval.as_ref()
    }

    /// Get the pending deferral input state.
    pub fn pending_deferral(&self) -> Option<&PendingDeferral> {
        self.pending_deferral.as_ref()
    }

    /// Select the last item in a list of the given length.
    ///
    /// Sets `selected_index` to `len - 1`, or 0 if the list is empty.
    pub(crate) fn select_last(&mut self, len: usize) {
        self.selected_index = len.saturating_sub(1);
    }
}

/// Terminal manager that ensures proper cleanup.
///
/// This struct handles terminal setup and teardown, guaranteeing cleanup
/// even if the program panics through its Drop implementation.
struct TerminalManager {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalManager {
    /// Set up the terminal for TUI mode.
    ///
    /// # Errors
    ///
    /// Returns an error if terminal setup fails (raw mode, alternate screen, mouse capture, or backend creation).
    fn setup() -> io::Result<Self> {
        enable_raw_mode()?;
        io::stdout()
            .execute(EnterAlternateScreen)?
            .execute(EnableMouseCapture)?;

        let backend = CrosstermBackend::new(io::stdout());
        let terminal = Terminal::new(backend)?;

        Ok(Self { terminal })
    }

    /// Get a mutable reference to the terminal.
    fn terminal_mut(&mut self) -> &mut Terminal<CrosstermBackend<Stdout>> {
        &mut self.terminal
    }
}

impl Drop for TerminalManager {
    fn drop(&mut self) {
        // Ensure terminal is restored even on panic.
        // Ignore errors during cleanup - best effort restoration.
        let _ = disable_raw_mode();
        let _ = io::stdout()
            .execute(LeaveAlternateScreen)
            .and_then(|out| out.execute(DisableMouseCapture));
    }
}

/// Available views in the TUI.
#[non_exhaustive]
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
#[non_exhaustive]
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
            list_len: Cell::new(0),
            current_directory_id: Cell::new(None),
            pending_approval: None,
            pending_deferral: None,
        }
    }

    /// Run the TUI main loop.
    ///
    /// This sets up the terminal in raw mode with an alternate screen, then enters
    /// the main event loop. The loop polls for keyboard/mouse events with a timeout,
    /// processes input through the `InputHandler`, and renders the UI via `ui::render`.
    ///
    /// The terminal is guaranteed to be restored to its original state on exit,
    /// even if a panic occurs, through the `TerminalManager`'s Drop implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if terminal setup fails or if rendering encounters an I/O error.
    // TODO(cleanup): Remove allow once TUI main loop becomes async (e.g., async database queries).
    // Currently sync event polling doesn't require await, but keeping async for future compatibility.
    #[allow(clippy::unused_async)]
    pub async fn run(&mut self, config: &Config, db: &Database) -> Result<()> {
        let mut terminal_manager = TerminalManager::setup().map_err(crate::error::Error::Io)?;

        // Main event loop
        while !self.should_quit {
            // Render the current state
            terminal_manager
                .terminal_mut()
                .draw(|frame| ui::render(self, config, db, frame))
                .map_err(crate::error::Error::Io)?;

            // Poll for events with a timeout to limit frame rate
            if event::poll(Duration::from_millis(100)).map_err(crate::error::Error::Io)?
                && let Event::Key(key) = event::read().map_err(crate::error::Error::Io)?
            {
                InputHandler::handle(self, config, db, key);
            }
        }

        // Terminal cleanup happens automatically via Drop
        Ok(())
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_mode_default_is_expiration() {
        let mode = SortMode::default();
        assert_eq!(
            mode,
            SortMode::Expiration,
            "Default sort mode should be Expiration"
        );
    }

    #[test]
    fn app_new_has_correct_defaults() {
        let app = App::new();
        assert!(!app.should_quit, "App should not start in quit state");
        assert_eq!(
            app.view,
            View::DirectoryList,
            "App should start in DirectoryList view"
        );
        assert_eq!(app.selected_index, 0, "App should start with index 0");
        assert_eq!(
            app.sort_mode,
            SortMode::Expiration,
            "App should start with Expiration sort mode"
        );
        assert_eq!(app.filter_days, None, "App should start with no filter");
        assert_eq!(app.list_len.get(), 0, "App should start with list_len 0");
        assert_eq!(
            app.current_directory_id.get(),
            None,
            "App should start with no directory selected"
        );
        assert_eq!(
            app.pending_approval, None,
            "App should start with no pending approval"
        );
        assert_eq!(
            app.pending_deferral, None,
            "App should start with no pending deferral"
        );
    }

    #[test]
    fn app_select_last_with_empty_list() {
        let mut app = App::new();
        app.select_last(0);
        assert_eq!(
            app.selected_index, 0,
            "Selecting last in empty list should set index to 0"
        );
    }

    #[test]
    fn app_select_last_with_nonempty_list() {
        let mut app = App::new();
        app.select_last(10);
        assert_eq!(
            app.selected_index, 9,
            "Selecting last in list of 10 should set index to 9"
        );
    }

    #[test]
    fn app_getters_return_correct_values() {
        let mut app = App::new();
        app.view = View::Help;
        app.selected_index = 5;
        app.sort_mode = SortMode::Size;
        app.filter_days = Some(30);
        app.current_directory_id.set(Some(42));

        assert_eq!(app.view(), View::Help);
        assert_eq!(app.selected_index(), 5);
        assert_eq!(app.sort_mode(), SortMode::Size);
        assert_eq!(app.filter_days(), Some(30));
        assert_eq!(app.current_directory_id(), Some(42));
    }
}
