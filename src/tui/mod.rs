//! TUI application state and main loop.

mod input;
mod ui;

use std::cell::Cell;
use std::collections::HashSet;
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
    /// Directory ID being deferred (or file ID for file deferrals - field name is misleading due to legacy code).
    pub directory_id: i64,

    /// Path of the directory being deferred (or first path in multi-select).
    pub path: String,

    /// Accumulated input buffer for days to defer.
    pub input: String,

    /// Default number of days to defer (from config).
    pub default_days: u32,

    /// Additional file IDs for multi-select deferral (excludes `directory_id` which is included separately).
    pub additional_file_ids: Vec<i64>,
}

/// Main TUI application state.
pub struct App {
    /// Whether the app should quit.
    pub(crate) should_quit: bool,

    /// Current view mode.
    pub(crate) view: View,

    /// Which panel is focused (sidebar or main panel) in `FileList` view.
    pub(crate) focus_panel: FocusPanel,

    /// Currently selected index in the sidebar (tracked directories).
    pub(crate) sidebar_selected_index: usize,

    /// Currently selected index in the main panel (files).
    pub(crate) file_selected_index: usize,

    /// Current sort mode for files.
    pub(crate) sort_mode: SortMode,

    /// Filter for days until expiration.
    // Allow: Planned feature for filtering file list by expiration days.
    // Reserved for future implementation.
    #[allow(dead_code)]
    pub(crate) filter_days: Option<u32>,

    /// Length of the sidebar list (updated by render, used for navigation bounds).
    pub(crate) sidebar_len: Cell<usize>,

    /// Length of the file list (updated by render, used for navigation bounds).
    pub(crate) file_list_len: Cell<usize>,

    /// The directory ID currently selected in sidebar for viewing files.
    /// Uses Cell for interior mutability since it's updated during rendering.
    pub(crate) current_directory_id: Cell<Option<i64>>,

    /// Pending approval confirmation state (legacy directory-level).
    /// Contains the directory ID and path awaiting user confirmation for approval.
    pub(crate) pending_approval: Option<(i64, String)>,

    /// Pending deferral input state (legacy directory-level).
    pub(crate) pending_deferral: Option<PendingDeferral>,

    /// Pending ignore confirmation state (legacy directory-level).
    /// Contains the directory ID and path awaiting user confirmation for ignoring.
    pub(crate) pending_ignore: Option<(i64, String)>,

    /// Pending file deletion confirmation state.
    /// Contains a vector of (`file_id`, path) tuples awaiting user confirmation for deletion.
    pub(crate) pending_file_delete: Option<Vec<(i64, String)>>,

    /// Pending file deferral input state.
    pub(crate) pending_file_deferral: Option<PendingDeferral>,

    /// Pending file ignore confirmation state.
    /// Contains a vector of (`file_id`, path) tuples awaiting user confirmation for ignoring.
    pub(crate) pending_file_ignore: Option<Vec<(i64, String)>>,

    /// Pending file approval confirmation state.
    /// Contains a vector of (`file_id`, path) tuples awaiting user confirmation for approval.
    pub(crate) pending_file_approval: Option<Vec<(i64, String)>>,

    /// Set of selected file IDs for multi-select operations.
    pub(crate) selected_files: HashSet<i64>,
}

impl App {
    /// Get the current view.
    pub fn view(&self) -> View {
        self.view
    }

    /// Get which panel is focused.
    pub fn focus_panel(&self) -> FocusPanel {
        self.focus_panel
    }

    /// Get the currently selected index in sidebar.
    pub fn sidebar_selected_index(&self) -> usize {
        self.sidebar_selected_index
    }

    /// Get the currently selected index in file list.
    pub fn file_selected_index(&self) -> usize {
        self.file_selected_index
    }

    /// Get the current sort mode.
    pub fn sort_mode(&self) -> SortMode {
        self.sort_mode
    }

    /// Get the filter for days until expiration.
    // Allow: Getter for filter_days field which is planned for future implementation.
    #[allow(dead_code)]
    pub fn filter_days(&self) -> Option<u32> {
        self.filter_days
    }

    /// Get the current directory ID selected in sidebar.
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

    /// Get the pending ignore confirmation state.
    pub fn pending_ignore(&self) -> Option<&(i64, String)> {
        self.pending_ignore.as_ref()
    }

    /// Get the pending file deletion confirmation state.
    pub fn pending_file_delete(&self) -> Option<&Vec<(i64, String)>> {
        self.pending_file_delete.as_ref()
    }

    /// Get the pending file deferral input state.
    pub fn pending_file_deferral(&self) -> Option<&PendingDeferral> {
        self.pending_file_deferral.as_ref()
    }

    /// Get the pending file ignore confirmation state.
    pub fn pending_file_ignore(&self) -> Option<&Vec<(i64, String)>> {
        self.pending_file_ignore.as_ref()
    }

    /// Get the pending file approval confirmation state.
    pub fn pending_file_approval(&self) -> Option<&Vec<(i64, String)>> {
        self.pending_file_approval.as_ref()
    }

    /// Get the set of selected file IDs.
    pub fn selected_files(&self) -> &HashSet<i64> {
        &self.selected_files
    }

    /// Clear all file selections.
    pub(crate) fn clear_selection(&mut self) {
        self.selected_files.clear();
    }

    /// Toggle selection of a file ID.
    pub(crate) fn toggle_file_selection(&mut self, file_id: i64) {
        if self.selected_files.contains(&file_id) {
            self.selected_files.remove(&file_id);
        } else {
            self.selected_files.insert(file_id);
        }
    }

    /// Select the last item in the sidebar.
    ///
    /// Sets `sidebar_selected_index` to `len - 1`, or 0 if the list is empty.
    pub(crate) fn select_last_sidebar(&mut self, len: usize) {
        self.sidebar_selected_index = len.saturating_sub(1);
    }

    /// Select the last item in the file list.
    ///
    /// Sets `file_selected_index` to `len - 1`, or 0 if the list is empty.
    pub(crate) fn select_last_file(&mut self, len: usize) {
        self.file_selected_index = len.saturating_sub(1);
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
    /// Main file list view with sidebar for directory navigation.
    #[default]
    FileList,

    /// Audit log view.
    AuditLog,

    /// Help/keybindings view.
    Help,
}

/// Focus panel in the file list view.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FocusPanel {
    /// Sidebar panel with tracked directories.
    #[default]
    Sidebar,

    /// Main panel with file list.
    MainPanel,
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

    /// Sort by modification time (most recent first).
    Modified,
}

impl App {
    /// Create a new TUI application.
    pub fn new() -> Self {
        Self {
            should_quit: false,
            view: View::default(),
            focus_panel: FocusPanel::default(),
            sidebar_selected_index: 0,
            file_selected_index: 0,
            sort_mode: SortMode::default(),
            filter_days: None,
            sidebar_len: Cell::new(0),
            file_list_len: Cell::new(0),
            current_directory_id: Cell::new(None),
            pending_approval: None,
            pending_deferral: None,
            pending_ignore: None,
            pending_file_delete: None,
            pending_file_deferral: None,
            pending_file_ignore: None,
            pending_file_approval: None,
            selected_files: HashSet::new(),
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
    ///
    /// # Note on async signature
    ///
    /// This function is async for future compatibility when async database queries
    /// are integrated. Currently the event loop is synchronous but maintaining the
    /// async signature allows smooth transition to async operations without breaking
    /// the API.
    // Allow: Async signature maintained for future compatibility when async database
    // queries are integrated. This prevents a breaking API change later.
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
            View::FileList,
            "App should start in FileList view"
        );
        assert_eq!(
            app.focus_panel,
            FocusPanel::Sidebar,
            "App should start with sidebar focused"
        );
        assert_eq!(
            app.sidebar_selected_index, 0,
            "App should start with sidebar index 0"
        );
        assert_eq!(
            app.file_selected_index, 0,
            "App should start with file index 0"
        );
        assert_eq!(
            app.sort_mode,
            SortMode::Expiration,
            "App should start with Expiration sort mode"
        );
        assert_eq!(app.filter_days, None, "App should start with no filter");
        assert_eq!(
            app.sidebar_len.get(),
            0,
            "App should start with sidebar_len 0"
        );
        assert_eq!(
            app.file_list_len.get(),
            0,
            "App should start with file_list_len 0"
        );
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
        assert_eq!(
            app.pending_ignore, None,
            "App should start with no pending ignore"
        );
        assert_eq!(
            app.pending_file_delete, None,
            "App should start with no pending file delete"
        );
        assert_eq!(
            app.pending_file_deferral, None,
            "App should start with no pending file deferral"
        );
        assert_eq!(
            app.pending_file_ignore, None,
            "App should start with no pending file ignore"
        );
        assert_eq!(
            app.pending_file_approval, None,
            "App should start with no pending file approval"
        );
        assert!(
            app.selected_files.is_empty(),
            "App should start with no selected files"
        );
    }

    #[test]
    fn app_select_last_sidebar_with_empty_list() {
        let mut app = App::new();
        app.select_last_sidebar(0);
        assert_eq!(
            app.sidebar_selected_index, 0,
            "Selecting last in empty sidebar should set index to 0"
        );
    }

    #[test]
    fn app_select_last_sidebar_with_nonempty_list() {
        let mut app = App::new();
        app.select_last_sidebar(10);
        assert_eq!(
            app.sidebar_selected_index, 9,
            "Selecting last in sidebar of 10 should set index to 9"
        );
    }

    #[test]
    fn app_select_last_file_with_empty_list() {
        let mut app = App::new();
        app.select_last_file(0);
        assert_eq!(
            app.file_selected_index, 0,
            "Selecting last in empty file list should set index to 0"
        );
    }

    #[test]
    fn app_select_last_file_with_nonempty_list() {
        let mut app = App::new();
        app.select_last_file(10);
        assert_eq!(
            app.file_selected_index, 9,
            "Selecting last in file list of 10 should set index to 9"
        );
    }

    #[test]
    fn app_getters_return_correct_values() {
        let mut app = App::new();
        app.view = View::Help;
        app.focus_panel = FocusPanel::MainPanel;
        app.sidebar_selected_index = 3;
        app.file_selected_index = 5;
        app.sort_mode = SortMode::Size;
        app.filter_days = Some(30);
        app.current_directory_id.set(Some(42));

        assert_eq!(app.view(), View::Help);
        assert_eq!(app.focus_panel(), FocusPanel::MainPanel);
        assert_eq!(app.sidebar_selected_index(), 3);
        assert_eq!(app.file_selected_index(), 5);
        assert_eq!(app.sort_mode(), SortMode::Size);
        assert_eq!(app.filter_days(), Some(30));
        assert_eq!(app.current_directory_id(), Some(42));
    }
}
