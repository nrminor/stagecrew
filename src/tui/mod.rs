//! TUI application state and main loop.

mod input;
mod ui;

use std::cell::Cell;
use std::collections::HashSet;
use std::io::{self, Stdout};

use crossterm::ExecutableCommand;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, EventStream, MouseEvent, MouseEventKind,
};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::config::Config;
use crate::db::Database;
use crate::error::Result;
use crate::scanner::{Scanner, scan_and_persist};

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
// Allow: The bools represent independent flags (quit, sidebar visibility, scan state)
// that don't naturally form a state machine. Each has distinct semantics.
#[allow(clippy::struct_excessive_bools)]
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

    /// Pending add path text input state.
    /// Contains the accumulated input buffer for the new path to add.
    pub(crate) pending_add_path: Option<String>,

    /// Pending remove path confirmation state.
    /// Contains the path awaiting user confirmation for removal from config.
    pub(crate) pending_remove_path: Option<String>,

    /// Whether the sidebar is visible.
    pub(crate) sidebar_visible: bool,

    /// Whether a scan has been requested (set by R keybind, cleared by main loop).
    pub(crate) scan_requested: bool,

    /// Whether a scan is currently in progress.
    pub(crate) scan_in_progress: bool,

    /// Status message to display (e.g., "Scanning...", "Scan complete").
    pub(crate) status_message: Option<String>,

    /// When the status message was set (for auto-clearing after timeout).
    pub(crate) status_message_time: Option<std::time::Instant>,
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

    /// Get the pending add path input state.
    pub fn pending_add_path(&self) -> Option<&str> {
        self.pending_add_path.as_deref()
    }

    /// Get the pending remove path confirmation state.
    pub fn pending_remove_path(&self) -> Option<&str> {
        self.pending_remove_path.as_deref()
    }

    /// Check if the sidebar is visible.
    pub fn sidebar_visible(&self) -> bool {
        self.sidebar_visible
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
    Sidebar,

    /// Main panel with file list.
    #[default]
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
            pending_add_path: None,
            pending_remove_path: None,
            sidebar_visible: true,
            scan_requested: false,
            scan_in_progress: false,
            status_message: None,
            status_message_time: None,
        }
    }

    /// Run the TUI main loop.
    ///
    /// This sets up the terminal in raw mode with an alternate screen, then enters
    /// the main event loop. The loop uses `tokio::select!` to handle both keyboard
    /// events and background task completion without blocking.
    ///
    /// The terminal is guaranteed to be restored to its original state on exit,
    /// even if a panic occurs, through the `TerminalManager`'s Drop implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if terminal setup fails or if rendering encounters an I/O error.
    pub async fn run(
        &mut self,
        config: &Config,
        db: &Database,
        db_path: &std::path::Path,
    ) -> Result<()> {
        let mut terminal_manager = TerminalManager::setup().map_err(crate::error::Error::Io)?;
        let mut event_stream = EventStream::new();

        // Channel for receiving scan completion results
        let (scan_tx, mut scan_rx) =
            tokio::sync::mpsc::channel::<std::result::Result<(), String>>(1);

        // Track the scan task handle so we can await it properly
        let mut scan_handle: Option<tokio::task::JoinHandle<()>> = None;

        // Main event loop
        loop {
            // Render the current state
            terminal_manager
                .terminal_mut()
                .draw(|frame| ui::render(self, config, db, frame))
                .map_err(crate::error::Error::Io)?;

            // Check if we should quit (after rendering final state)
            if self.should_quit {
                break;
            }

            // Clear status message after 3 seconds
            if let Some(time) = self.status_message_time
                && time.elapsed() > std::time::Duration::from_secs(3)
            {
                self.status_message = None;
                self.status_message_time = None;
            }

            // Check if a scan was requested and none is in progress
            if self.scan_requested && !self.scan_in_progress {
                self.scan_requested = false;
                self.scan_in_progress = true;
                self.status_message = Some("Scanning...".to_string());
                // Don't set timestamp for "Scanning..." - we want it to persist until done

                // Clone what we need for the background task
                let scanner = Scanner::new();
                let tracked_paths = config.tracked_paths.clone();
                let expiration_days = config.expiration_days;
                let warning_days = config.warning_days;
                let task_db_path = db_path.to_path_buf();
                let tx = scan_tx.clone();

                // Spawn the scan as a background task
                // We use a separate tokio task that runs the scan. The scan internally
                // uses spawn_blocking for filesystem operations, so this is safe.
                scan_handle = Some(tokio::spawn(async move {
                    // Run the scan in a way that's compatible with the non-Send Database.
                    // We create a new runtime context for the blocking database operations.
                    let scan_result = tokio::task::spawn_blocking(move || {
                        // Create a new runtime for the async scan operation
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(async {
                            match Database::open(&task_db_path) {
                                Ok(task_db) => scan_and_persist(
                                    &task_db,
                                    &scanner,
                                    &tracked_paths,
                                    expiration_days,
                                    warning_days,
                                )
                                .await
                                .map(|_| ())
                                .map_err(|e| e.to_string()),
                                Err(e) => Err(format!("Failed to open database: {e}")),
                            }
                        })
                    })
                    .await
                    .unwrap_or_else(|e| Err(format!("Scan task panicked: {e}")));

                    // Send result back (ignore send errors if receiver dropped)
                    let _ = tx.send(scan_result).await;
                }));
            }

            // Use select! to handle events and scan completion concurrently
            tokio::select! {
                // Handle keyboard/mouse events
                maybe_event = event_stream.next() => {
                    match maybe_event {
                        Some(Ok(Event::Key(key))) => {
                            InputHandler::handle(self, config, db, key);
                        }
                        Some(Ok(Event::Mouse(mouse))) => {
                            Self::handle_mouse_event(self, mouse);
                        }
                        _ => {}
                    }
                }

                // Handle scan completion
                Some(result) = scan_rx.recv() => {
                    self.scan_in_progress = false;
                    scan_handle = None;
                    self.status_message_time = Some(std::time::Instant::now());
                    match result {
                        Ok(()) => {
                            self.status_message = Some("Scan complete".to_string());
                        }
                        Err(e) => {
                            self.status_message = Some(format!("Scan failed: {e}"));
                        }
                    }
                }

                // Timeout to ensure we re-render periodically even without events
                // This clears status messages after a delay and keeps UI responsive
                () = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Clear status message after showing it for a bit
                    // (A more sophisticated approach would track message age)
                }
            }
        }

        // Clean up any running scan task
        if let Some(handle) = scan_handle {
            handle.abort();
        }

        // Terminal cleanup happens automatically via Drop
        Ok(())
    }

    /// Handle mouse events (scroll wheel navigation).
    fn handle_mouse_event(&mut self, event: MouseEvent) {
        match event.kind {
            MouseEventKind::ScrollDown => {
                // Scroll down = move selection down (same as 'j')
                match self.focus_panel {
                    FocusPanel::Sidebar => {
                        self.sidebar_selected_index = self.sidebar_selected_index.saturating_add(1);
                    }
                    FocusPanel::MainPanel => {
                        self.file_selected_index = self.file_selected_index.saturating_add(1);
                    }
                }
            }
            MouseEventKind::ScrollUp => {
                // Scroll up = move selection up (same as 'k')
                match self.focus_panel {
                    FocusPanel::Sidebar => {
                        self.sidebar_selected_index = self.sidebar_selected_index.saturating_sub(1);
                    }
                    FocusPanel::MainPanel => {
                        self.file_selected_index = self.file_selected_index.saturating_sub(1);
                    }
                }
            }
            _ => {
                // Ignore other mouse events (clicks, drags, etc.)
            }
        }
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
            FocusPanel::MainPanel,
            "App should start with main panel focused for immediate file interaction"
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
        assert_eq!(
            app.pending_add_path, None,
            "App should start with no pending add path"
        );
        assert_eq!(
            app.pending_remove_path, None,
            "App should start with no pending remove path"
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
