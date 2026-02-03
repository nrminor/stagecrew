//! TUI application state and main loop.

mod input;
mod ui;

use std::cell::Cell;
use std::collections::HashSet;
use std::io::{self, Stdout};
use std::path::PathBuf;

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

/// An entry awaiting user confirmation for a pending action (delete, ignore, approve, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingEntry {
    /// Database row ID.
    pub id: i64,
    /// Absolute filesystem path.
    pub path: PathBuf,
    /// Whether this entry is a directory (affects propagation to children).
    pub is_dir: bool,
}

/// State for an in-progress deferral action.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PendingDeferral {
    /// Entries being deferred.
    pub entries: Vec<PendingEntry>,

    /// Accumulated input buffer for days to defer.
    pub input: String,

    /// Default number of days to defer (from config).
    pub default_days: u32,
}

/// Main TUI application state.
// Allow: The bools represent independent flags (quit, sidebar visibility, scan state,
// search input) that don't naturally form a state machine. Each has distinct semantics.
#[allow(clippy::struct_excessive_bools)]
pub struct App {
    /// Whether the app should quit.
    pub(crate) should_quit: bool,

    /// Current view mode.
    pub(crate) view: View,

    /// Which panel is focused (sidebar or main panel) in `FileList` view.
    pub(crate) focus_panel: FocusPanel,

    /// Currently selected index in the sidebar (tracked roots).
    pub(crate) sidebar_selected_index: usize,

    /// Currently selected index in the main panel (entries).
    pub(crate) entry_selected_index: usize,

    /// Current sort mode for entries.
    pub(crate) sort_mode: SortMode,

    /// Filter for days until expiration.
    // Allow: Planned feature for filtering entry list by expiration days.
    // Reserved for future implementation.
    #[allow(dead_code)]
    pub(crate) filter_days: Option<u32>,

    /// Length of the sidebar list (updated by render, used for navigation bounds).
    pub(crate) sidebar_len: Cell<usize>,

    /// Length of the entry list (updated by render, used for navigation bounds).
    pub(crate) entry_list_len: Cell<usize>,

    /// The root ID currently selected in sidebar for viewing entries.
    /// Uses Cell for interior mutability since it's updated during rendering.
    pub(crate) current_root_id: Cell<Option<i64>>,

    /// The current path being browsed within the selected root.
    /// This enables hierarchical navigation within a root.
    pub(crate) current_path: String,

    /// Pending entry deletion confirmation state.
    pub(crate) pending_entry_delete: Option<Vec<PendingEntry>>,

    /// Pending entry deferral input state.
    pub(crate) pending_entry_deferral: Option<PendingDeferral>,

    /// Pending entry ignore confirmation state.
    pub(crate) pending_entry_ignore: Option<Vec<PendingEntry>>,

    /// Pending entry approval confirmation state.
    pub(crate) pending_entry_approval: Option<Vec<PendingEntry>>,

    /// Set of selected entry IDs for multi-select operations.
    pub(crate) selected_entries: HashSet<i64>,

    /// Visual mode anchor index. `Some(idx)` means visual mode is active,
    /// anchored at position `idx` in the sorted entry list. `None` means
    /// normal mode.
    pub(crate) visual_anchor: Option<usize>,

    /// Snapshot of selected entries taken when visual mode was entered.
    /// Preserves pre-existing Space selections so the visual range is
    /// additive rather than replacing them.
    pub(crate) pre_visual_selection: HashSet<i64>,

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

    /// Cached header stats, refreshed after actions and scans.
    pub(crate) cached_stats: crate::db::Stats,

    /// Active search query. `Some` means a search is active (either typing or
    /// navigating matches). `None` means normal mode with no search.
    pub(crate) search_query: Option<String>,

    /// Whether the user is currently typing into the search input.
    /// When true, keystrokes go to the search buffer. When false (but
    /// `search_query` is `Some`), the user can navigate matches with n/N.
    pub(crate) search_input_active: bool,
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

    /// Get the currently selected index in entry list.
    pub fn entry_selected_index(&self) -> usize {
        self.entry_selected_index
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

    /// Get the current root ID selected in sidebar.
    // Allow: Will be used by TUI-014 directory navigation to determine root boundaries.
    #[allow(dead_code)]
    pub fn current_root_id(&self) -> Option<i64> {
        self.current_root_id.get()
    }

    /// Get the current path being browsed.
    pub fn current_path(&self) -> &str {
        &self.current_path
    }

    /// Get the pending entry deletion confirmation state.
    pub fn pending_entry_delete(&self) -> Option<&Vec<PendingEntry>> {
        self.pending_entry_delete.as_ref()
    }

    /// Get the pending entry deferral input state.
    pub fn pending_entry_deferral(&self) -> Option<&PendingDeferral> {
        self.pending_entry_deferral.as_ref()
    }

    /// Get the pending entry ignore confirmation state.
    pub fn pending_entry_ignore(&self) -> Option<&Vec<PendingEntry>> {
        self.pending_entry_ignore.as_ref()
    }

    /// Get the pending entry approval confirmation state.
    pub fn pending_entry_approval(&self) -> Option<&Vec<PendingEntry>> {
        self.pending_entry_approval.as_ref()
    }

    /// Get the set of selected entry IDs.
    pub fn selected_entries(&self) -> &HashSet<i64> {
        &self.selected_entries
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

    /// Clear all entry selections.
    pub(crate) fn clear_selection(&mut self) {
        self.selected_entries.clear();
    }

    /// Toggle selection of an entry ID.
    pub(crate) fn toggle_entry_selection(&mut self, entry_id: i64) {
        if self.selected_entries.contains(&entry_id) {
            self.selected_entries.remove(&entry_id);
        } else {
            self.selected_entries.insert(entry_id);
        }
    }

    /// Whether visual mode is active.
    pub fn is_visual_mode(&self) -> bool {
        self.visual_anchor.is_some()
    }

    /// Exit visual mode, keeping the current selection intact.
    pub(crate) fn exit_visual_mode(&mut self) {
        self.visual_anchor = None;
        self.pre_visual_selection.clear();
    }

    /// Recompute `selected_entries` from the visual range plus the pre-visual snapshot.
    ///
    /// The visual range spans from the anchor to the cursor (inclusive) in the
    /// sorted entry list. The result is the union of that range with whatever
    /// was selected before visual mode was entered.
    pub(crate) fn recompute_visual_selection(&mut self, entry_ids: &[i64]) {
        let Some(anchor) = self.visual_anchor else {
            return;
        };
        let cursor = self.entry_selected_index;
        let lo = anchor.min(cursor);
        let hi = anchor.max(cursor).min(entry_ids.len().saturating_sub(1));

        self.selected_entries = self.pre_visual_selection.clone();
        for &id in &entry_ids[lo..=hi] {
            self.selected_entries.insert(id);
        }
    }

    /// Select the last item in the sidebar.
    ///
    /// Sets `sidebar_selected_index` to `len - 1`, or 0 if the list is empty.
    pub(crate) fn select_last_sidebar(&mut self, len: usize) {
        self.sidebar_selected_index = len.saturating_sub(1);
    }

    /// Select the last item in the entry list.
    ///
    /// Sets `entry_selected_index` to `len - 1`, or 0 if the list is empty.
    pub(crate) fn select_last_entry(&mut self, len: usize) {
        self.entry_selected_index = len.saturating_sub(1);
    }

    /// Clear any active search state.
    pub(crate) fn clear_search(&mut self) {
        self.search_query = None;
        self.search_input_active = false;
    }

    /// Navigate into a directory entry.
    ///
    /// Sets the current path to the given directory path and resets entry selection.
    /// Also clears any active search since results are directory-specific.
    pub(crate) fn navigate_into(&mut self, path: String) {
        self.current_path = path;
        self.entry_selected_index = 0;
        self.clear_search();
    }

    /// Auto-enter the first root if no root is currently entered.
    ///
    /// Called at startup and after scan completion so the user sees files
    /// immediately instead of the empty "Select a root" prompt. This is a
    /// no-op when a root is already entered (i.e., `current_path` is non-empty).
    pub(crate) fn auto_enter_first_root(&mut self, db: &crate::db::Database) {
        if !self.current_path.is_empty() {
            return;
        }
        if let Ok(roots) = db.list_roots()
            && !roots.is_empty()
        {
            self.navigate_into(roots[0].path.clone());
            self.current_root_id.set(Some(roots[0].id));
        }
    }

    /// Navigate up to the parent directory.
    ///
    /// If already at a root level, this is a no-op.
    /// Clears any active search since results are directory-specific.
    pub(crate) fn navigate_up(&mut self) {
        if let Some(parent) = std::path::Path::new(&self.current_path).parent() {
            self.current_path = parent.to_string_lossy().to_string();
            self.entry_selected_index = 0;
            self.clear_search();
        }
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
            entry_selected_index: 0,
            sort_mode: SortMode::default(),
            filter_days: None,
            sidebar_len: Cell::new(0),
            entry_list_len: Cell::new(0),
            current_root_id: Cell::new(None),
            current_path: String::new(),
            pending_entry_delete: None,
            pending_entry_deferral: None,
            pending_entry_ignore: None,
            pending_entry_approval: None,
            selected_entries: HashSet::new(),
            visual_anchor: None,
            pre_visual_selection: HashSet::new(),
            pending_add_path: None,
            pending_remove_path: None,
            sidebar_visible: true,
            scan_requested: false,
            scan_in_progress: false,
            status_message: None,
            status_message_time: None,
            cached_stats: crate::db::Stats {
                total_files: 0,
                total_size_bytes: 0,
                files_within_warning: 0,
                files_pending_approval: 0,
                files_overdue: 0,
                last_scan_completed: None,
            },
            search_query: None,
            search_input_active: false,
        }
    }

    /// Refresh cached header stats from the entries table.
    ///
    /// Call this after any action that changes entry status (ignore, approve,
    /// defer, unignore, delete) and after scan completion.
    pub(crate) fn refresh_stats(
        &mut self,
        db: &crate::db::Database,
        config: &crate::config::Config,
    ) {
        match db.compute_live_stats(config.expiration_days, config.warning_days) {
            Ok(stats) => {
                tracing::debug!(
                    total_files = stats.total_files,
                    total_size = stats.total_size_bytes,
                    "Refreshed live stats"
                );
                self.cached_stats = stats;
            }
            Err(e) => tracing::warn!("Failed to refresh stats: {e}"),
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

        // Auto-enter the first root so the user sees files immediately
        // instead of staring at "Select a root from the sidebar".
        self.auto_enter_first_root(db);

        // Channel for receiving scan completion results
        let (scan_tx, mut scan_rx) =
            tokio::sync::mpsc::channel::<std::result::Result<(), String>>(1);

        // Track the scan task handle so we can await it properly
        let mut scan_handle: Option<tokio::task::JoinHandle<()>> = None;

        // Load initial stats from the entries table
        self.refresh_stats(db, config);

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
                let config_tracked_paths = config.tracked_paths.clone();
                let expiration_days = config.expiration_days;
                let warning_days = config.warning_days;
                let task_db_path = db_path.to_path_buf();
                let tx = scan_tx.clone();

                // Spawn the scan as a background task.
                // The scan seeds config baseline paths into the DB, then queries
                // all DB roots (config + user-added) for the full set to scan.
                scan_handle = Some(tokio::spawn(async move {
                    let scan_result = tokio::task::spawn_blocking(move || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(async {
                            match Database::open(&task_db_path) {
                                Ok(task_db) => scan_and_persist(
                                    &task_db,
                                    &scanner,
                                    &config_tracked_paths,
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
                            self.refresh_stats(db, config);
                            self.auto_enter_first_root(db);
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
                        self.entry_selected_index = self.entry_selected_index.saturating_add(1);
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
                        self.entry_selected_index = self.entry_selected_index.saturating_sub(1);
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
            app.entry_selected_index, 0,
            "App should start with entry index 0"
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
            app.entry_list_len.get(),
            0,
            "App should start with entry_list_len 0"
        );
        assert_eq!(
            app.current_root_id.get(),
            None,
            "App should start with no root selected"
        );
        assert!(
            app.current_path.is_empty(),
            "App should start with empty current_path"
        );
        assert_eq!(
            app.pending_entry_delete, None,
            "App should start with no pending entry delete"
        );
        assert_eq!(
            app.pending_entry_deferral, None,
            "App should start with no pending entry deferral"
        );
        assert_eq!(
            app.pending_entry_ignore, None,
            "App should start with no pending entry ignore"
        );
        assert_eq!(
            app.pending_entry_approval, None,
            "App should start with no pending entry approval"
        );
        assert!(
            app.selected_entries.is_empty(),
            "App should start with no selected entries"
        );
        assert_eq!(
            app.pending_add_path, None,
            "App should start with no pending add path"
        );
        assert_eq!(
            app.pending_remove_path, None,
            "App should start with no pending remove path"
        );
        assert_eq!(
            app.search_query, None,
            "App should start with no search query"
        );
        assert!(
            !app.search_input_active,
            "App should start with search input inactive"
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
    fn app_select_last_entry_with_empty_list() {
        let mut app = App::new();
        app.select_last_entry(0);
        assert_eq!(
            app.entry_selected_index, 0,
            "Selecting last in empty entry list should set index to 0"
        );
    }

    #[test]
    fn app_select_last_entry_with_nonempty_list() {
        let mut app = App::new();
        app.select_last_entry(10);
        assert_eq!(
            app.entry_selected_index, 9,
            "Selecting last in entry list of 10 should set index to 9"
        );
    }

    #[test]
    fn app_getters_return_correct_values() {
        let mut app = App::new();
        app.view = View::Help;
        app.focus_panel = FocusPanel::MainPanel;
        app.sidebar_selected_index = 3;
        app.entry_selected_index = 5;
        app.sort_mode = SortMode::Size;
        app.filter_days = Some(30);
        app.current_root_id.set(Some(42));

        assert_eq!(app.view(), View::Help);
        assert_eq!(app.focus_panel(), FocusPanel::MainPanel);
        assert_eq!(app.sidebar_selected_index(), 3);
        assert_eq!(app.entry_selected_index(), 5);
        assert_eq!(app.sort_mode(), SortMode::Size);
        assert_eq!(app.filter_days(), Some(30));
        assert_eq!(app.current_root_id(), Some(42));
    }

    #[test]
    fn app_navigate_into_sets_path_and_resets_index() {
        let mut app = App::new();
        app.entry_selected_index = 5;
        app.navigate_into("/test/path".to_string());
        assert_eq!(app.current_path, "/test/path");
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn app_navigate_up_goes_to_parent() {
        let mut app = App::new();
        app.current_path = "/test/path/child".to_string();
        app.entry_selected_index = 5;
        app.navigate_up();
        assert_eq!(app.current_path, "/test/path");
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn app_navigate_up_at_root_is_noop() {
        let mut app = App::new();
        app.current_path = "/".to_string();
        app.entry_selected_index = 5;
        app.navigate_up();
        // "/" has no parent, so navigate_up is a no-op (path and index unchanged).
        assert_eq!(app.current_path, "/");
        assert_eq!(app.entry_selected_index, 5);
    }
}
