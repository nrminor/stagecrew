//! TUI application state and main loop.

// TODO(cleanup): Remove these allows as TUI is fully implemented.
// Tracking issue: TUI module is stubbed, awaiting full implementation.
#![allow(dead_code, unused)]

mod input;
mod ui;

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
    pub async fn run(&mut self, _config: &Config, _db: &Database) -> Result<()> {
        let mut terminal_manager = TerminalManager::setup().map_err(crate::error::Error::Io)?;

        // Main event loop
        while !self.should_quit {
            // Render the current state
            terminal_manager
                .terminal_mut()
                .draw(|frame| ui::render(self, frame))
                .map_err(crate::error::Error::Io)?;

            // Poll for events with a timeout to limit frame rate
            if event::poll(Duration::from_millis(100)).map_err(crate::error::Error::Io)?
                && let Event::Key(key) = event::read().map_err(crate::error::Error::Io)?
            {
                InputHandler::handle(self, key);
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
