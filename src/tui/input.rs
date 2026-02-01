//! Vim-style keybinding handling.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::{App, SortMode, View};

/// Handles keyboard input with vim-style bindings.
pub(crate) struct InputHandler;

impl InputHandler {
    /// Process a key event and update app state.
    pub fn handle(app: &mut App, key: KeyEvent) {
        match app.view {
            View::DirectoryList => Self::handle_directory_list(app, key),
            View::DirectoryDetail => Self::handle_directory_detail(app, key),
            View::PendingApprovals => Self::handle_pending_approvals(app, key),
            View::AuditLog => Self::handle_audit_log(app, key),
            View::Help => Self::handle_help(app, key),
        }
    }

    fn handle_directory_list(app: &mut App, key: KeyEvent) {
        match key.code {
            // Quit
            KeyCode::Char('q') => app.should_quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.should_quit = true;
            }

            // Navigation (vim-style)
            KeyCode::Char('j') | KeyCode::Down => {
                app.selected_index = app.selected_index.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                app.selected_index = app.selected_index.saturating_sub(1);
            }
            KeyCode::Char('g') => app.selected_index = 0, // Go to top
            KeyCode::Char('G') => app.select_last(app.list_len.get()), // Go to bottom

            // Enter detail view
            KeyCode::Enter | KeyCode::Char('l') => {
                app.view = View::DirectoryDetail;
            }

            // Sort modes
            KeyCode::Char('s') => {
                app.sort_mode = match app.sort_mode {
                    SortMode::Expiration => SortMode::Size,
                    SortMode::Size => SortMode::Name,
                    SortMode::Name => SortMode::Expiration,
                };
            }

            // Views
            KeyCode::Char('p') => app.view = View::PendingApprovals,
            KeyCode::Char('a') => app.view = View::AuditLog,
            KeyCode::Char('?') => app.view = View::Help,

            // TODO(tui): Implement these actions in future stories
            // 'd' - Defer selected (US-018)
            // 'i' - Ignore selected (US-019)
            // 'x' - Approve removal (US-017)
            _ => {}
        }
    }

    fn handle_directory_detail(app: &mut App, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q' | 'h') | KeyCode::Esc => {
                app.view = View::DirectoryList;
            }
            KeyCode::Char('j') | KeyCode::Down => {
                app.selected_index = app.selected_index.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                app.selected_index = app.selected_index.saturating_sub(1);
            }
            _ => {}
        }
    }

    fn handle_pending_approvals(app: &mut App, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => app.view = View::DirectoryList,
            KeyCode::Char('j') | KeyCode::Down => {
                app.selected_index = app.selected_index.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                app.selected_index = app.selected_index.saturating_sub(1);
            }
            // TODO(tui): Implement 'x' (approve) and 'd' (defer) actions
            _ => {}
        }
    }

    fn handle_audit_log(app: &mut App, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => app.view = View::DirectoryList,
            KeyCode::Char('j') | KeyCode::Down => {
                app.selected_index = app.selected_index.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                app.selected_index = app.selected_index.saturating_sub(1);
            }
            _ => {}
        }
    }

    fn handle_help(app: &mut App, key: KeyEvent) {
        // Any key closes the help view
        let _ = key;
        app.view = View::DirectoryList;
    }
}
