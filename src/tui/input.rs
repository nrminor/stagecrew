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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key_event(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn make_key_event_with_mods(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn sort_mode_cycles_on_s_key() {
        let mut app = App::new();
        assert_eq!(
            app.sort_mode,
            SortMode::Expiration,
            "Default sort mode should be Expiration"
        );

        // Press 's' - should cycle to Size
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('s')));
        assert_eq!(
            app.sort_mode,
            SortMode::Size,
            "Sort mode should cycle to Size"
        );

        // Press 's' again - should cycle to Name
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('s')));
        assert_eq!(
            app.sort_mode,
            SortMode::Name,
            "Sort mode should cycle to Name"
        );

        // Press 's' again - should cycle back to Expiration
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('s')));
        assert_eq!(
            app.sort_mode,
            SortMode::Expiration,
            "Sort mode should cycle back to Expiration"
        );
    }

    #[test]
    fn sort_mode_persists_across_navigation() {
        let mut app = App::new();

        // Change to Size sort
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Size);

        // Navigate down
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('j')));
        assert_eq!(
            app.sort_mode,
            SortMode::Size,
            "Sort mode should persist after navigation"
        );

        // Navigate up
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('k')));
        assert_eq!(
            app.sort_mode,
            SortMode::Size,
            "Sort mode should persist after navigation"
        );
    }

    #[test]
    fn sort_mode_persists_across_view_changes() {
        let mut app = App::new();

        // Change to Name sort
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('s')));
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Name);

        // Switch to pending approvals view
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('p')));
        assert_eq!(app.view, View::PendingApprovals);
        assert_eq!(
            app.sort_mode,
            SortMode::Name,
            "Sort mode should persist across view changes"
        );

        // Return to directory list
        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('q')));
        assert_eq!(app.view, View::DirectoryList);
        assert_eq!(
            app.sort_mode,
            SortMode::Name,
            "Sort mode should persist when returning to directory list"
        );
    }

    #[test]
    fn quit_on_q_key() {
        let mut app = App::new();
        assert!(!app.should_quit);

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('q')));
        assert!(app.should_quit, "App should quit on 'q' key");
    }

    #[test]
    fn quit_on_ctrl_c() {
        let mut app = App::new();
        assert!(!app.should_quit);

        InputHandler::handle(
            &mut app,
            make_key_event_with_mods(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert!(app.should_quit, "App should quit on Ctrl+C");
    }

    #[test]
    fn navigation_j_increments_index() {
        let mut app = App::new();
        assert_eq!(app.selected_index, 0);

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('j')));
        assert_eq!(app.selected_index, 1, "'j' should increment selected index");

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('j')));
        assert_eq!(
            app.selected_index, 2,
            "'j' should increment selected index again"
        );
    }

    #[test]
    fn navigation_k_decrements_index() {
        let mut app = App::new();
        app.selected_index = 5;

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('k')));
        assert_eq!(app.selected_index, 4, "'k' should decrement selected index");

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('k')));
        assert_eq!(
            app.selected_index, 3,
            "'k' should decrement selected index again"
        );
    }

    #[test]
    fn navigation_g_goes_to_top() {
        let mut app = App::new();
        app.selected_index = 10;

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('g')));
        assert_eq!(app.selected_index, 0, "'g' should go to top (index 0)");
    }

    #[test]
    fn navigation_capital_g_goes_to_bottom() {
        let mut app = App::new();
        app.list_len.set(10);

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('G')));
        assert_eq!(
            app.selected_index, 9,
            "'G' should go to bottom (list_len - 1)"
        );
    }

    #[test]
    fn view_switching_with_p_key() {
        let mut app = App::new();
        assert_eq!(app.view, View::DirectoryList);

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('p')));
        assert_eq!(
            app.view,
            View::PendingApprovals,
            "'p' should switch to pending approvals view"
        );
    }

    #[test]
    fn view_switching_with_a_key() {
        let mut app = App::new();
        assert_eq!(app.view, View::DirectoryList);

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('a')));
        assert_eq!(
            app.view,
            View::AuditLog,
            "'a' should switch to audit log view"
        );
    }

    #[test]
    fn view_switching_with_question_mark_key() {
        let mut app = App::new();
        assert_eq!(app.view, View::DirectoryList);

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('?')));
        assert_eq!(app.view, View::Help, "'?' should switch to help view");
    }

    #[test]
    fn help_view_closes_on_any_key() {
        let mut app = App::new();
        app.view = View::Help;

        InputHandler::handle(&mut app, make_key_event(KeyCode::Char('x')));
        assert_eq!(
            app.view,
            View::DirectoryList,
            "Any key should close help view and return to directory list"
        );
    }
}
