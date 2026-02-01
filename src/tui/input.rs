//! Vim-style keybinding handling.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::{App, PendingDeferral, SortMode, View};
use crate::audit::{AuditAction, AuditService};
use crate::config::Config;
use crate::db::Database;

/// Handles keyboard input with vim-style bindings.
pub(crate) struct InputHandler;

impl InputHandler {
    /// Process a key event and update app state.
    ///
    /// Takes a config reference for reading default values and a database reference
    /// for actions that modify state (approve, defer, ignore).
    pub fn handle(app: &mut App, config: &Config, db: &Database, key: KeyEvent) {
        match app.view {
            View::DirectoryList => Self::handle_directory_list(app, config, db, key),
            View::DirectoryDetail => Self::handle_directory_detail(app, key),
            View::PendingApprovals => Self::handle_pending_approvals(app, config, db, key),
            View::AuditLog => Self::handle_audit_log(app, key),
            View::Help => Self::handle_help(app, key),
        }
    }

    fn handle_directory_list(app: &mut App, config: &Config, db: &Database, key: KeyEvent) {
        // If there's a pending deferral input, handle numeric input/Enter/Esc
        if app.pending_deferral.is_some() {
            Self::handle_deferral_input(app, config, db, key);
            return;
        }

        // If there's a pending approval confirmation, handle y/n/Esc
        if app.pending_approval.is_some() {
            Self::handle_confirmation(app, db, key);
            return;
        }

        // If there's a pending ignore confirmation, handle y/n/Esc
        if app.pending_ignore.is_some() {
            Self::handle_ignore_confirmation(app, db, key);
            return;
        }

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
            // Note: current_directory_id will be set by the render function
            // when it identifies which directory is selected
            KeyCode::Enter | KeyCode::Char('l') => {
                app.view = View::DirectoryDetail;
                app.selected_index = 0; // Reset selection for file list
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
            KeyCode::Char('a') => {
                app.view = View::AuditLog;
                app.selected_index = 0; // Reset selection for audit log
            }
            KeyCode::Char('?') => app.view = View::Help,

            // Approve removal (US-017)
            KeyCode::Char('x') => {
                // Get the currently selected directory ID from the app state
                // (set by the render function based on selected_index)
                if let Some(dir_id) = app.current_directory_id()
                    && let Ok(directories) = db.list_directories(None)
                    && let Some(dir) = directories.iter().find(|d| d.id == dir_id)
                {
                    // Set pending approval with directory ID and path
                    app.pending_approval = Some((dir.id, dir.path.clone()));
                }
            }

            // Defer expiration (US-018)
            KeyCode::Char('d') => {
                // Get the currently selected directory ID from the app state
                if let Some(dir_id) = app.current_directory_id()
                    && let Ok(directories) = db.list_directories(None)
                    && let Some(dir) = directories.iter().find(|d| d.id == dir_id)
                {
                    // Set pending deferral with directory ID, path, empty input buffer, and default days
                    app.pending_deferral = Some(PendingDeferral {
                        directory_id: dir.id,
                        path: dir.path.clone(),
                        input: String::new(),
                        default_days: config.expiration_days,
                    });
                }
            }

            // Ignore path permanently (US-019)
            KeyCode::Char('i') => {
                // Get the currently selected directory ID from the app state
                if let Some(dir_id) = app.current_directory_id()
                    && let Ok(directories) = db.list_directories(None)
                    && let Some(dir) = directories.iter().find(|d| d.id == dir_id)
                {
                    // Set pending ignore with directory ID and path
                    app.pending_ignore = Some((dir.id, dir.path.clone()));
                }
            }

            _ => {}
        }
    }

    /// Handle confirmation prompt (y/n/Esc) for ignore action.
    fn handle_ignore_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the ignore
                if let Some((dir_id, path)) = &app.pending_ignore {
                    // Update directory status to 'ignored'
                    if let Err(e) = db.update_directory_status(*dir_id, "ignored") {
                        // Log error but continue - user will see status unchanged
                        tracing::warn!("Failed to ignore directory {}: {}", path, e);
                    } else {
                        // Record audit entry
                        let audit = AuditService::new(db);
                        let user = AuditService::current_user();
                        if let Err(e) = audit.record(
                            &user,
                            AuditAction::Ignore,
                            Some(path),
                            None,
                            Some(*dir_id),
                        ) {
                            tracing::warn!("Failed to record audit entry for ignore: {}", e);
                        }
                    }
                }
                // Clear pending ignore
                app.pending_ignore = None;
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel confirmation
                app.pending_ignore = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Handle confirmation prompt (y/n/Esc) for approval action.
    fn handle_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the approval
                if let Some((dir_id, path)) = &app.pending_approval {
                    // Update directory status to 'approved'
                    if let Err(e) = db.update_directory_status(*dir_id, "approved") {
                        // Log error but continue - user will see status unchanged
                        tracing::warn!("Failed to approve directory {}: {}", path, e);
                    } else {
                        // Record audit entry
                        let audit = AuditService::new(db);
                        let user = AuditService::current_user();
                        if let Err(e) = audit.record(
                            &user,
                            AuditAction::Approve,
                            Some(path),
                            None,
                            Some(*dir_id),
                        ) {
                            tracing::warn!("Failed to record audit entry for approval: {}", e);
                        }
                    }
                }
                // Clear pending approval
                app.pending_approval = None;
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel confirmation
                app.pending_approval = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Handle deferral input prompt for numeric days input.
    ///
    /// Accepts digit characters to build up the input string, Enter to confirm,
    /// Backspace to delete digits, and Esc to cancel.
    fn handle_deferral_input(app: &mut App, _config: &Config, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char(c) if c.is_ascii_digit() => {
                // Append digit to input buffer
                if let Some(ref mut deferral) = app.pending_deferral {
                    deferral.input.push(c);
                }
            }
            KeyCode::Backspace => {
                // Remove last digit from input buffer
                if let Some(ref mut deferral) = app.pending_deferral {
                    deferral.input.pop();
                }
            }
            KeyCode::Enter => {
                // User confirmed - process the deferral
                if let Some(deferral) = &app.pending_deferral {
                    // Parse the input as days (or use default if empty)
                    let days: u32 = if deferral.input.is_empty() {
                        // Use default from config
                        deferral.default_days
                    } else if let Ok(parsed_days) = deferral.input.parse::<u32>() {
                        // Validate that days > 0
                        if parsed_days == 0 {
                            // Invalid input - clear and return (user can try again)
                            tracing::warn!("Invalid deferral period: must be > 0");
                            app.pending_deferral = None;
                            return;
                        }
                        parsed_days
                    } else {
                        // Parse error - clear and return
                        tracing::warn!("Invalid deferral input: {}", deferral.input);
                        app.pending_deferral = None;
                        return;
                    };

                    // Calculate deferred_until timestamp
                    let now = jiff::Timestamp::now();
                    let days_i64 = i64::from(days);
                    let deferred_until = now.as_second() + (days_i64 * 86400);

                    // Update directory status to 'deferred' and set deferred_until
                    if let Err(e) = db.update_directory_status(deferral.directory_id, "deferred") {
                        tracing::warn!("Failed to defer directory {}: {}", deferral.path, e);
                    } else {
                        // Update deferred_until timestamp using raw SQL
                        // Note: This is a limitation of the current CRUD interface which doesn't
                        // expose deferred_until updates. Future work: add update_directory_deferral() method.
                        let conn = db.conn();
                        if let Err(e) = conn.execute(
                            "UPDATE directories SET deferred_until = ? WHERE id = ?",
                            rusqlite::params![deferred_until, deferral.directory_id],
                        ) {
                            tracing::warn!(
                                "Failed to set deferred_until for {}: {}",
                                deferral.path,
                                e
                            );
                        } else {
                            // Record audit entry
                            let audit = AuditService::new(db);
                            let user = AuditService::current_user();
                            let details = Some(format!("Deferred for {days} days"));
                            if let Err(e) = audit.record(
                                &user,
                                AuditAction::Defer,
                                Some(&deferral.path),
                                details.as_deref(),
                                Some(deferral.directory_id),
                            ) {
                                tracing::warn!("Failed to record audit entry for deferral: {}", e);
                            }
                        }
                    }
                }
                // Clear pending deferral
                app.pending_deferral = None;
            }
            KeyCode::Esc => {
                // Cancel deferral input
                app.pending_deferral = None;
            }
            _ => {
                // Ignore other keys during input
            }
        }
    }

    fn handle_directory_detail(app: &mut App, key: KeyEvent) {
        match key.code {
            // Return to directory list
            KeyCode::Char('q' | 'h') | KeyCode::Esc => {
                app.view = View::DirectoryList;
                app.current_directory_id.set(None); // Clear the directory context
                app.selected_index = 0; // Reset selection
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

            _ => {}
        }
    }

    fn handle_pending_approvals(app: &mut App, config: &Config, db: &Database, key: KeyEvent) {
        // If there's a pending deferral input, handle numeric input/Enter/Esc
        if app.pending_deferral.is_some() {
            Self::handle_deferral_input(app, config, db, key);
            return;
        }

        // If there's a pending approval confirmation, handle y/n/Esc
        if app.pending_approval.is_some() {
            Self::handle_confirmation(app, db, key);
            return;
        }

        // If there's a pending ignore confirmation, handle y/n/Esc
        if app.pending_ignore.is_some() {
            Self::handle_ignore_confirmation(app, db, key);
            return;
        }

        match key.code {
            // Return to directory list
            KeyCode::Char('q') | KeyCode::Esc => {
                app.view = View::DirectoryList;
                app.selected_index = 0; // Reset selection
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

            // Sort modes
            KeyCode::Char('s') => {
                app.sort_mode = match app.sort_mode {
                    SortMode::Expiration => SortMode::Size,
                    SortMode::Size => SortMode::Name,
                    SortMode::Name => SortMode::Expiration,
                };
            }

            // Approve removal (same as directory list)
            KeyCode::Char('x') => {
                // Get the currently selected directory ID from the app state
                // (set by the render function based on selected_index)
                if let Some(dir_id) = app.current_directory_id()
                    && let Ok(directories) = db.list_directories(Some("pending"))
                    && let Some(dir) = directories.iter().find(|d| d.id == dir_id)
                {
                    // Set pending approval with directory ID and path
                    app.pending_approval = Some((dir.id, dir.path.clone()));
                }
            }

            // Defer expiration (same as directory list)
            KeyCode::Char('d') => {
                // Get the currently selected directory ID from the app state
                if let Some(dir_id) = app.current_directory_id()
                    && let Ok(directories) = db.list_directories(Some("pending"))
                    && let Some(dir) = directories.iter().find(|d| d.id == dir_id)
                {
                    // Set pending deferral with directory ID, path, empty input buffer, and default days
                    app.pending_deferral = Some(PendingDeferral {
                        directory_id: dir.id,
                        path: dir.path.clone(),
                        input: String::new(),
                        default_days: config.expiration_days,
                    });
                }
            }

            // Ignore path permanently (same as directory list)
            KeyCode::Char('i') => {
                // Get the currently selected directory ID from the app state
                if let Some(dir_id) = app.current_directory_id()
                    && let Ok(directories) = db.list_directories(Some("pending"))
                    && let Some(dir) = directories.iter().find(|d| d.id == dir_id)
                {
                    // Set pending ignore with directory ID and path
                    app.pending_ignore = Some((dir.id, dir.path.clone()));
                }
            }

            _ => {}
        }
    }

    fn handle_audit_log(app: &mut App, key: KeyEvent) {
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
            KeyCode::Char('g') => app.selected_index = 0, // Go to top
            KeyCode::Char('G') => app.select_last(app.list_len.get()), // Go to bottom
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
    use crate::config::Config;
    use crate::db::Database;
    use tempfile::tempdir;

    fn make_key_event(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn make_key_event_with_mods(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    fn temp_database() -> (Database, tempfile::TempDir) {
        let dir = tempdir().expect("Failed to create temp dir");
        let db_path = dir.path().join("test.db");
        let db = Database::open(&db_path).expect("Failed to create test database");
        (db, dir)
    }

    fn test_config() -> Config {
        Config::default()
    }

    #[test]
    fn sort_mode_cycles_on_s_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        assert_eq!(
            app.sort_mode,
            SortMode::Expiration,
            "Default sort mode should be Expiration"
        );

        // Press 's' - should cycle to Size
        let config = test_config();
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(
            app.sort_mode,
            SortMode::Size,
            "Sort mode should cycle to Size"
        );

        // Press 's' again - should cycle to Name
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(
            app.sort_mode,
            SortMode::Name,
            "Sort mode should cycle to Name"
        );

        // Press 's' again - should cycle back to Expiration
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(
            app.sort_mode,
            SortMode::Expiration,
            "Sort mode should cycle back to Expiration"
        );
    }

    #[test]
    fn sort_mode_persists_across_navigation() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Change to Size sort
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Size);

        // Navigate down
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('j')));
        assert_eq!(
            app.sort_mode,
            SortMode::Size,
            "Sort mode should persist after navigation"
        );

        // Navigate up
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('k')));
        assert_eq!(
            app.sort_mode,
            SortMode::Size,
            "Sort mode should persist after navigation"
        );
    }

    #[test]
    fn sort_mode_persists_across_view_changes() {
        let (db, _dir) = temp_database();
        let mut app = App::new();

        // Change to Name sort
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('s')),
        );
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('s')),
        );
        assert_eq!(app.sort_mode, SortMode::Name);

        // Switch to pending approvals view
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('p')),
        );
        assert_eq!(app.view, View::PendingApprovals);
        assert_eq!(
            app.sort_mode,
            SortMode::Name,
            "Sort mode should persist across view changes"
        );

        // Return to directory list
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('q')),
        );
        assert_eq!(app.view, View::DirectoryList);
        assert_eq!(
            app.sort_mode,
            SortMode::Name,
            "Sort mode should persist when returning to directory list"
        );
    }

    #[test]
    fn quit_on_q_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        assert!(!app.should_quit);

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('q')),
        );
        assert!(app.should_quit, "App should quit on 'q' key");
    }

    #[test]
    fn quit_on_ctrl_c() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        assert!(!app.should_quit);

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event_with_mods(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert!(app.should_quit, "App should quit on Ctrl+C");
    }

    #[test]
    fn navigation_j_increments_index() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        assert_eq!(app.selected_index, 0);

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('j')),
        );
        assert_eq!(app.selected_index, 1, "'j' should increment selected index");

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('j')),
        );
        assert_eq!(
            app.selected_index, 2,
            "'j' should increment selected index again"
        );
    }

    #[test]
    fn navigation_k_decrements_index() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.selected_index = 5;

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('k')),
        );
        assert_eq!(app.selected_index, 4, "'k' should decrement selected index");

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('k')),
        );
        assert_eq!(
            app.selected_index, 3,
            "'k' should decrement selected index again"
        );
    }

    #[test]
    fn navigation_g_goes_to_top() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.selected_index = 10;

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('g')),
        );
        assert_eq!(app.selected_index, 0, "'g' should go to top (index 0)");
    }

    #[test]
    fn navigation_capital_g_goes_to_bottom() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.list_len.set(10);

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('G')),
        );
        assert_eq!(
            app.selected_index, 9,
            "'G' should go to bottom (list_len - 1)"
        );
    }

    #[test]
    fn view_switching_with_p_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        assert_eq!(app.view, View::DirectoryList);

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('p')),
        );
        assert_eq!(
            app.view,
            View::PendingApprovals,
            "'p' should switch to pending approvals view"
        );
    }

    #[test]
    fn view_switching_with_a_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        assert_eq!(app.view, View::DirectoryList);

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('a')),
        );
        assert_eq!(
            app.view,
            View::AuditLog,
            "'a' should switch to audit log view"
        );
    }

    #[test]
    fn view_switching_with_question_mark_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        assert_eq!(app.view, View::DirectoryList);

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('?')),
        );
        assert_eq!(app.view, View::Help, "'?' should switch to help view");
    }

    #[test]
    fn help_view_closes_on_any_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::Help;

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('x')),
        );
        assert_eq!(
            app.view,
            View::DirectoryList,
            "Any key should close help view and return to directory list"
        );
    }

    // Tests for directory detail view input handling (US-016)

    #[test]
    fn enter_detail_view_on_enter_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.selected_index = 5;
        app.current_directory_id.set(Some(42)); // Would be set by render

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Enter),
        );

        assert_eq!(app.view, View::DirectoryDetail);
        assert_eq!(
            app.selected_index, 0,
            "Selection should reset for file list"
        );
    }

    #[test]
    fn enter_detail_view_on_l_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.current_directory_id.set(Some(42));

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('l')),
        );

        assert_eq!(app.view, View::DirectoryDetail);
        assert_eq!(
            app.selected_index, 0,
            "Selection should reset for file list"
        );
    }

    #[test]
    fn exit_detail_view_on_esc() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::DirectoryDetail;
        app.current_directory_id.set(Some(42));

        InputHandler::handle(&mut app, &test_config(), &db, make_key_event(KeyCode::Esc));

        assert_eq!(app.view, View::DirectoryList);
        assert_eq!(
            app.current_directory_id(),
            None,
            "Directory ID should be cleared"
        );
        assert_eq!(app.selected_index, 0, "Selection should reset");
    }

    #[test]
    fn exit_detail_view_on_h_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::DirectoryDetail;
        app.current_directory_id.set(Some(42));

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('h')),
        );

        assert_eq!(app.view, View::DirectoryList);
        assert_eq!(app.current_directory_id(), None);
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn exit_detail_view_on_q_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::DirectoryDetail;
        app.current_directory_id.set(Some(42));

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('q')),
        );

        assert_eq!(app.view, View::DirectoryList);
        assert_eq!(app.current_directory_id(), None);
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn detail_view_navigation_j_k_works() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::DirectoryDetail;
        app.list_len.set(10);

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('j')),
        );
        assert_eq!(app.selected_index, 1, "j should move down");

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('k')),
        );
        assert_eq!(app.selected_index, 0, "k should move up");
    }

    #[test]
    fn detail_view_navigation_g_capital_g_works() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::DirectoryDetail;
        app.selected_index = 5;
        app.list_len.set(10);

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('g')),
        );
        assert_eq!(app.selected_index, 0, "g should go to top");

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('G')),
        );
        assert_eq!(app.selected_index, 9, "G should go to bottom");
    }

    #[test]
    fn full_detail_view_navigation_flow() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.list_len.set(5);
        app.selected_index = 2;

        // Simulate render setting directory ID
        app.current_directory_id.set(Some(42));

        // Enter detail view
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Enter),
        );
        assert_eq!(app.view, View::DirectoryDetail);
        assert_eq!(app.selected_index, 0, "Selection resets on entry");
        assert_eq!(
            app.current_directory_id(),
            Some(42),
            "Directory ID preserved"
        );

        // Navigate in detail view
        app.list_len.set(3); // Simulate file list
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('j')),
        );
        assert_eq!(app.selected_index, 1);

        // Exit detail view
        InputHandler::handle(&mut app, &test_config(), &db, make_key_event(KeyCode::Esc));
        assert_eq!(app.view, View::DirectoryList);
        assert_eq!(
            app.current_directory_id(),
            None,
            "Directory ID cleared on exit"
        );
        assert_eq!(app.selected_index, 0, "Selection resets on exit");
    }

    // Tests for approval action (US-017)

    #[test]
    fn pressing_x_sets_pending_approval_with_valid_directory() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/path", 1024, 1, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        app.current_directory_id.set(Some(test_dir.id));

        // Press 'x' to initiate approval
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('x')),
        );

        // Should have pending approval set
        assert!(
            app.pending_approval.is_some(),
            "Pending approval should be set"
        );
        let (dir_id, path) = app.pending_approval.as_ref().expect(
            "pending approval should be set by 'x' key press - check InputHandler approval logic",
        );
        assert_eq!(*dir_id, test_dir.id, "Directory ID should match");
        assert_eq!(path, "/test/path", "Path should match");
    }

    #[test]
    fn pressing_x_with_no_directory_does_nothing() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.current_directory_id.set(None);

        // Press 'x' with no directory selected
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('x')),
        );

        // Should not set pending approval
        assert!(
            app.pending_approval.is_none(),
            "Pending approval should not be set with no directory"
        );
    }

    #[test]
    fn pressing_y_approves_directory() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/approve", 2048, 5, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        // Simulate pending approval state (would be set by pressing 'x')
        app.pending_approval = Some((test_dir.id, "/test/approve".to_string()));

        // Press 'y' to confirm
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('y')),
        );

        // Should clear pending approval
        assert!(
            app.pending_approval.is_none(),
            "Pending approval should be cleared after confirmation"
        );

        // Should update directory status to 'approved'
        let updated_dir = db
            .get_directory_by_path("/test/approve")
            .expect("Failed to get directory")
            .expect("Directory should exist");
        assert_eq!(
            updated_dir.status, "approved",
            "Directory status should be 'approved'"
        );
    }

    #[test]
    fn pressing_n_cancels_approval() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/cancel", 1024, 2, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        // Simulate pending approval state
        app.pending_approval = Some((test_dir.id, "/test/cancel".to_string()));

        // Press 'n' to cancel
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('n')),
        );

        // Should clear pending approval
        assert!(
            app.pending_approval.is_none(),
            "Pending approval should be cleared after cancel"
        );

        // Should NOT update directory status (should remain 'tracked')
        let unchanged_dir = db
            .get_directory_by_path("/test/cancel")
            .expect("Failed to get directory")
            .expect("Directory should exist");
        assert_eq!(
            unchanged_dir.status, "tracked",
            "Directory status should remain 'tracked'"
        );
    }

    #[test]
    fn pressing_esc_cancels_approval() {
        let (db, _dir) = temp_database();

        let mut app = App::new();
        app.pending_approval = Some((42, "/test/path".to_string()));

        // Press Esc to cancel
        InputHandler::handle(&mut app, &test_config(), &db, make_key_event(KeyCode::Esc));

        // Should clear pending approval
        assert!(
            app.pending_approval.is_none(),
            "Pending approval should be cleared after Esc"
        );
    }

    #[test]
    fn other_keys_ignored_during_confirmation() {
        let (db, _dir) = temp_database();

        let mut app = App::new();
        app.pending_approval = Some((42, "/test/path".to_string()));

        // Press random key during confirmation
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('j')),
        );

        // Should still have pending approval (ignored)
        assert!(
            app.pending_approval.is_some(),
            "Pending approval should remain during confirmation"
        );
    }

    #[test]
    fn approval_creates_audit_entry() {
        use crate::audit::AuditService;

        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/audit", 4096, 10, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        app.pending_approval = Some((test_dir.id, "/test/audit".to_string()));

        // Press 'y' to approve
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('y')),
        );

        // Check audit log
        let audit = AuditService::new(&db);
        let entries = audit.list_recent(10).expect("Failed to list audit entries");

        assert!(!entries.is_empty(), "Should have at least one audit entry");

        // Find the approve entry
        let approve_entry = entries
            .iter()
            .find(|e| e.action == "approve" && e.target_path == Some("/test/audit".to_string()));

        assert!(
            approve_entry.is_some(),
            "Should have an 'approve' audit entry for the path"
        );
    }

    // Tests for ignore action (US-019)

    #[test]
    fn pressing_i_sets_pending_ignore_with_valid_directory() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/ignore", 512, 2, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        app.current_directory_id.set(Some(test_dir.id));

        // Press 'i' to initiate ignore
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('i')),
        );

        // Should have pending ignore set
        assert!(app.pending_ignore.is_some(), "Pending ignore should be set");
        let (dir_id, path) = app.pending_ignore.as_ref().expect(
            "pending ignore should be set by 'i' key press - check InputHandler ignore logic",
        );
        assert_eq!(*dir_id, test_dir.id, "Directory ID should match");
        assert_eq!(path, "/test/ignore", "Path should match");
    }

    #[test]
    fn pressing_i_with_no_directory_does_nothing() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.current_directory_id.set(None);

        // Press 'i' with no directory selected
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('i')),
        );

        // Should not set pending ignore
        assert!(
            app.pending_ignore.is_none(),
            "Pending ignore should not be set with no directory"
        );
    }

    #[test]
    fn pressing_y_ignores_directory() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/ignore_confirm", 1024, 3, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        // Simulate pending ignore state (would be set by pressing 'i')
        app.pending_ignore = Some((test_dir.id, "/test/ignore_confirm".to_string()));

        // Press 'y' to confirm
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('y')),
        );

        // Should clear pending ignore
        assert!(
            app.pending_ignore.is_none(),
            "Pending ignore should be cleared after confirmation"
        );

        // Should update directory status to 'ignored'
        let updated_dir = db
            .get_directory_by_path("/test/ignore_confirm")
            .expect("Failed to get directory")
            .expect("Directory should exist");
        assert_eq!(
            updated_dir.status, "ignored",
            "Directory status should be 'ignored'"
        );
    }

    #[test]
    fn pressing_n_cancels_ignore() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/cancel_ignore", 2048, 4, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        // Simulate pending ignore state
        app.pending_ignore = Some((test_dir.id, "/test/cancel_ignore".to_string()));

        // Press 'n' to cancel
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('n')),
        );

        // Should clear pending ignore
        assert!(
            app.pending_ignore.is_none(),
            "Pending ignore should be cleared after cancel"
        );

        // Should NOT update directory status (should remain 'tracked')
        let unchanged_dir = db
            .get_directory_by_path("/test/cancel_ignore")
            .expect("Failed to get directory")
            .expect("Directory should exist");
        assert_eq!(
            unchanged_dir.status, "tracked",
            "Directory status should remain 'tracked'"
        );
    }

    #[test]
    fn pressing_esc_cancels_ignore() {
        let (db, _dir) = temp_database();

        let mut app = App::new();
        app.pending_ignore = Some((42, "/test/path".to_string()));

        // Press Esc to cancel
        InputHandler::handle(&mut app, &test_config(), &db, make_key_event(KeyCode::Esc));

        // Should clear pending ignore
        assert!(
            app.pending_ignore.is_none(),
            "Pending ignore should be cleared after Esc"
        );
    }

    #[test]
    fn other_keys_ignored_during_ignore_confirmation() {
        let (db, _dir) = temp_database();

        let mut app = App::new();
        app.pending_ignore = Some((42, "/test/path".to_string()));

        // Press random key during confirmation
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('j')),
        );

        // Should still have pending ignore (ignored)
        assert!(
            app.pending_ignore.is_some(),
            "Pending ignore should remain during confirmation"
        );
    }

    #[test]
    fn ignore_creates_audit_entry() {
        use crate::audit::AuditService;

        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/audit_ignore", 4096, 8, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        app.pending_ignore = Some((test_dir.id, "/test/audit_ignore".to_string()));

        // Press 'y' to ignore
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('y')),
        );

        // Check audit log
        let audit = AuditService::new(&db);
        let entries = audit.list_recent(10).expect("Failed to list audit entries");

        assert!(!entries.is_empty(), "Should have at least one audit entry");

        // Find the ignore entry
        let ignore_entry = entries.iter().find(|e| {
            e.action == "ignore" && e.target_path == Some("/test/audit_ignore".to_string())
        });

        assert!(
            ignore_entry.is_some(),
            "Should have an 'ignore' audit entry for the path"
        );
    }

    // Tests for deferral action (US-018)

    #[test]
    fn pressing_d_sets_pending_deferral_with_valid_directory() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/defer", 2048, 3, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        app.current_directory_id.set(Some(test_dir.id));

        // Press 'd' to initiate deferral
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('d')),
        );

        // Should have pending deferral set
        assert!(
            app.pending_deferral.is_some(),
            "Pending deferral should be set"
        );
        let deferral = app.pending_deferral.as_ref().expect(
            "pending deferral should be set by 'd' key press - check InputHandler defer logic",
        );
        assert_eq!(
            deferral.directory_id, test_dir.id,
            "Directory ID should match"
        );
        assert_eq!(deferral.path, "/test/defer", "Path should match");
        assert_eq!(deferral.input, "", "Input buffer should be empty initially");
    }

    #[test]
    fn pressing_d_with_no_directory_does_nothing() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.current_directory_id.set(None);

        // Press 'd' with no directory selected
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('d')),
        );

        // Should not set pending deferral
        assert!(
            app.pending_deferral.is_none(),
            "Pending deferral should not be set with no directory"
        );
    }

    #[test]
    fn deferral_input_accumulates_digits() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.pending_deferral = Some(PendingDeferral {
            directory_id: 42,
            path: "/test/path".to_string(),
            input: String::new(),
            default_days: 90,
        });

        // Type '3', then '0'
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('3')),
        );
        assert!(app.pending_deferral.is_some());
        let deferral = app.pending_deferral.as_ref().expect(
            "pending deferral should be set by 'd' key press - check InputHandler defer logic",
        );
        assert_eq!(deferral.input, "3", "Input should be '3'");

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('0')),
        );
        let deferral = app.pending_deferral.as_ref().expect(
            "pending deferral should be set by 'd' key press - check InputHandler defer logic",
        );
        assert_eq!(deferral.input, "30", "Input should be '30'");
    }

    #[test]
    fn deferral_input_backspace_removes_digits() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.pending_deferral = Some(PendingDeferral {
            directory_id: 42,
            path: "/test/path".to_string(),
            input: "123".to_string(),
            default_days: 90,
        });

        // Press backspace
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Backspace),
        );
        assert!(app.pending_deferral.is_some());
        let deferral = app.pending_deferral.as_ref().expect(
            "pending deferral should be set by 'd' key press - check InputHandler defer logic",
        );
        assert_eq!(deferral.input, "12", "Input should be '12' after backspace");

        // Press backspace again
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Backspace),
        );
        let deferral = app.pending_deferral.as_ref().expect(
            "pending deferral should be set by 'd' key press - check InputHandler defer logic",
        );
        assert_eq!(
            deferral.input, "1",
            "Input should be '1' after second backspace"
        );
    }

    #[test]
    fn deferral_enter_with_valid_input_defers_directory() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/defer", 1024, 5, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        // Simulate deferral state with input "30"
        app.pending_deferral = Some(PendingDeferral {
            directory_id: test_dir.id,
            path: "/test/defer".to_string(),
            input: "30".to_string(),
            default_days: 90,
        });

        // Press Enter to confirm
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Enter),
        );

        // Should clear pending deferral
        assert!(
            app.pending_deferral.is_none(),
            "Pending deferral should be cleared after confirmation"
        );

        // Should update directory status to 'deferred'
        let updated_dir = db
            .get_directory_by_path("/test/defer")
            .expect("Failed to get directory")
            .expect("Directory should exist");
        assert_eq!(
            updated_dir.status, "deferred",
            "Directory status should be 'deferred'"
        );

        // Should set deferred_until timestamp (approximately 30 days from now)
        assert!(
            updated_dir.deferred_until.is_some(),
            "deferred_until should be set"
        );
        let deferred_until = updated_dir.deferred_until.expect("deferred_until should be set after deferral confirmation - check database update logic");
        let expected = now + (30 * 86400);
        // Allow 5 seconds of test execution time
        assert!(
            (deferred_until - expected).abs() <= 5,
            "deferred_until should be approximately 30 days from now"
        );
    }

    #[test]
    fn deferral_enter_with_empty_input_uses_default() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/default", 512, 2, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        // Simulate deferral state with empty input
        app.pending_deferral = Some(PendingDeferral {
            directory_id: test_dir.id,
            path: "/test/default".to_string(),
            input: String::new(),
            default_days: 90,
        });

        // Press Enter to confirm (should use default of 90 days)
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Enter),
        );

        // Should clear pending deferral
        assert!(
            app.pending_deferral.is_none(),
            "Pending deferral should be cleared"
        );

        // Should update directory status to 'deferred'
        let updated_dir = db
            .get_directory_by_path("/test/default")
            .expect("Failed to get directory")
            .expect("Directory should exist");
        assert_eq!(updated_dir.status, "deferred");

        // Should set deferred_until to approximately 90 days from now (default)
        let deferred_until = updated_dir.deferred_until.expect("deferred_until should be set after deferral confirmation - check database update logic");
        let expected = now + (90 * 86400);
        assert!(
            (deferred_until - expected).abs() <= 5,
            "deferred_until should use default of 90 days"
        );
    }

    #[test]
    fn deferral_enter_with_zero_clears_without_change() {
        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/zero", 256, 1, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        // Simulate deferral state with "0" input
        app.pending_deferral = Some(PendingDeferral {
            directory_id: test_dir.id,
            path: "/test/zero".to_string(),
            input: "0".to_string(),
            default_days: 90,
        });

        // Press Enter (should reject zero and clear)
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Enter),
        );

        // Should clear pending deferral
        assert!(
            app.pending_deferral.is_none(),
            "Pending deferral should be cleared for invalid input"
        );

        // Should NOT update directory status (should remain 'tracked')
        let unchanged_dir = db
            .get_directory_by_path("/test/zero")
            .expect("Failed to get directory")
            .expect("Directory should exist");
        assert_eq!(
            unchanged_dir.status, "tracked",
            "Directory status should remain 'tracked' for invalid input"
        );
    }

    #[test]
    fn deferral_esc_cancels_without_change() {
        let (db, _dir) = temp_database();

        let mut app = App::new();
        app.pending_deferral = Some(PendingDeferral {
            directory_id: 42,
            path: "/test/cancel".to_string(),
            input: "15".to_string(),
            default_days: 90,
        });

        // Press Esc to cancel
        InputHandler::handle(&mut app, &test_config(), &db, make_key_event(KeyCode::Esc));

        // Should clear pending deferral
        assert!(
            app.pending_deferral.is_none(),
            "Pending deferral should be cleared after Esc"
        );
    }

    #[test]
    fn deferral_creates_audit_entry() {
        use crate::audit::AuditService;

        let (db, _dir) = temp_database();

        // Insert a test directory
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/audit_defer", 2048, 4, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];

        let mut app = App::new();
        app.pending_deferral = Some(PendingDeferral {
            directory_id: test_dir.id,
            path: "/test/audit_defer".to_string(),
            input: "45".to_string(),
            default_days: 90,
        });

        // Press Enter to defer
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Enter),
        );

        // Check audit log
        let audit = AuditService::new(&db);
        let entries = audit.list_recent(10).expect("Failed to list audit entries");

        assert!(!entries.is_empty(), "Should have at least one audit entry");

        // Find the defer entry
        let defer_entry = entries.iter().find(|e| {
            e.action == "defer" && e.target_path == Some("/test/audit_defer".to_string())
        });

        assert!(
            defer_entry.is_some(),
            "Should have a 'defer' audit entry for the path"
        );

        // Check that details contains "45 days"
        let entry = defer_entry
            .expect("audit entry should exist after defer action - check AuditService recording");
        assert!(
            entry
                .details
                .as_ref()
                .expect("audit entry should have details field - check AuditService recording")
                .contains("45 days"),
            "Audit entry should contain deferral period"
        );
    }

    #[test]
    fn deferral_ignores_non_digit_keys() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.pending_deferral = Some(PendingDeferral {
            directory_id: 42,
            path: "/test/path".to_string(),
            input: "12".to_string(),
            default_days: 90,
        });

        // Press 'a' (non-digit)
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('a')),
        );

        // Input should remain unchanged
        assert!(app.pending_deferral.is_some());
        let deferral = app.pending_deferral.as_ref().expect(
            "pending deferral should be set by 'd' key press - check InputHandler defer logic",
        );
        assert_eq!(
            deferral.input, "12",
            "Input should remain '12' (non-digit ignored)"
        );
    }

    // Tests for pending approvals view (US-020)

    #[test]
    fn pending_approvals_view_supports_navigation() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::PendingApprovals;
        app.list_len.set(10);

        // Test j/k navigation
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('j')),
        );
        assert_eq!(app.selected_index, 1, "j should increment index");

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('k')),
        );
        assert_eq!(app.selected_index, 0, "k should decrement index");

        // Test g/G navigation
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('G')),
        );
        assert_eq!(app.selected_index, 9, "G should go to bottom");

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('g')),
        );
        assert_eq!(app.selected_index, 0, "g should go to top");
    }

    #[test]
    fn pending_approvals_view_exits_on_q_and_esc() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::PendingApprovals;

        // Test q key
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('q')),
        );
        assert_eq!(
            app.view,
            View::DirectoryList,
            "q should return to directory list"
        );
        assert_eq!(app.selected_index, 0, "Selection should reset");

        // Reset for Esc test
        app.view = View::PendingApprovals;
        app.selected_index = 5;

        InputHandler::handle(&mut app, &test_config(), &db, make_key_event(KeyCode::Esc));
        assert_eq!(
            app.view,
            View::DirectoryList,
            "Esc should return to directory list"
        );
        assert_eq!(app.selected_index, 0, "Selection should reset");
    }

    #[test]
    fn pending_approvals_view_supports_sort_cycling() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::PendingApprovals;
        assert_eq!(app.sort_mode, SortMode::Expiration);

        // Press 's' to cycle sort modes
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('s')),
        );
        assert_eq!(
            app.sort_mode,
            SortMode::Size,
            "Sort mode should cycle to Size"
        );

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('s')),
        );
        assert_eq!(
            app.sort_mode,
            SortMode::Name,
            "Sort mode should cycle to Name"
        );

        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('s')),
        );
        assert_eq!(
            app.sort_mode,
            SortMode::Expiration,
            "Sort mode should cycle back to Expiration"
        );
    }

    #[test]
    fn pending_approvals_view_x_key_initiates_approval() {
        let (db, _dir) = temp_database();

        // Insert a test directory with status='pending'
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/pending", 1024, 2, Some(now), now)
            .expect("Failed to insert directory");
        // Update status to pending
        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];
        db.update_directory_status(test_dir.id, "pending")
            .expect("Failed to update status");

        let mut app = App::new();
        app.view = View::PendingApprovals;
        app.current_directory_id.set(Some(test_dir.id));

        // Press 'x' to initiate approval
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('x')),
        );

        // Should have pending approval set
        assert!(
            app.pending_approval.is_some(),
            "Pending approval should be set"
        );
        let (dir_id, path) = app.pending_approval.as_ref().expect(
            "pending approval should be set by 'x' key press - check InputHandler approval logic",
        );
        assert_eq!(*dir_id, test_dir.id, "Directory ID should match");
        assert_eq!(path, "/test/pending", "Path should match");
    }

    #[test]
    fn pending_approvals_view_d_key_initiates_deferral() {
        let (db, _dir) = temp_database();

        // Insert a test directory with status='pending'
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/defer_pending", 2048, 3, Some(now), now)
            .expect("Failed to insert directory");
        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];
        db.update_directory_status(test_dir.id, "pending")
            .expect("Failed to update status");

        let mut app = App::new();
        app.view = View::PendingApprovals;
        app.current_directory_id.set(Some(test_dir.id));

        // Press 'd' to initiate deferral
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('d')),
        );

        // Should have pending deferral set
        assert!(
            app.pending_deferral.is_some(),
            "Pending deferral should be set"
        );
        let deferral = app.pending_deferral.as_ref().expect(
            "pending deferral should be set by 'd' key press - check InputHandler defer logic",
        );
        assert_eq!(
            deferral.directory_id, test_dir.id,
            "Directory ID should match"
        );
        assert_eq!(deferral.path, "/test/defer_pending", "Path should match");
        assert_eq!(deferral.input, "", "Input buffer should be empty initially");
    }

    #[test]
    fn pending_approvals_view_i_key_initiates_ignore() {
        let (db, _dir) = temp_database();

        // Insert a test directory with status='pending'
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/ignore_pending", 512, 1, Some(now), now)
            .expect("Failed to insert directory");
        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];
        db.update_directory_status(test_dir.id, "pending")
            .expect("Failed to update status");

        let mut app = App::new();
        app.view = View::PendingApprovals;
        app.current_directory_id.set(Some(test_dir.id));

        // Press 'i' to initiate ignore
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('i')),
        );

        // Should have pending ignore set
        assert!(app.pending_ignore.is_some(), "Pending ignore should be set");
        let (dir_id, path) = app.pending_ignore.as_ref().expect(
            "pending ignore should be set by 'i' key press - check InputHandler ignore logic",
        );
        assert_eq!(*dir_id, test_dir.id, "Directory ID should match");
        assert_eq!(path, "/test/ignore_pending", "Path should match");
    }

    #[test]
    fn pending_approvals_view_actions_only_work_on_pending_directories() {
        let (db, _dir) = temp_database();

        // Insert a test directory with status='tracked' (not pending)
        let now = jiff::Timestamp::now().as_second();
        db.insert_or_update_directory("/test/tracked", 1024, 2, Some(now), now)
            .expect("Failed to insert directory");

        let directories = db
            .list_directories(None)
            .expect("Failed to list directories");
        let test_dir = &directories[0];
        // Status is 'tracked' by default

        let mut app = App::new();
        app.view = View::PendingApprovals;
        app.current_directory_id.set(Some(test_dir.id));

        // Press 'x' - should not set pending approval (directory is not pending)
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('x')),
        );
        assert!(
            app.pending_approval.is_none(),
            "Pending approval should not be set for non-pending directory"
        );

        // Press 'd' - should not set pending deferral
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('d')),
        );
        assert!(
            app.pending_deferral.is_none(),
            "Pending deferral should not be set for non-pending directory"
        );

        // Press 'i' - should not set pending ignore
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('i')),
        );
        assert!(
            app.pending_ignore.is_none(),
            "Pending ignore should not be set for non-pending directory"
        );
    }

    #[test]
    fn audit_log_view_exits_on_q() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::AuditLog;

        // Press 'q' to exit
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('q')),
        );

        assert_eq!(
            app.view,
            View::DirectoryList,
            "Should return to DirectoryList view"
        );
    }

    #[test]
    fn audit_log_view_exits_on_esc() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::AuditLog;

        // Press Esc to exit
        InputHandler::handle(&mut app, &test_config(), &db, make_key_event(KeyCode::Esc));

        assert_eq!(
            app.view,
            View::DirectoryList,
            "Should return to DirectoryList view"
        );
    }

    #[test]
    fn audit_log_view_exits_on_h() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::AuditLog;

        // Press 'h' to go back
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('h')),
        );

        assert_eq!(
            app.view,
            View::DirectoryList,
            "Should return to DirectoryList view"
        );
    }

    #[test]
    fn audit_log_view_navigation_j_k_works() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::AuditLog;
        app.selected_index = 0;

        // Press 'j' to move down
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('j')),
        );
        assert_eq!(app.selected_index, 1, "j should move down");

        // Press 'k' to move up
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('k')),
        );
        assert_eq!(app.selected_index, 0, "k should move up");
    }

    #[test]
    fn audit_log_view_navigation_g_capital_g_works() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        app.view = View::AuditLog;
        app.selected_index = 5;
        app.list_len.set(10);

        // Press 'g' to go to top
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('g')),
        );
        assert_eq!(app.selected_index, 0, "g should go to top");

        // Press 'G' to go to bottom
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('G')),
        );
        assert_eq!(app.selected_index, 9, "G should go to bottom (last item)");
    }

    #[test]
    fn audit_log_view_full_navigation_flow() {
        let (db, _dir) = temp_database();
        let mut app = App::new();

        // Start in directory list
        assert_eq!(app.view, View::DirectoryList);

        // Press 'a' to enter audit log view
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('a')),
        );
        assert_eq!(app.view, View::AuditLog, "Should switch to AuditLog view");

        // Navigate within audit log
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('j')),
        );
        assert_eq!(app.selected_index, 1);

        // Exit audit log view
        InputHandler::handle(
            &mut app,
            &test_config(),
            &db,
            make_key_event(KeyCode::Char('q')),
        );
        assert_eq!(
            app.view,
            View::DirectoryList,
            "Should return to DirectoryList view"
        );
    }
}
