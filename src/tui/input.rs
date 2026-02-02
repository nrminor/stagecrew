//! Vim-style keybinding handling.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::{App, FocusPanel, SortMode, View};
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
            View::FileList => Self::handle_file_list(app, config, db, key),
            View::AuditLog => Self::handle_audit_log(app, key),
            View::Help => Self::handle_help(app, key),
        }
    }

    fn handle_file_list(app: &mut App, _config: &Config, _db: &Database, key: KeyEvent) {
        match key.code {
            // Quit
            KeyCode::Char('q') => app.should_quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.should_quit = true;
            }

            // Switch focus between sidebar and main panel
            KeyCode::Tab | KeyCode::Char('\t') => {
                app.focus_panel = match app.focus_panel {
                    FocusPanel::Sidebar => FocusPanel::MainPanel,
                    FocusPanel::MainPanel => FocusPanel::Sidebar,
                };
            }
            KeyCode::Char('h') => {
                app.focus_panel = FocusPanel::Sidebar;
            }
            KeyCode::Char('l') => {
                app.focus_panel = FocusPanel::MainPanel;
            }

            // Navigation (vim-style) - operates on focused panel
            KeyCode::Char('j') | KeyCode::Down => match app.focus_panel {
                FocusPanel::Sidebar => {
                    app.sidebar_selected_index = app.sidebar_selected_index.saturating_add(1);
                }
                FocusPanel::MainPanel => {
                    app.file_selected_index = app.file_selected_index.saturating_add(1);
                }
            },
            KeyCode::Char('k') | KeyCode::Up => match app.focus_panel {
                FocusPanel::Sidebar => {
                    app.sidebar_selected_index = app.sidebar_selected_index.saturating_sub(1);
                }
                FocusPanel::MainPanel => {
                    app.file_selected_index = app.file_selected_index.saturating_sub(1);
                }
            },
            KeyCode::Char('g') => {
                // Go to top of focused panel
                match app.focus_panel {
                    FocusPanel::Sidebar => app.sidebar_selected_index = 0,
                    FocusPanel::MainPanel => app.file_selected_index = 0,
                }
            }
            KeyCode::Char('G') => {
                // Go to bottom of focused panel
                match app.focus_panel {
                    FocusPanel::Sidebar => {
                        app.select_last_sidebar(app.sidebar_len.get());
                    }
                    FocusPanel::MainPanel => {
                        app.select_last_file(app.file_list_len.get());
                    }
                }
            }

            // Sort modes (applies to file list)
            KeyCode::Char('s') => {
                app.sort_mode = match app.sort_mode {
                    SortMode::Expiration => SortMode::Size,
                    SortMode::Size => SortMode::Name,
                    SortMode::Name => SortMode::Modified,
                    SortMode::Modified => SortMode::Expiration,
                };
            }

            // Views
            KeyCode::Char('a') => app.view = View::AuditLog,
            KeyCode::Char('?') => app.view = View::Help,

            _ => {}
        }
    }

    /// Handle confirmation prompt (y/n/Esc) for ignore action.
    // Allow: Will be used for file-level actions in US-030.
    #[allow(dead_code)]
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
    // Allow: Will be used for file-level actions in US-030.
    #[allow(dead_code)]
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
    // Allow: Will be used for file-level actions in US-030.
    #[allow(dead_code)]
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

    fn handle_audit_log(app: &mut App, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q' | 'h') | KeyCode::Esc => {
                app.view = View::FileList;
            }
            KeyCode::Char('j') | KeyCode::Down => {
                app.sidebar_selected_index = app.sidebar_selected_index.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                app.sidebar_selected_index = app.sidebar_selected_index.saturating_sub(1);
            }
            KeyCode::Char('g') => app.sidebar_selected_index = 0, // Go to top
            KeyCode::Char('G') => app.select_last_sidebar(app.sidebar_len.get()), // Go to bottom
            _ => {}
        }
    }

    fn handle_help(app: &mut App, key: KeyEvent) {
        // Any key closes the help view
        let _ = key;
        app.view = View::FileList;
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

    // ===== Navigation and Focus Tests =====

    #[test]
    fn tab_switches_focus_between_panels() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Start with sidebar focused
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);

        // Tab to main panel
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Tab));
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);

        // Tab back to sidebar
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Tab));
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
    }

    #[test]
    fn h_focuses_sidebar() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.focus_panel = FocusPanel::MainPanel;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('h')));
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
    }

    #[test]
    fn l_focuses_main_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.focus_panel = FocusPanel::Sidebar;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('l')));
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
    }

    #[test]
    fn j_navigates_down_in_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Navigate down in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('j')));
        assert_eq!(app.sidebar_selected_index, 1);
        assert_eq!(app.file_selected_index, 0, "File index should not change");

        // Navigate down in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('j')));
        assert_eq!(app.file_selected_index, 1);
        assert_eq!(
            app.sidebar_selected_index, 1,
            "Sidebar index should not change"
        );
    }

    #[test]
    fn k_navigates_up_in_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Navigate up in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 5;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('k')));
        assert_eq!(app.sidebar_selected_index, 4);

        // Navigate up in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 5;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('k')));
        assert_eq!(app.file_selected_index, 4);
    }

    #[test]
    fn g_goes_to_top_of_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Go to top in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 10;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('g')));
        assert_eq!(app.sidebar_selected_index, 0);

        // Go to top in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 10;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('g')));
        assert_eq!(app.file_selected_index, 0);
    }

    #[test]
    fn capital_g_goes_to_bottom_of_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Simulate list lengths (normally set by render)
        app.sidebar_len.set(10);
        app.file_list_len.set(20);

        // Go to bottom in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('G')));
        assert_eq!(app.sidebar_selected_index, 9); // len - 1

        // Go to bottom in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.file_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('G')));
        assert_eq!(app.file_selected_index, 19); // len - 1
    }

    // ===== Sort Mode Tests =====

    #[test]
    fn s_cycles_sort_modes() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert_eq!(app.sort_mode, SortMode::Expiration);

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Size);

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Name);

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Modified);

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Expiration);
    }

    // ===== View Switching Tests =====

    #[test]
    fn a_switches_to_audit_log_view() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert_eq!(app.view, View::FileList);
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('a')));
        assert_eq!(app.view, View::AuditLog);
    }

    #[test]
    fn question_mark_switches_to_help_view() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert_eq!(app.view, View::FileList);
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('?')));
        assert_eq!(app.view, View::Help);
    }

    #[test]
    fn help_view_closes_on_any_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.view = View::Help;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('x')));
        assert_eq!(app.view, View::FileList);
    }

    #[test]
    fn audit_log_view_returns_to_file_list_on_q() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.view = View::AuditLog;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('q')));
        assert_eq!(app.view, View::FileList);
    }

    // ===== Quit Tests =====

    #[test]
    fn q_quits_application() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert!(!app.should_quit);
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('q')));
        assert!(app.should_quit);
    }

    #[test]
    fn ctrl_c_quits_application() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        assert!(!app.should_quit);
        InputHandler::handle(
            &mut app,
            &config,
            &db,
            make_key_event_with_mods(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert!(app.should_quit);
    }
}
