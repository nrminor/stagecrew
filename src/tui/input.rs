//! Vim-style keybinding handling.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::ui::sort_entry_rows;
use super::{App, FocusPanel, SortMode, View};
use crate::audit::{AuditAction, AuditService};
use crate::config::Config;
use crate::db::Database;
use crate::scanner::calculate_expiration;

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

    // Allow: This function coordinates input handling for the file list view, including
    // modal dispatch, focus management, navigation, actions, and view switching. Breaking
    // it into smaller functions would obscure the input handling flow.
    #[allow(clippy::too_many_lines)]
    fn handle_file_list(app: &mut App, config: &Config, db: &Database, key: KeyEvent) {
        // Check for pending confirmations/inputs first
        if app.pending_add_path.is_some() {
            Self::handle_add_path_input(app, config, db, key);
            return;
        }
        if app.pending_remove_path.is_some() {
            Self::handle_remove_path_confirmation(app, db, key);
            return;
        }
        if app.pending_entry_delete.is_some() {
            Self::handle_entry_delete_confirmation(app, db, key);
            return;
        }
        if app.pending_entry_deferral.is_some() {
            Self::handle_entry_deferral_input(app, config, db, key);
            return;
        }
        if app.pending_entry_ignore.is_some() {
            Self::handle_entry_ignore_confirmation(app, db, key);
            return;
        }
        if app.pending_entry_approval.is_some() {
            Self::handle_entry_approval_confirmation(app, db, key);
            return;
        }

        match key.code {
            // Quit
            KeyCode::Char('q') => app.should_quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.should_quit = true;
            }

            // Toggle sidebar visibility
            KeyCode::Char('B') => {
                app.sidebar_visible = !app.sidebar_visible;
                // If hiding sidebar while it's focused, switch to main panel
                if !app.sidebar_visible && app.focus_panel == FocusPanel::Sidebar {
                    app.focus_panel = FocusPanel::MainPanel;
                }
            }

            // Switch focus between sidebar and main panel
            KeyCode::Tab | KeyCode::Char('\t') => {
                if app.sidebar_visible {
                    app.focus_panel = match app.focus_panel {
                        FocusPanel::Sidebar => FocusPanel::MainPanel,
                        FocusPanel::MainPanel => FocusPanel::Sidebar,
                    };
                } else {
                    // Show sidebar and focus it
                    app.sidebar_visible = true;
                    app.focus_panel = FocusPanel::Sidebar;
                }
            }
            KeyCode::Char('h') => {
                Self::handle_h_navigation(app, db);
            }
            KeyCode::Char('l') => {
                Self::handle_l_navigation(app, config, db);
            }

            // Navigation (vim-style) - operates on focused panel
            KeyCode::Char('j') | KeyCode::Down => match app.focus_panel {
                FocusPanel::Sidebar => {
                    app.sidebar_selected_index = app.sidebar_selected_index.saturating_add(1);
                }
                FocusPanel::MainPanel => {
                    app.entry_selected_index = app.entry_selected_index.saturating_add(1);
                }
            },
            KeyCode::Char('k') | KeyCode::Up => match app.focus_panel {
                FocusPanel::Sidebar => {
                    app.sidebar_selected_index = app.sidebar_selected_index.saturating_sub(1);
                }
                FocusPanel::MainPanel => {
                    app.entry_selected_index = app.entry_selected_index.saturating_sub(1);
                }
            },
            KeyCode::Char('g') => {
                // Go to top of focused panel
                match app.focus_panel {
                    FocusPanel::Sidebar => app.sidebar_selected_index = 0,
                    FocusPanel::MainPanel => app.entry_selected_index = 0,
                }
            }
            KeyCode::Char('G') => {
                // Go to bottom of focused panel
                match app.focus_panel {
                    FocusPanel::Sidebar => {
                        app.select_last_sidebar(app.sidebar_len.get());
                    }
                    FocusPanel::MainPanel => {
                        app.select_last_entry(app.entry_list_len.get());
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

            // Selection mode (only when main panel is focused)
            KeyCode::Char(' ') if app.focus_panel == FocusPanel::MainPanel => {
                Self::toggle_entry_selection(app, config, db);
            }
            KeyCode::Char('v') if app.focus_panel == FocusPanel::MainPanel => {
                Self::enter_visual_mode(app, db);
            }
            KeyCode::Esc if app.focus_panel == FocusPanel::MainPanel => {
                app.clear_selection();
            }

            // Entry-level actions (only when main panel is focused)
            KeyCode::Char('d') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_entry_delete(app, config, db);
            }
            KeyCode::Char('r') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_entry_defer(app, config, db);
            }
            KeyCode::Char('i') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_entry_ignore(app, config, db);
            }
            KeyCode::Char('x') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_entry_approve(app, config, db);
            }
            KeyCode::Char('u') if app.focus_panel == FocusPanel::MainPanel => {
                Self::unignore_entry(app, config, db);
            }

            // Views
            KeyCode::Char('a') => app.view = View::AuditLog,
            KeyCode::Char('?') => app.view = View::Help,

            // Refresh/rescan (R = rescan tracked paths)
            KeyCode::Char('R') => {
                if !app.scan_in_progress {
                    app.scan_requested = true;
                }
            }

            // Enter a root from the sidebar
            KeyCode::Enter if app.focus_panel == FocusPanel::Sidebar => {
                if let Ok(roots) = db.list_roots() {
                    let idx = app
                        .sidebar_selected_index
                        .min(roots.len().saturating_sub(1));
                    if let Some(root) = roots.get(idx) {
                        app.navigate_into(root.path.clone());
                        app.focus_panel = FocusPanel::MainPanel;
                    }
                }
            }

            // Return to sidebar from main panel at root level
            KeyCode::Backspace if app.focus_panel == FocusPanel::MainPanel => {
                if let Ok(roots) = db.list_roots() {
                    let at_root_level = roots.iter().any(|r| r.path == app.current_path);
                    if at_root_level {
                        app.focus_panel = FocusPanel::Sidebar;
                    }
                }
            }

            // Path management (A = add, X = remove from sidebar)
            KeyCode::Char('A') => {
                // Initiate add path modal
                app.pending_add_path = Some(String::new());
            }
            KeyCode::Char('X') if app.focus_panel == FocusPanel::Sidebar => {
                Self::initiate_remove_path(app, config, db);
            }

            _ => {}
        }
    }

    /// Toggle selection of the currently focused file.
    fn toggle_entry_selection(app: &mut App, config: &Config, db: &Database) {
        // Get entries for current path
        if app.current_path.is_empty() {
            tracing::warn!("Cannot toggle selection: no path selected");
            return;
        }

        // Query entries for current browsing path
        let entries = match db.list_entries_by_parent(app.current_path()) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Failed to query entries: {}", e);
                return;
            }
        };

        // Sort entries the same way the UI does so indices match
        let mut entry_rows: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                let days_remaining = entry.mtime.map_or(i64::MAX, |m| {
                    calculate_expiration(m, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Get the selected entry based on entry_selected_index
        let Some((entry, _)) = entry_rows.get(app.entry_selected_index) else {
            tracing::warn!("No entry selected (index out of bounds)");
            return;
        };

        // Toggle selection and advance cursor for hold-to-multi-select behavior
        app.toggle_entry_selection(entry.id);
        app.entry_selected_index = app.entry_selected_index.saturating_add(1);
    }

    /// Enter visual mode by selecting all visible entries in the current directory.
    fn enter_visual_mode(app: &mut App, db: &Database) {
        // Get entries for current path
        if app.current_path.is_empty() {
            tracing::warn!("Cannot enter visual mode: no path selected");
            return;
        }

        // Query entries for current browsing path
        let entries = match db.list_entries_by_parent(app.current_path()) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Failed to query entries: {}", e);
                return;
            }
        };

        // Select all entry IDs
        for entry in entries {
            app.selected_entries.insert(entry.id);
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

    /// Handle `h` key: ranger-style navigate up or return to sidebar.
    ///
    /// - Main panel, inside a subdirectory: navigate up to parent
    /// - Main panel, at root level: return focus to sidebar
    /// - Sidebar: no-op
    fn handle_h_navigation(app: &mut App, db: &Database) {
        match app.focus_panel {
            FocusPanel::Sidebar => {}
            FocusPanel::MainPanel => {
                if let Ok(roots) = db.list_roots() {
                    let at_root_level = roots.iter().any(|r| r.path == app.current_path);
                    if at_root_level {
                        app.sidebar_visible = true;
                        app.focus_panel = FocusPanel::Sidebar;
                    } else {
                        app.navigate_up();
                    }
                }
            }
        }
    }

    /// Handle `l` key: ranger-style navigate into directory or enter root.
    ///
    /// - Sidebar: enter the selected root (same as Enter)
    /// - Main panel, cursor on a directory: navigate into it
    /// - Main panel, cursor on a file: no-op
    fn handle_l_navigation(app: &mut App, config: &Config, db: &Database) {
        match app.focus_panel {
            FocusPanel::Sidebar => {
                // Enter the selected root (same behavior as Enter)
                if let Ok(roots) = db.list_roots() {
                    let idx = app
                        .sidebar_selected_index
                        .min(roots.len().saturating_sub(1));
                    if let Some(root) = roots.get(idx) {
                        app.navigate_into(root.path.clone());
                        app.focus_panel = FocusPanel::MainPanel;
                    }
                }
            }
            FocusPanel::MainPanel => {
                if app.current_path.is_empty() {
                    return;
                }

                // Look up the entry under the cursor
                let Ok(entries) = db.list_entries_by_parent(app.current_path()) else {
                    return;
                };

                let mut entry_rows: Vec<_> = entries
                    .into_iter()
                    .map(|entry| {
                        let days_remaining = entry.mtime.map_or(i64::MAX, |m| {
                            calculate_expiration(m, config.expiration_days)
                        });
                        (entry, days_remaining)
                    })
                    .collect();
                sort_entry_rows(&mut entry_rows, app.sort_mode());

                if let Some((entry, _)) = entry_rows.get(app.entry_selected_index)
                    && entry.is_dir
                {
                    app.navigate_into(entry.path.clone());
                }
            }
        }
    }

    /// Initiate entry deletion by querying the database for the selected entry/entries.
    ///
    /// If entries are selected via multi-select, delete all selected entries.
    /// Otherwise, delete the currently focused entry.
    fn initiate_entry_delete(app: &mut App, config: &Config, db: &Database) {
        // Get entries for current path
        if app.current_path.is_empty() {
            tracing::warn!("Cannot delete entry: no path selected");
            return;
        }

        // Query entries for current browsing path
        let entries = match db.list_entries_by_parent(app.current_path()) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Failed to query entries: {}", e);
                return;
            }
        };

        // Sort entries the same way the UI does so indices match
        let mut entry_rows: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                let days_remaining = entry.mtime.map_or(i64::MAX, |m| {
                    calculate_expiration(m, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Determine which entries to delete (id, path, is_dir)
        let entries_to_delete: Vec<(i64, String, bool)> = if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                vec![(entry.id, entry.path.clone(), entry.is_dir)]
            } else {
                tracing::warn!("No entry selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected entries (selection is by ID, so sorting doesn't matter here)
            entry_rows
                .into_iter()
                .filter(|(e, _)| app.selected_entries.contains(&e.id))
                .map(|(e, _)| (e.id, e.path.clone(), e.is_dir))
                .collect()
        };

        if entries_to_delete.is_empty() {
            tracing::warn!("No entries to delete");
            return;
        }

        // Set pending deletion state
        app.pending_entry_delete = Some(entries_to_delete);
    }

    /// Handle entry deletion confirmation (y/n/Esc).
    fn handle_entry_delete_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the deletion for all pending entries
                if let Some(entries) = &app.pending_entry_delete {
                    let audit = AuditService::new(db);
                    let user = AuditService::current_user();
                    let root_id = app.current_root_id.get();

                    let mut success_count = 0;
                    let mut fail_count = 0;
                    let total = entries.len();

                    for (entry_id, path, is_dir) in entries {
                        if let Err(e) = db.delete_entry(*entry_id, path, *is_dir) {
                            tracing::warn!("Failed to delete entry {}: {}", path, e);
                            app.status_message = Some(format!("Delete failed: {e}"));
                            app.status_message_time = Some(std::time::Instant::now());
                            fail_count += 1;
                        } else {
                            success_count += 1;
                            // Record audit entry
                            let detail = if *is_dir {
                                "Directory deleted by user"
                            } else {
                                "File deleted by user"
                            };
                            if let Err(e) = audit.record(
                                &user,
                                AuditAction::Remove,
                                Some(path),
                                Some(detail),
                                root_id,
                            ) {
                                tracing::warn!("Failed to record audit entry for deletion: {}", e);
                            }
                        }
                    }

                    // Show result in status bar
                    if fail_count == 0 && success_count > 0 {
                        app.status_message = Some(format!("Deleted {success_count} file(s)"));
                        app.status_message_time = Some(std::time::Instant::now());
                    } else if fail_count > 0 && success_count > 0 {
                        app.status_message = Some(format!(
                            "Deleted {success_count}/{total}, {fail_count} failed"
                        ));
                        app.status_message_time = Some(std::time::Instant::now());
                    }
                    // If all failed, the last error message is already set
                } else {
                    app.status_message = Some("No files pending delete".to_string());
                    app.status_message_time = Some(std::time::Instant::now());
                }
                // Clear pending deletion and selection
                app.pending_entry_delete = None;
                app.clear_selection();
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel deletion
                app.pending_entry_delete = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Initiate entry deferral by setting up the input state.
    ///
    /// If entries are selected via multi-select, defer all selected entries with the same number of days.
    /// Otherwise, defer the currently focused entry.
    fn initiate_entry_defer(app: &mut App, config: &Config, db: &Database) {
        // Get entries for current path
        if app.current_path.is_empty() {
            tracing::warn!("Cannot defer entry: no path selected");
            return;
        }

        // Query entries for current browsing path
        let entries = match db.list_entries_by_parent(app.current_path()) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Failed to query entries: {}", e);
                return;
            }
        };

        // Sort entries the same way the UI does so indices match
        let mut entry_rows: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                let days_remaining = entry.mtime.map_or(i64::MAX, |m| {
                    calculate_expiration(m, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Determine which entries to defer
        let entries_to_defer: Vec<i64> = if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                vec![entry.id]
            } else {
                tracing::warn!("No entry selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected entries (selection is by ID, so sorting doesn't matter here)
            entry_rows
                .iter()
                .filter(|(e, _)| app.selected_entries.contains(&e.id))
                .map(|(e, _)| e.id)
                .collect()
        };

        if entries_to_defer.is_empty() {
            tracing::warn!("No entries to defer");
            return;
        }

        // Get first entry path for display in modal
        let first_entry_path = entry_rows
            .iter()
            .find(|(e, _)| entries_to_defer.contains(&e.id))
            .map_or_else(|| "unknown".to_string(), |(e, _)| e.path.clone());

        // Set pending deferral state
        app.pending_entry_deferral = Some(super::PendingDeferral {
            entry_id: entries_to_defer[0],
            path: first_entry_path,
            input: String::new(),
            default_days: config.expiration_days,
            additional_entry_ids: entries_to_defer[1..].to_vec(),
        });
    }

    /// Handle entry deferral input (digits/backspace/enter/esc).
    fn handle_entry_deferral_input(app: &mut App, _config: &Config, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char(c) if c.is_ascii_digit() => {
                // Append digit to input buffer
                if let Some(ref mut deferral) = app.pending_entry_deferral {
                    deferral.input.push(c);
                }
            }
            KeyCode::Backspace => {
                // Remove last digit from input buffer
                if let Some(ref mut deferral) = app.pending_entry_deferral {
                    deferral.input.pop();
                }
            }
            KeyCode::Enter => {
                // User confirmed - process the deferral
                if let Some(deferral) = &app.pending_entry_deferral {
                    // Parse the input as days (or use default if empty)
                    let days: u32 = if deferral.input.is_empty() {
                        deferral.default_days
                    } else if let Ok(parsed_days) = deferral.input.parse::<u32>() {
                        if parsed_days == 0 {
                            tracing::warn!("Invalid deferral period: must be > 0");
                            app.pending_entry_deferral = None;
                            return;
                        }
                        parsed_days
                    } else {
                        tracing::warn!("Invalid deferral input: {}", deferral.input);
                        app.pending_entry_deferral = None;
                        return;
                    };

                    // Calculate deferred_until timestamp
                    let now = jiff::Timestamp::now();
                    let days_i64 = i64::from(days);
                    let deferred_until = now.as_second() + (days_i64 * 86400);

                    // Update entry status to 'deferred' and set deferred_until for all entries
                    let entry_ids_to_defer: Vec<i64> = std::iter::once(deferral.entry_id)
                        .chain(deferral.additional_entry_ids.iter().copied())
                        .collect();

                    let audit = AuditService::new(db);
                    let user = AuditService::current_user();
                    let details = Some(format!("Deferred for {days} days"));
                    let root_id = app.current_root_id.get();

                    for entry_id in entry_ids_to_defer {
                        if let Err(e) = db.defer_entry(entry_id, deferred_until) {
                            tracing::warn!("Failed to defer entry with id {}: {}", entry_id, e);
                        } else {
                            // Record audit entry (path is not available for all, using first entry's path)
                            if let Err(e) = audit.record(
                                &user,
                                AuditAction::Defer,
                                Some(&deferral.path),
                                details.as_deref(),
                                root_id,
                            ) {
                                tracing::warn!("Failed to record audit entry for deferral: {}", e);
                            }
                        }
                    }
                }
                // Clear pending deferral and selection
                app.pending_entry_deferral = None;
                app.clear_selection();
            }
            KeyCode::Esc => {
                // Cancel deferral input
                app.pending_entry_deferral = None;
            }
            _ => {
                // Ignore other keys during input
            }
        }
    }

    /// Initiate file ignore by setting up the confirmation state.
    ///
    /// If files are selected via multi-select, ignore all selected files.
    /// Otherwise, ignore the currently focused file.
    fn initiate_entry_ignore(app: &mut App, config: &Config, db: &Database) {
        // Get entries for current path
        if app.current_path.is_empty() {
            tracing::warn!("Cannot ignore entry: no path selected");
            return;
        }

        // Query entries for current browsing path
        let entries = match db.list_entries_by_parent(app.current_path()) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Failed to query entries: {}", e);
                return;
            }
        };

        // Sort entries the same way the UI does so indices match
        let mut entry_rows: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                let days_remaining = entry.mtime.map_or(i64::MAX, |m| {
                    calculate_expiration(m, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Determine which entries to ignore
        let entries_to_ignore: Vec<(i64, String)> = if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                vec![(entry.id, entry.path.clone())]
            } else {
                tracing::warn!("No entry selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected entries (selection is by ID, so sorting doesn't matter here)
            entry_rows
                .into_iter()
                .filter(|(e, _)| app.selected_entries.contains(&e.id))
                .map(|(e, _)| (e.id, e.path.clone()))
                .collect()
        };

        if entries_to_ignore.is_empty() {
            tracing::warn!("No entries to ignore");
            return;
        }

        // Set pending ignore state
        app.pending_entry_ignore = Some(entries_to_ignore);
    }

    /// Handle entry ignore confirmation (y/n/Esc).
    fn handle_entry_ignore_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the ignore for all pending entries
                if let Some(entries) = &app.pending_entry_ignore {
                    let audit = AuditService::new(db);
                    let user = AuditService::current_user();
                    let root_id = app.current_root_id.get();

                    for (entry_id, path) in entries {
                        if let Err(e) = db.update_entry_status(*entry_id, "ignored") {
                            tracing::warn!("Failed to ignore entry {}: {}", path, e);
                        } else {
                            // Record audit entry
                            if let Err(e) =
                                audit.record(&user, AuditAction::Ignore, Some(path), None, root_id)
                            {
                                tracing::warn!("Failed to record audit entry for ignore: {}", e);
                            }
                        }
                    }
                }
                // Clear pending ignore and selection
                app.pending_entry_ignore = None;
                app.clear_selection();
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel ignore
                app.pending_entry_ignore = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Initiate entry approval by setting up the confirmation state.
    ///
    /// If entries are selected via multi-select, approve all selected entries.
    /// Otherwise, approve the currently focused entry.
    fn initiate_entry_approve(app: &mut App, config: &Config, db: &Database) {
        // Get entries for current path
        if app.current_path.is_empty() {
            tracing::warn!("Cannot approve entry: no path selected");
            return;
        }

        // Query entries for current browsing path
        let entries = match db.list_entries_by_parent(app.current_path()) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Failed to query entries: {}", e);
                return;
            }
        };

        // Sort entries the same way the UI does so indices match
        let mut entry_rows: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                let days_remaining = entry.mtime.map_or(i64::MAX, |m| {
                    calculate_expiration(m, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Determine which entries to approve
        let entries_to_approve: Vec<(i64, String)> = if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                vec![(entry.id, entry.path.clone())]
            } else {
                tracing::warn!("No entry selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected entries (selection is by ID, so sorting doesn't matter here)
            entry_rows
                .into_iter()
                .filter(|(e, _)| app.selected_entries.contains(&e.id))
                .map(|(e, _)| (e.id, e.path.clone()))
                .collect()
        };

        if entries_to_approve.is_empty() {
            tracing::warn!("No entries to approve");
            return;
        }

        // Set pending approval state
        app.pending_entry_approval = Some(entries_to_approve);
    }

    /// Handle entry approval confirmation (y/n/Esc).
    fn handle_entry_approval_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the approval for all pending entries
                if let Some(entries) = &app.pending_entry_approval {
                    let audit = AuditService::new(db);
                    let user = AuditService::current_user();
                    let root_id = app.current_root_id.get();

                    for (entry_id, path) in entries {
                        if let Err(e) = db.update_entry_status(*entry_id, "approved") {
                            tracing::warn!("Failed to approve entry {}: {}", path, e);
                        } else {
                            // Record audit entry
                            if let Err(e) =
                                audit.record(&user, AuditAction::Approve, Some(path), None, root_id)
                            {
                                tracing::warn!("Failed to record audit entry for approval: {}", e);
                            }
                        }
                    }
                }
                // Clear pending approval and selection
                app.pending_entry_approval = None;
                app.clear_selection();
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel approval
                app.pending_entry_approval = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }

    /// Unignore an entry (reset status from "ignored" back to "tracked").
    ///
    /// This is a direct action without confirmation since it's non-destructive.
    /// Works on selected entries if any, otherwise the currently focused entry.
    fn unignore_entry(app: &mut App, config: &Config, db: &Database) {
        // Get entries for current path
        if app.current_path.is_empty() {
            tracing::warn!("Cannot unignore entry: no path selected");
            return;
        }

        // Query entries for current browsing path
        let entries = match db.list_entries_by_parent(app.current_path()) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Failed to query entries: {}", e);
                return;
            }
        };

        // Sort entries the same way the UI does so indices match
        let mut entry_rows: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                let days_remaining = entry.mtime.map_or(i64::MAX, |m| {
                    calculate_expiration(m, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Determine which entries to unignore
        let entries_to_unignore: Vec<(i64, String)> = if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                // Only unignore if currently ignored
                if entry.status == "ignored" {
                    vec![(entry.id, entry.path.clone())]
                } else {
                    app.status_message = Some("Entry is not ignored".to_string());
                    app.status_message_time = Some(std::time::Instant::now());
                    return;
                }
            } else {
                tracing::warn!("No entry selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected entries that are ignored
            entry_rows
                .into_iter()
                .filter(|(e, _)| app.selected_entries.contains(&e.id) && e.status == "ignored")
                .map(|(e, _)| (e.id, e.path.clone()))
                .collect()
        };

        if entries_to_unignore.is_empty() {
            app.status_message = Some("No ignored entries selected".to_string());
            app.status_message_time = Some(std::time::Instant::now());
            return;
        }

        // Perform the unignore
        let audit = AuditService::new(db);
        let user = AuditService::current_user();
        let root_id = app.current_root_id.get();

        let mut success_count = 0;
        for (entry_id, path) in &entries_to_unignore {
            if let Err(e) = db.update_entry_status(*entry_id, "tracked") {
                tracing::warn!("Failed to unignore entry {}: {}", path, e);
                app.status_message = Some(format!("Unignore failed: {e}"));
                app.status_message_time = Some(std::time::Instant::now());
            } else {
                success_count += 1;
                // Record audit entry
                if let Err(e) =
                    audit.record(&user, AuditAction::Unignore, Some(path), None, root_id)
                {
                    tracing::warn!("Failed to record audit entry for unignore: {}", e);
                }
            }
        }

        if success_count > 0 {
            app.status_message = Some(format!("Unignored {success_count} entry/entries"));
            app.status_message_time = Some(std::time::Instant::now());
        }
        app.clear_selection();
    }

    /// Handle add path text input (characters/backspace/enter/esc).
    fn handle_add_path_input(app: &mut App, config: &Config, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char(c) if !c.is_control() => {
                // Append character to input buffer
                if let Some(ref mut input) = app.pending_add_path {
                    input.push(c);
                }
            }
            KeyCode::Backspace => {
                // Remove last character from input buffer
                if let Some(ref mut input) = app.pending_add_path {
                    input.pop();
                }
            }
            KeyCode::Enter => {
                // User confirmed - add the path
                if let Some(input) = &app.pending_add_path {
                    if input.trim().is_empty() {
                        tracing::warn!("Cannot add empty path");
                        app.pending_add_path = None;
                        return;
                    }

                    // Expand tilde
                    let expanded_path = shellexpand::tilde(input.trim());
                    let path = std::path::PathBuf::from(expanded_path.as_ref());

                    // Validate path exists and is a directory
                    if !path.exists() {
                        tracing::warn!("Path does not exist: {}", path.display());
                        app.pending_add_path = None;
                        return;
                    }

                    if !path.is_dir() {
                        tracing::warn!("Path is not a directory: {}", path.display());
                        app.pending_add_path = None;
                        return;
                    }

                    // Canonicalize to prevent duplicates
                    let canonical_path = match path.canonicalize() {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!("Failed to canonicalize path {}: {}", path.display(), e);
                            app.pending_add_path = None;
                            return;
                        }
                    };

                    // Check if already tracked in config or database
                    if config.tracked_paths.contains(&canonical_path) {
                        tracing::warn!(
                            "Path already tracked (in config): {}",
                            canonical_path.display()
                        );
                        app.pending_add_path = None;
                        return;
                    }

                    let path_str = canonical_path.to_string_lossy();
                    if let Ok(roots) = db.list_roots()
                        && roots.iter().any(|r| r.path == path_str.as_ref())
                    {
                        tracing::warn!("Path already tracked: {}", canonical_path.display());
                        app.pending_add_path = None;
                        return;
                    }

                    // Insert as a root in the database
                    if let Err(e) = db.insert_root(&path_str) {
                        tracing::warn!("Failed to add root to database: {}", e);
                        app.pending_add_path = None;
                        return;
                    }

                    tracing::info!(
                        "Added tracked path: {} (will be scanned on next rescan)",
                        canonical_path.display()
                    );
                }

                // Clear pending add path
                app.pending_add_path = None;
            }
            KeyCode::Esc => {
                // Cancel add path input
                app.pending_add_path = None;
            }
            _ => {
                // Ignore other keys during input
            }
        }
    }

    /// Initiate path removal by querying the database for the selected sidebar root.
    ///
    /// Roots defined in `config.tracked_paths` cannot be removed from the TUI
    /// because they are re-seeded on every scan. The user must edit the config
    /// file directly to remove those.
    fn initiate_remove_path(app: &mut App, config: &Config, db: &Database) {
        let roots = match db.list_roots() {
            Ok(roots) => roots,
            Err(e) => {
                tracing::warn!("Failed to query roots: {}", e);
                return;
            }
        };

        if let Some(root) = roots.get(app.sidebar_selected_index) {
            let is_config_root = config
                .tracked_paths
                .iter()
                .any(|p| p.to_string_lossy() == root.path);

            if is_config_root {
                app.status_message =
                    Some("This root is defined in config.toml — remove it there".to_string());
                app.status_message_time = Some(std::time::Instant::now());
            } else {
                app.pending_remove_path = Some(root.path.clone());
            }
        } else {
            tracing::warn!("No root selected for removal");
        }
    }

    /// Handle remove path confirmation (y/n/Esc).
    fn handle_remove_path_confirmation(app: &mut App, db: &Database, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - remove the root from the database
                if let Some(path_to_remove) = &app.pending_remove_path {
                    match db.get_root_by_path(path_to_remove) {
                        Ok(Some(root)) => {
                            if let Err(e) = db.delete_root(root.id) {
                                tracing::warn!("Failed to remove root from database: {}", e);
                            } else {
                                tracing::info!("Removed tracked path: {}", path_to_remove);
                                // If we were browsing this root, clear the view
                                if app.current_path.starts_with(path_to_remove.as_str()) {
                                    app.current_path.clear();
                                    app.current_root_id.set(None);
                                    app.focus_panel = FocusPanel::Sidebar;
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::warn!("Root not found in database: {}", path_to_remove);
                        }
                        Err(e) => {
                            tracing::warn!("Failed to look up root: {}", e);
                        }
                    }
                }

                // Clear pending remove path
                app.pending_remove_path = None;
            }
            KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                // Cancel removal
                app.pending_remove_path = None;
            }
            _ => {
                // Ignore other keys during confirmation
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::db::Database;
    use crate::tui::PendingDeferral;
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

        // Start with main panel focused (default for immediate file interaction)
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);

        // Tab to sidebar
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Tab));
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);

        // Tab back to main panel
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Tab));
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
    }

    #[test]
    fn h_at_root_level_returns_to_sidebar() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        db.insert_root("/test/downloads")
            .expect("Failed to create test root");

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = "/test/downloads".to_string();
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('h')));
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
        assert!(app.sidebar_visible);
    }

    #[test]
    fn h_in_subdirectory_navigates_up() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        db.insert_root("/test/downloads")
            .expect("Failed to create test root");

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = "/test/downloads/subdir".to_string();
        app.entry_selected_index = 3;

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('h')));

        assert_eq!(app.current_path, "/test/downloads");
        assert_eq!(app.entry_selected_index, 0);
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
    }

    #[test]
    fn h_in_sidebar_is_noop() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 2;

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('h')));

        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
        assert_eq!(app.sidebar_selected_index, 2);
    }

    #[test]
    fn l_from_sidebar_enters_root() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        db.insert_root("/test/downloads")
            .expect("Failed to create test root");

        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('l')));

        assert_eq!(app.current_path, "/test/downloads");
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn l_on_directory_entry_navigates_into_it() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        let root_id = db
            .insert_root("/test/downloads")
            .expect("Failed to create test root");
        db.upsert_entry(
            root_id,
            "/test/downloads/subdir",
            "/test/downloads",
            true,
            0,
            None,
        )
        .expect("Failed to create dir entry");
        db.upsert_entry(
            root_id,
            "/test/downloads/file.txt",
            "/test/downloads",
            false,
            100,
            Some(1000),
        )
        .expect("Failed to create file entry");

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = "/test/downloads".to_string();
        // Sort by name puts directories first, so index 0 should be the subdir
        app.sort_mode = SortMode::Name;
        app.entry_selected_index = 0;

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('l')));

        assert_eq!(app.current_path, "/test/downloads/subdir");
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn l_on_file_entry_is_noop() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        let root_id = db
            .insert_root("/test/downloads")
            .expect("Failed to create test root");
        db.upsert_entry(
            root_id,
            "/test/downloads/file.txt",
            "/test/downloads",
            false,
            100,
            Some(1000),
        )
        .expect("Failed to create file entry");

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = "/test/downloads".to_string();
        app.entry_selected_index = 0;

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('l')));

        // Should remain in the same directory
        assert_eq!(app.current_path, "/test/downloads");
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
        assert_eq!(app.entry_selected_index, 0, "File index should not change");

        // Navigate down in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('j')));
        assert_eq!(app.entry_selected_index, 1);
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
        app.entry_selected_index = 5;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('k')));
        assert_eq!(app.entry_selected_index, 4);
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
        app.entry_selected_index = 10;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('g')));
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn capital_g_goes_to_bottom_of_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Simulate list lengths (normally set by render)
        app.sidebar_len.set(10);
        app.entry_list_len.set(20);

        // Go to bottom in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('G')));
        assert_eq!(app.sidebar_selected_index, 9); // len - 1

        // Go to bottom in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('G')));
        assert_eq!(app.entry_selected_index, 19); // len - 1
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

    // ===== Root Entry/Exit Tests =====

    #[test]
    fn enter_in_sidebar_sets_current_path_and_focuses_main_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Create a root in the database
        db.insert_root("/test/downloads")
            .expect("Failed to create test root");

        // Focus sidebar and select the root
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;

        // Press Enter
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Enter));

        assert_eq!(app.current_path, "/test/downloads");
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn enter_in_sidebar_with_no_roots_is_noop() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        app.focus_panel = FocusPanel::Sidebar;

        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Enter));

        assert!(app.current_path.is_empty());
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
    }

    #[test]
    fn backspace_at_root_level_returns_to_sidebar() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        db.insert_root("/test/downloads")
            .expect("Failed to create test root");

        // Simulate being inside a root
        app.current_path = "/test/downloads".to_string();
        app.focus_panel = FocusPanel::MainPanel;

        // Press Backspace
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Backspace));

        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
    }

    #[test]
    fn backspace_not_at_root_level_is_noop() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        db.insert_root("/test/downloads")
            .expect("Failed to create test root");

        // Simulate being inside a subdirectory within a root
        app.current_path = "/test/downloads/subdir".to_string();
        app.focus_panel = FocusPanel::MainPanel;

        // Press Backspace — should not switch to sidebar since we're not at root level
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Backspace));

        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
    }

    // ===== File-Level Action Tests =====

    /// Helper to set up test with a root and entries in the database.
    fn setup_with_files(db: &Database) -> (i64, Vec<i64>) {
        // Create a test root
        let root_id = db
            .insert_root("/test/dir")
            .expect("Failed to create test root");

        // Insert two test entries (files)
        let entry1_id = db
            .upsert_entry(
                root_id,
                "/test/dir/file1.txt",
                "/test/dir",
                false,
                500,
                Some(100),
            )
            .expect("Failed to create entry1");
        let entry2_id = db
            .upsert_entry(
                root_id,
                "/test/dir/file2.txt",
                "/test/dir",
                false,
                500,
                Some(150),
            )
            .expect("Failed to create entry2");

        (root_id, vec![entry1_id, entry2_id])
    }

    #[test]
    fn d_key_initiates_file_delete_confirmation() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);

        // Set up app state to simulate viewing directory with first entry selected
        app.current_root_id.set(Some(dir_id));
        app.current_path = "/test/dir".to_string();
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;

        // Press 'd' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('d')));

        // Should set pending_file_delete with first file
        assert!(
            app.pending_entry_delete.is_some(),
            "pending_file_delete should be set"
        );
        let files = app
            .pending_entry_delete
            .as_ref()
            .expect("Expected pending delete");
        assert_eq!(files.len(), 1, "Should have one file pending deletion");
        assert_eq!(files[0].0, file_ids[0], "Should be first file");
        assert_eq!(
            files[0].1, "/test/dir/file1.txt",
            "Path should match first file"
        );
    }

    #[test]
    fn d_key_ignored_when_sidebar_focused() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, _file_ids) = setup_with_files(&db);

        // Focus sidebar (not main panel)
        app.current_root_id.set(Some(dir_id));
        app.focus_panel = FocusPanel::Sidebar;

        // Press 'd' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('d')));

        // Should NOT set pending_file_delete when sidebar is focused
        assert!(
            app.pending_entry_delete.is_none(),
            "pending_file_delete should not be set when sidebar focused"
        );
    }

    #[test]
    fn file_delete_confirmation_y_deletes_file() {
        let (db, _db_dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Create a real temporary file for deletion
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_file.txt");
        std::fs::write(&file_path, b"test content").expect("Failed to create temp file");

        // Set up database with the real file path
        let root_id = db
            .insert_root(temp_dir.path().to_str().expect("Invalid path"))
            .expect("Failed to create test root");
        let file_path_str = file_path.to_str().expect("Invalid path");
        let parent_path = temp_dir.path().to_str().expect("Invalid path");
        let entry_id = db
            .upsert_entry(root_id, file_path_str, parent_path, false, 13, Some(100))
            .expect("Failed to create entry");

        app.current_root_id.set(Some(root_id));

        // Manually set pending delete (simulating 'd' key press)
        // pending_entry_delete is Vec<(entry_id, path, is_dir)>
        app.pending_entry_delete = Some(vec![(entry_id, file_path_str.to_string(), false)]);

        // Press 'y' to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('y')));

        // Should clear pending delete
        assert!(
            app.pending_entry_delete.is_none(),
            "pending_entry_delete should be cleared"
        );

        // Entry should be marked as removed in database (query directly since
        // list_entries_by_parent filters out removed entries)
        let status: String = db
            .conn()
            .query_row(
                "SELECT status FROM entries WHERE id = ?1",
                [entry_id],
                |row| row.get(0),
            )
            .expect("Entry should still exist in DB");
        assert_eq!(status, "removed", "Entry status should be 'removed'");

        // Entry should no longer appear in the active entries list
        let entries = db
            .list_entries_by_parent(parent_path)
            .expect("Failed to list entries");
        assert!(
            !entries.iter().any(|e| e.id == entry_id),
            "Removed entry should not appear in list_entries_by_parent"
        );

        // File should be deleted from filesystem
        assert!(
            !file_path.exists(),
            "File should be deleted from filesystem"
        );
    }

    #[test]
    fn file_delete_confirmation_n_cancels() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id.set(Some(dir_id));

        // Manually set pending delete (entry_id, path, is_dir)
        app.pending_entry_delete = Some(vec![(
            file_ids[0],
            "/test/dir/file1.txt".to_string(),
            false,
        )]);

        // Press 'n' to cancel
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('n')));

        // Should clear pending delete
        assert!(
            app.pending_entry_delete.is_none(),
            "pending_file_delete should be cleared"
        );

        // Entry should still be in tracked status
        let entries = db
            .list_entries_by_parent("/test/dir")
            .expect("Failed to list entries");
        let entry = entries
            .iter()
            .find(|e| e.id == file_ids[0])
            .expect("Entry should exist");
        assert_eq!(
            entry.status, "tracked",
            "Entry status should remain 'tracked'"
        );
    }

    #[test]
    fn r_key_initiates_file_deferral() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, _file_ids) = setup_with_files(&db);

        // Set up app state
        app.current_root_id.set(Some(dir_id));
        app.current_path = "/test/dir".to_string();
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;

        // Press 'r' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('r')));

        // Should set pending_file_deferral
        assert!(
            app.pending_entry_deferral.is_some(),
            "pending_file_deferral should be set"
        );
        let deferral = app
            .pending_entry_deferral
            .as_ref()
            .expect("Expected deferral");
        assert_eq!(deferral.path, "/test/dir/file1.txt");
        assert_eq!(deferral.default_days, 90); // from test_config
    }

    #[test]
    fn file_deferral_enter_confirms_with_default_days() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id.set(Some(dir_id));

        // Manually set pending deferral (empty input means use default)
        app.pending_entry_deferral = Some(PendingDeferral {
            entry_id: file_ids[0],
            path: "/test/dir/file1.txt".to_string(),
            input: String::new(),
            default_days: 90,
            additional_entry_ids: Vec::new(),
        });

        // Press Enter to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Enter));

        // Should clear pending deferral
        assert!(
            app.pending_entry_deferral.is_none(),
            "pending_file_deferral should be cleared"
        );

        // Entry should be marked as deferred
        let entries = db
            .list_entries_by_parent("/test/dir")
            .expect("Failed to list entries");
        let entry = entries
            .iter()
            .find(|e| e.id == file_ids[0])
            .expect("Entry should exist");
        assert_eq!(
            entry.status, "deferred",
            "Entry status should be 'deferred'"
        );
        assert!(
            entry.deferred_until.is_some(),
            "deferred_until should be set"
        );
    }

    #[test]
    fn file_deferral_enter_confirms_with_custom_days() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id.set(Some(dir_id));

        // Manually set pending deferral with input
        app.pending_entry_deferral = Some(PendingDeferral {
            entry_id: file_ids[0],
            path: "/test/dir/file1.txt".to_string(),
            input: "30".to_string(),
            default_days: 90,
            additional_entry_ids: Vec::new(),
        });

        // Press Enter to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Enter));

        // Should clear pending deferral
        assert!(
            app.pending_entry_deferral.is_none(),
            "pending_file_deferral should be cleared"
        );

        // Entry should be deferred with calculated timestamp
        let entries = db
            .list_entries_by_parent("/test/dir")
            .expect("Failed to list entries");
        let entry = entries
            .iter()
            .find(|e| e.id == file_ids[0])
            .expect("Entry should exist");
        assert_eq!(entry.status, "deferred");

        // Verify deferred_until is approximately 30 days in the future
        let now = jiff::Timestamp::now().as_second();
        let days_30_secs = 30 * 86400;
        let expected_until = now + days_30_secs;
        let actual_until = entry.deferred_until.expect("deferred_until should be set");
        let diff = (actual_until - expected_until).abs();
        assert!(
            diff < 10,
            "deferred_until should be approximately 30 days from now (diff: {diff})"
        );
    }

    #[test]
    fn i_key_initiates_file_ignore() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);

        // Set up app state
        app.current_root_id.set(Some(dir_id));
        app.current_path = "/test/dir".to_string();
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;

        // Press 'i' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('i')));

        // Should set pending_file_ignore
        assert!(
            app.pending_entry_ignore.is_some(),
            "pending_file_ignore should be set"
        );
        let entries = app
            .pending_entry_ignore
            .as_ref()
            .expect("Expected pending ignore");
        assert_eq!(entries.len(), 1, "Should have one entry pending ignore");
        assert_eq!(entries[0].0, file_ids[0]);
        assert_eq!(entries[0].1, "/test/dir/file1.txt");
    }

    #[test]
    fn file_ignore_confirmation_y_ignores_file() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id.set(Some(dir_id));

        // Manually set pending ignore
        app.pending_entry_ignore = Some(vec![(file_ids[0], "/test/dir/file1.txt".to_string())]);

        // Press 'y' to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('y')));

        // Should clear pending ignore
        assert!(
            app.pending_entry_ignore.is_none(),
            "pending_file_ignore should be cleared"
        );

        // Entry should be marked as ignored
        let entries = db
            .list_entries_by_parent("/test/dir")
            .expect("Failed to list entries");
        let entry = entries
            .iter()
            .find(|e| e.id == file_ids[0])
            .expect("Entry should exist");
        assert_eq!(entry.status, "ignored", "Entry status should be 'ignored'");
    }

    #[test]
    fn x_key_initiates_file_approval() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);

        // Set up app state
        app.current_root_id.set(Some(dir_id));
        app.current_path = "/test/dir".to_string();
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;

        // Press 'x' key
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('x')));

        // Should set pending_file_approval
        assert!(
            app.pending_entry_approval.is_some(),
            "pending_file_approval should be set"
        );
        let entries = app
            .pending_entry_approval
            .as_ref()
            .expect("Expected pending approval");
        assert_eq!(entries.len(), 1, "Should have one entry pending approval");
        assert_eq!(entries[0].0, file_ids[0]);
        assert_eq!(entries[0].1, "/test/dir/file1.txt");
    }

    #[test]
    fn file_approval_confirmation_y_approves_file() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let config = test_config();

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id.set(Some(dir_id));

        // Manually set pending approval
        app.pending_entry_approval = Some(vec![(file_ids[0], "/test/dir/file1.txt".to_string())]);

        // Press 'y' to confirm
        InputHandler::handle(&mut app, &config, &db, make_key_event(KeyCode::Char('y')));

        // Should clear pending approval
        assert!(
            app.pending_entry_approval.is_none(),
            "pending_file_approval should be cleared"
        );

        // Entry should be marked as approved
        let entries = db
            .list_entries_by_parent("/test/dir")
            .expect("Failed to list entries");
        let entry = entries
            .iter()
            .find(|e| e.id == file_ids[0])
            .expect("Entry should exist");
        assert_eq!(
            entry.status, "approved",
            "Entry status should be 'approved'"
        );
    }
}
