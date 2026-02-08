//! Vim-style keybinding handling.

use std::path::PathBuf;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::ui::sort_entry_rows;
use super::{App, FocusPanel, PendingDeletion, PendingEntry, SortMode, TuiContext, View};
use crate::audit::{AuditAction, AuditService};
use crate::config::Config;
use crate::db::Database;
use crate::removal::RemovalMethod;
use crate::scanner::calculate_expiration;

/// Handles keyboard input with vim-style bindings.
pub(crate) struct InputHandler;

/// Find indices of entries whose filename contains the query (case-insensitive).
///
/// Operates on the already-sorted entry rows so indices correspond to what the
/// user sees on screen.
pub(super) fn find_search_matches(
    entry_rows: &[(crate::db::Entry, i64)],
    query: &str,
) -> Vec<usize> {
    if query.is_empty() {
        return Vec::new();
    }
    let query_lower = query.to_lowercase();
    entry_rows
        .iter()
        .enumerate()
        .filter(|(_, (entry, _))| {
            let filename = entry.path.file_name().map_or_else(
                || entry.path.to_string_lossy().to_lowercase(),
                |f| f.to_string_lossy().to_lowercase(),
            );
            filename.contains(&query_lower)
        })
        .map(|(i, _)| i)
        .collect()
}

impl InputHandler {
    /// Process a key event and update app state.
    ///
    /// Takes a context reference for database access and per-root config resolution.
    pub fn handle(app: &mut App, ctx: &TuiContext, key: KeyEvent) {
        match app.view {
            View::FileList => Self::handle_file_list(app, ctx, key),
            View::AuditLog => Self::handle_audit_log(app, key),
            View::Help => Self::handle_help(app, key),
        }
    }

    // Allow: This function coordinates input handling for the file list view, including
    // modal dispatch, focus management, navigation, actions, and view switching. Breaking
    // it into smaller functions would obscure the input handling flow.
    #[allow(clippy::too_many_lines)]
    fn handle_file_list(app: &mut App, ctx: &TuiContext, key: KeyEvent) {
        let config = ctx.config(app);
        let db = ctx.db;

        // Check for search input mode first (highest priority text input)
        if app.search_input_active {
            Self::handle_search_input(app, ctx, key);
            return;
        }

        // Check for pending confirmations/inputs first
        if app.pending_add_path.is_some() {
            Self::handle_add_path_input(app, ctx, key);
            return;
        }
        if app.pending_remove_path.is_some() {
            Self::handle_remove_path_confirmation(app, db, key);
            return;
        }
        if app.pending_entry_delete.is_some() {
            Self::handle_entry_delete_confirmation(app, ctx, key);
            return;
        }
        if app.pending_entry_deferral.is_some() {
            Self::handle_entry_deferral_input(app, ctx, key);
            return;
        }
        if app.pending_entry_ignore.is_some() {
            Self::handle_entry_ignore_confirmation(app, ctx, key);
            return;
        }
        if app.pending_quota_target.is_some() {
            Self::handle_quota_target_input(app, db, key);
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
                Self::handle_h_navigation(app, ctx);
            }
            KeyCode::Char('l') => {
                Self::handle_l_navigation(app, ctx);
            }

            // Navigation (vim-style) - operates on focused panel
            KeyCode::Char('j') | KeyCode::Down => {
                match app.focus_panel {
                    FocusPanel::Sidebar => {
                        app.sidebar_selected_index = app.sidebar_selected_index.saturating_add(1);
                        if app.sidebar_len > 0 {
                            app.sidebar_selected_index =
                                app.sidebar_selected_index.min(app.sidebar_len - 1);
                        }
                        app.sync_to_sidebar_selection(db);
                    }
                    FocusPanel::MainPanel => {
                        app.entry_selected_index = app.entry_selected_index.saturating_add(1);
                        if app.entry_list_len > 0 {
                            app.entry_selected_index =
                                app.entry_selected_index.min(app.entry_list_len - 1);
                        }
                        Self::update_visual_selection(app, ctx);
                    }
                }
                app.ensure_cursor_visible = true;
            }
            KeyCode::Char('k') | KeyCode::Up => {
                match app.focus_panel {
                    FocusPanel::Sidebar => {
                        app.sidebar_selected_index = app.sidebar_selected_index.saturating_sub(1);
                        app.sync_to_sidebar_selection(db);
                    }
                    FocusPanel::MainPanel => {
                        app.entry_selected_index = app.entry_selected_index.saturating_sub(1);
                        Self::update_visual_selection(app, ctx);
                    }
                }
                app.ensure_cursor_visible = true;
            }
            KeyCode::Char('g') => {
                // Go to top of focused panel
                match app.focus_panel {
                    FocusPanel::Sidebar => app.sidebar_selected_index = 0,
                    FocusPanel::MainPanel => {
                        app.entry_selected_index = 0;
                        Self::update_visual_selection(app, ctx);
                    }
                }
                app.ensure_cursor_visible = true;
            }
            KeyCode::Char('G') => {
                // Go to bottom of focused panel
                match app.focus_panel {
                    FocusPanel::Sidebar => {
                        app.select_last_sidebar(app.sidebar_len);
                    }
                    FocusPanel::MainPanel => {
                        app.select_last_entry(app.entry_list_len);
                        Self::update_visual_selection(app, ctx);
                    }
                }
                app.ensure_cursor_visible = true;
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
                Self::toggle_entry_selection(app, ctx);
            }
            KeyCode::Char('v') if app.focus_panel == FocusPanel::MainPanel => {
                Self::toggle_visual_mode(app, ctx);
            }
            KeyCode::Char('a') if app.focus_panel == FocusPanel::MainPanel => {
                Self::select_all_entries(app, ctx);
            }
            KeyCode::Esc if app.focus_panel == FocusPanel::MainPanel => {
                if app.is_visual_mode() {
                    app.exit_visual_mode();
                } else if app.search_query.is_some() {
                    app.clear_search();
                } else {
                    app.clear_selection();
                }
            }

            // Entry-level actions (only when main panel is focused)
            // d = move to trash (safe, recoverable)
            KeyCode::Char('d') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_entry_delete(app, config, db, RemovalMethod::Trash);
            }
            // D = permanent delete (irreversible)
            KeyCode::Char('D') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_entry_delete(app, config, db, RemovalMethod::PermanentDelete);
            }
            KeyCode::Char('r') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_entry_defer(app, config, db);
            }
            KeyCode::Char('i') if app.focus_panel == FocusPanel::MainPanel => {
                Self::initiate_entry_ignore(app, config, db);
            }
            KeyCode::Char('x') if app.focus_panel == FocusPanel::MainPanel => {
                Self::toggle_entry_approval(app, ctx);
            }
            KeyCode::Char('u') if app.focus_panel == FocusPanel::MainPanel => {
                Self::unignore_entry(app, ctx);
            }

            // Open in external application
            KeyCode::Char('e') if app.focus_panel == FocusPanel::MainPanel => {
                Self::request_open_in_editor(app, ctx);
            }
            KeyCode::Char('o') if app.focus_panel == FocusPanel::MainPanel => {
                Self::request_open_in_system_viewer(app, ctx);
            }

            // Search
            KeyCode::Char('/') if app.focus_panel == FocusPanel::MainPanel => {
                app.search_query = Some(String::new());
                app.search_input_active = true;
            }
            KeyCode::Char('n') if app.search_query.is_some() => {
                Self::jump_to_next_match(app, ctx);
                app.ensure_cursor_visible = true;
            }
            KeyCode::Char('N') if app.search_query.is_some() => {
                Self::jump_to_prev_match(app, ctx);
                app.ensure_cursor_visible = true;
            }

            // Views
            KeyCode::Char('1') => app.view = View::FileList,
            KeyCode::Char('2') => app.view = View::AuditLog,
            KeyCode::Char('3' | '?') => app.view = View::Help,

            // Refresh tracked paths (scan filesystem + transition expired files)
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
                Self::initiate_remove_path(app, ctx);
            }

            // Quota target (t = set target for selected root, works from sidebar or entries panel)
            KeyCode::Char('t')
                if matches!(app.focus_panel, FocusPanel::Sidebar | FocusPanel::MainPanel) =>
            {
                Self::initiate_quota_target(app, db);
            }

            _ => {}
        }
    }

    /// Toggle selection of the currently focused file.
    fn toggle_entry_selection(app: &mut App, ctx: &TuiContext) {
        let Some(entry_rows) = ctx.sorted_entry_rows(app) else {
            tracing::warn!("Cannot toggle selection: no path selected or query failed");
            return;
        };

        let Some((entry, _)) = entry_rows.get(app.entry_selected_index) else {
            tracing::warn!("No entry selected (index out of bounds)");
            return;
        };

        // Exit visual mode if active — Space is a manual override
        app.exit_visual_mode();

        // Toggle selection and advance cursor for hold-to-multi-select behavior
        app.toggle_entry_selection(entry.id);
        app.entry_selected_index = app.entry_selected_index.saturating_add(1);
    }

    /// Recompute the visual selection after a cursor movement.
    ///
    /// No-op if visual mode is not active.
    fn update_visual_selection(app: &mut App, ctx: &TuiContext) {
        if !app.is_visual_mode() {
            return;
        }
        let Some(entry_rows) = ctx.sorted_entry_rows(app) else {
            return;
        };
        let entry_ids: Vec<i64> = entry_rows.iter().map(|(e, _)| e.id).collect();
        app.recompute_visual_selection(&entry_ids);
    }

    /// Toggle visual mode on/off.
    ///
    /// On entry: snapshots the current selection, sets the anchor at the cursor,
    /// and selects the entry under the cursor.
    /// On exit (pressing `v` again): keeps the selection, clears visual state.
    fn toggle_visual_mode(app: &mut App, ctx: &TuiContext) {
        if app.is_visual_mode() {
            app.exit_visual_mode();
            return;
        }

        let Some(entry_rows) = ctx.sorted_entry_rows(app) else {
            tracing::warn!("Cannot enter visual mode: no path selected or query failed");
            return;
        };

        if entry_rows.is_empty() {
            return;
        }

        // Snapshot current selection so visual range is additive
        app.pre_visual_selection = app.selected_entries.clone();

        // Set anchor at current cursor position
        let cursor = app
            .entry_selected_index
            .min(entry_rows.len().saturating_sub(1));
        app.visual_anchor = Some(cursor);

        // Select the entry under the cursor
        if let Some((entry, _)) = entry_rows.get(cursor) {
            app.selected_entries.insert(entry.id);
        }
    }

    /// Select all entries in the current directory.
    fn select_all_entries(app: &mut App, ctx: &TuiContext) {
        let Some(entry_rows) = ctx.sorted_entry_rows(app) else {
            tracing::warn!("Cannot select all: no path selected or query failed");
            return;
        };

        // Exit visual mode if active
        app.exit_visual_mode();

        // Add all entry IDs to selection
        for (entry, _) in &entry_rows {
            app.selected_entries.insert(entry.id);
        }
    }

    fn handle_audit_log(app: &mut App, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q' | 'h' | '1') | KeyCode::Esc => {
                app.view = View::FileList;
            }
            KeyCode::Char('3' | '?') => {
                app.view = View::Help;
            }
            KeyCode::Char('j') | KeyCode::Down => {
                app.sidebar_selected_index = app.sidebar_selected_index.saturating_add(1);
                app.ensure_cursor_visible = true;
            }
            KeyCode::Char('k') | KeyCode::Up => {
                app.sidebar_selected_index = app.sidebar_selected_index.saturating_sub(1);
                app.ensure_cursor_visible = true;
            }
            KeyCode::Char('g') => {
                app.sidebar_selected_index = 0; // Go to top
                app.ensure_cursor_visible = true;
            }
            KeyCode::Char('G') => {
                app.select_last_sidebar(app.sidebar_len); // Go to bottom
                app.ensure_cursor_visible = true;
            }
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
    fn handle_h_navigation(app: &mut App, ctx: &TuiContext) {
        match app.focus_panel {
            FocusPanel::Sidebar => {}
            FocusPanel::MainPanel => {
                app.exit_visual_mode();
                if let Ok(roots) = ctx.db.list_roots() {
                    let at_root_level = roots.iter().any(|r| r.path == app.current_path);
                    if at_root_level {
                        app.sidebar_visible = true;
                        app.focus_panel = FocusPanel::Sidebar;
                    } else {
                        app.navigate_up(ctx);
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
    fn handle_l_navigation(app: &mut App, ctx: &TuiContext) {
        match app.focus_panel {
            FocusPanel::Sidebar => {
                // Enter the selected root (same behavior as Enter)
                if let Ok(roots) = ctx.db.list_roots() {
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
                let Some(entry_rows) = ctx.sorted_entry_rows(app) else {
                    return;
                };

                if let Some((entry, _)) = entry_rows.get(app.entry_selected_index)
                    && entry.is_dir
                {
                    app.exit_visual_mode();
                    app.navigate_into(entry.path.clone());
                }
            }
        }
    }

    /// Initiate entry deletion by querying the database for the selected entry/entries.
    ///
    /// If entries are selected via multi-select, delete all selected entries.
    /// Otherwise, delete the currently focused entry.
    ///
    /// The `method` parameter determines whether to trash (recoverable) or
    /// permanently delete (irreversible).
    fn initiate_entry_delete(app: &mut App, config: &Config, db: &Database, method: RemovalMethod) {
        // Get entries for current path
        if app.current_path.as_os_str().is_empty() {
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
                let days_remaining = entry.countdown_start.map_or(i64::MAX, |cs| {
                    calculate_expiration(cs, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Determine which entries to delete
        let entries_to_delete: Vec<PendingEntry> = if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                vec![PendingEntry {
                    id: entry.id,
                    path: entry.path.clone(),
                    is_dir: entry.is_dir,
                }]
            } else {
                tracing::warn!("No entry selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected entries (selection is by ID, so sorting doesn't matter here)
            entry_rows
                .into_iter()
                .filter(|(e, _)| app.selected_entries.contains(&e.id))
                .map(|(e, _)| PendingEntry {
                    id: e.id,
                    path: e.path.clone(),
                    is_dir: e.is_dir,
                })
                .collect()
        };

        if entries_to_delete.is_empty() {
            tracing::warn!("No entries to delete");
            return;
        }

        // Set pending deletion state with the chosen method
        app.pending_entry_delete = Some(PendingDeletion {
            entries: entries_to_delete,
            method,
        });
    }

    /// Handle entry deletion confirmation (y/n/Esc).
    fn handle_entry_delete_confirmation(app: &mut App, ctx: &TuiContext, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the deletion for all pending entries
                if let Some(deletion) = &app.pending_entry_delete {
                    let audit = AuditService::new(ctx.db);
                    let user = AuditService::current_user();
                    let root_id = app.current_root_id;
                    let method = deletion.method;
                    let action_past = method.past_tense();

                    let mut success_count = 0;
                    let mut fail_count = 0;
                    let total = deletion.entries.len();

                    for entry in &deletion.entries {
                        if let Err(e) =
                            ctx.db
                                .delete_entry(entry.id, &entry.path, entry.is_dir, method)
                        {
                            tracing::warn!(
                                "Failed to {} entry {}: {}",
                                method.description(),
                                entry.path.display(),
                                e
                            );
                            app.status_message =
                                Some(format!("{} failed: {e}", method.past_tense()));
                            app.status_message_time = Some(std::time::Instant::now());
                            fail_count += 1;
                        } else {
                            success_count += 1;
                            // Record audit entry with method-specific detail
                            let detail = match method {
                                RemovalMethod::Trash => {
                                    if entry.is_dir {
                                        "Directory moved to trash by user"
                                    } else {
                                        "File moved to trash by user"
                                    }
                                }
                                RemovalMethod::PermanentDelete => {
                                    if entry.is_dir {
                                        "Directory permanently deleted by user"
                                    } else {
                                        "File permanently deleted by user"
                                    }
                                }
                            };
                            if let Err(e) = audit.record(
                                &user,
                                AuditAction::Remove,
                                Some(entry.path.as_path()),
                                Some(detail),
                                root_id,
                            ) {
                                tracing::warn!("Failed to record audit entry for deletion: {}", e);
                            }
                        }
                    }

                    // Show result in status bar
                    if fail_count == 0 && success_count > 0 {
                        app.status_message = Some(format!("{action_past} {success_count} file(s)"));
                        app.status_message_time = Some(std::time::Instant::now());
                    } else if fail_count > 0 && success_count > 0 {
                        app.status_message = Some(format!(
                            "{action_past} {success_count}/{total}, {fail_count} failed"
                        ));
                        app.status_message_time = Some(std::time::Instant::now());
                    }
                    // If all failed, the last error message is already set
                } else {
                    app.status_message = Some("No files pending delete".to_string());
                    app.status_message_time = Some(std::time::Instant::now());
                }
                // Clear pending deletion, visual mode, and selection
                app.pending_entry_delete = None;
                app.exit_visual_mode();
                app.clear_selection();
                app.refresh_stats(ctx);
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
        if app.current_path.as_os_str().is_empty() {
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
                let days_remaining = entry.countdown_start.map_or(i64::MAX, |cs| {
                    calculate_expiration(cs, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Determine which entries to defer
        let entries_to_defer: Vec<super::PendingEntry> = if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                vec![super::PendingEntry {
                    id: entry.id,
                    path: entry.path.clone(),
                    is_dir: entry.is_dir,
                }]
            } else {
                tracing::warn!("No entry selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected entries (selection is by ID, so sorting doesn't matter here)
            entry_rows
                .into_iter()
                .filter(|(e, _)| app.selected_entries.contains(&e.id))
                .map(|(e, _)| super::PendingEntry {
                    id: e.id,
                    path: e.path.clone(),
                    is_dir: e.is_dir,
                })
                .collect()
        };

        if entries_to_defer.is_empty() {
            tracing::warn!("No entries to defer");
            return;
        }

        // Set pending deferral state
        app.pending_entry_deferral = Some(super::PendingDeferral {
            entries: entries_to_defer,
            input: String::new(),
            default_days: config.expiration_days,
        });
    }

    /// Handle entry deferral input (digits/backspace/enter/esc).
    fn handle_entry_deferral_input(app: &mut App, ctx: &TuiContext, key: KeyEvent) {
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

                    let audit = AuditService::new(ctx.db);
                    let user = AuditService::current_user();
                    let details = Some(format!("Deferred for {days} days"));
                    let root_id = app.current_root_id;

                    for entry in &deferral.entries {
                        if let Err(e) = ctx.db.defer_entry(entry.id, deferred_until) {
                            tracing::warn!("Failed to defer entry {}: {}", entry.path.display(), e);
                        } else {
                            // Propagate to children if this is a directory
                            if entry.is_dir
                                && let Err(e) = ctx
                                    .db
                                    .defer_entries_by_path_prefix(&entry.path, deferred_until)
                            {
                                tracing::warn!(
                                    "Failed to propagate deferral to children of {}: {}",
                                    entry.path.display(),
                                    e
                                );
                            }
                            if let Err(e) = audit.record(
                                &user,
                                AuditAction::Defer,
                                Some(entry.path.as_path()),
                                details.as_deref(),
                                root_id,
                            ) {
                                tracing::warn!("Failed to record audit entry for deferral: {}", e);
                            }
                        }
                    }
                }
                // Clear pending deferral, visual mode, and selection
                app.pending_entry_deferral = None;
                app.exit_visual_mode();
                app.clear_selection();
                app.refresh_stats(ctx);
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
        if app.current_path.as_os_str().is_empty() {
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
                let days_remaining = entry.countdown_start.map_or(i64::MAX, |cs| {
                    calculate_expiration(cs, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Determine which entries to ignore
        let entries_to_ignore: Vec<super::PendingEntry> = if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                vec![super::PendingEntry {
                    id: entry.id,
                    path: entry.path.clone(),
                    is_dir: entry.is_dir,
                }]
            } else {
                tracing::warn!("No entry selected (index out of bounds)");
                return;
            }
        } else {
            // Use selected entries (selection is by ID, so sorting doesn't matter here)
            entry_rows
                .into_iter()
                .filter(|(e, _)| app.selected_entries.contains(&e.id))
                .map(|(e, _)| super::PendingEntry {
                    id: e.id,
                    path: e.path.clone(),
                    is_dir: e.is_dir,
                })
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
    fn handle_entry_ignore_confirmation(app: &mut App, ctx: &TuiContext, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y' | 'Y') => {
                // User confirmed - perform the ignore for all pending entries
                if let Some(entries) = &app.pending_entry_ignore {
                    let audit = AuditService::new(ctx.db);
                    let user = AuditService::current_user();
                    let root_id = app.current_root_id;

                    for entry in entries {
                        if let Err(e) = ctx.db.update_entry_status(entry.id, "ignored") {
                            tracing::warn!(
                                "Failed to ignore entry {}: {}",
                                entry.path.display(),
                                e
                            );
                        } else {
                            // Propagate to children if this is a directory
                            if entry.is_dir
                                && let Err(e) =
                                    ctx.db.update_entries_by_path_prefix(&entry.path, "ignored")
                            {
                                tracing::warn!(
                                    "Failed to propagate ignore to children of {}: {}",
                                    entry.path.display(),
                                    e
                                );
                            }
                            if let Err(e) = audit.record(
                                &user,
                                AuditAction::Ignore,
                                Some(entry.path.as_path()),
                                None,
                                root_id,
                            ) {
                                tracing::warn!("Failed to record audit entry for ignore: {}", e);
                            }
                        }
                    }
                }
                // Clear pending ignore, visual mode, and selection
                app.pending_entry_ignore = None;
                app.exit_visual_mode();
                app.clear_selection();
                app.refresh_stats(ctx);
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

    /// Toggle approval state directly (no confirmation modal).
    ///
    /// Pressing `x` on an approved entry unapproves it back to tracked.
    /// Pressing `x` on other entries approves them.
    ///
    /// If entries are multi-selected, toggles each selected entry.
    fn toggle_entry_approval(app: &mut App, ctx: &TuiContext) {
        // Get entries for current path
        if app.current_path.as_os_str().is_empty() {
            tracing::warn!("Cannot toggle approval: no path selected");
            return;
        }

        let Some(entry_rows) = ctx.sorted_entry_rows(app) else {
            tracing::warn!("Failed to query entries");
            return;
        };

        // Determine which entries to toggle
        let entries_to_toggle: Vec<(super::PendingEntry, String)> =
            if app.selected_entries.is_empty() {
                // No selection - use currently focused entry
                if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                    vec![(
                        super::PendingEntry {
                            id: entry.id,
                            path: entry.path.clone(),
                            is_dir: entry.is_dir,
                        },
                        entry.status.clone(),
                    )]
                } else {
                    tracing::warn!("No entry selected (index out of bounds)");
                    return;
                }
            } else {
                // Use selected entries (selection is by ID, so sorting doesn't matter here)
                entry_rows
                    .into_iter()
                    .filter(|(e, _)| app.selected_entries.contains(&e.id))
                    .map(|(e, _)| {
                        (
                            super::PendingEntry {
                                id: e.id,
                                path: e.path.clone(),
                                is_dir: e.is_dir,
                            },
                            e.status,
                        )
                    })
                    .collect()
            };

        if entries_to_toggle.is_empty() {
            tracing::warn!("No entries to toggle approval");
            return;
        }

        let audit = AuditService::new(ctx.db);
        let user = AuditService::current_user();
        let root_id = app.current_root_id;

        let mut approved_count = 0;
        let mut unapproved_count = 0;

        for (entry, current_status) in &entries_to_toggle {
            let (next_status, details) = if current_status == "approved" {
                unapproved_count += 1;
                ("tracked", Some("Unapproved (set status to tracked)"))
            } else {
                approved_count += 1;
                ("approved", None)
            };

            if let Err(e) = ctx.db.update_entry_status(entry.id, next_status) {
                tracing::warn!(
                    "Failed to set approval status for entry {}: {}",
                    entry.path.display(),
                    e
                );
                continue;
            }

            // Propagate to children if this is a directory
            if entry.is_dir
                && let Err(e) = ctx
                    .db
                    .update_entries_by_path_prefix(&entry.path, next_status)
            {
                tracing::warn!(
                    "Failed to propagate {} to children of {}: {}",
                    next_status,
                    entry.path.display(),
                    e
                );
            }

            if let Err(e) = audit.record(
                &user,
                AuditAction::Approve,
                Some(entry.path.as_path()),
                details,
                root_id,
            ) {
                tracing::warn!("Failed to record audit entry for approval toggle: {}", e);
            }
        }

        // Clear interaction state and refresh.
        app.exit_visual_mode();
        app.clear_selection();
        app.refresh_stats(ctx);

        app.status_message = Some(match (approved_count, unapproved_count) {
            (a, 0) => format!("Approved {a} entr{}", if a == 1 { "y" } else { "ies" }),
            (0, u) => format!("Unapproved {u} entr{}", if u == 1 { "y" } else { "ies" }),
            (a, u) => format!(
                "Updated approval for {} entries ({} approved, {} unapproved)",
                a + u,
                a,
                u
            ),
        });
        app.status_message_time = Some(std::time::Instant::now());
    }

    /// Unignore an entry (reset status from "ignored" back to "tracked").
    ///
    /// This is a direct action without confirmation since it's non-destructive.
    /// Works on selected entries if any, otherwise the currently focused entry.
    fn unignore_entry(app: &mut App, ctx: &TuiContext) {
        // Get entries for current path
        if app.current_path.as_os_str().is_empty() {
            tracing::warn!("Cannot unignore entry: no path selected");
            return;
        }

        let config = ctx.config(app);

        // Query entries for current browsing path
        let entries = match ctx.db.list_entries_by_parent(app.current_path()) {
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
                let days_remaining = entry.countdown_start.map_or(i64::MAX, |cs| {
                    calculate_expiration(cs, config.expiration_days)
                });
                (entry, days_remaining)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, app.sort_mode());

        // Determine which entries to unignore
        let entries_to_unignore: Vec<super::PendingEntry> = if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            if let Some((entry, _)) = entry_rows.get(app.entry_selected_index) {
                // Only unignore if currently ignored
                if entry.status == "ignored" {
                    vec![super::PendingEntry {
                        id: entry.id,
                        path: entry.path.clone(),
                        is_dir: entry.is_dir,
                    }]
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
                .map(|(e, _)| super::PendingEntry {
                    id: e.id,
                    path: e.path.clone(),
                    is_dir: e.is_dir,
                })
                .collect()
        };

        if entries_to_unignore.is_empty() {
            app.status_message = Some("No ignored entries selected".to_string());
            app.status_message_time = Some(std::time::Instant::now());
            return;
        }

        // Perform the unignore
        let audit = AuditService::new(ctx.db);
        let user = AuditService::current_user();
        let root_id = app.current_root_id;

        let mut success_count = 0;
        for entry in &entries_to_unignore {
            if let Err(e) = ctx.db.update_entry_status(entry.id, "tracked") {
                tracing::warn!("Failed to unignore entry {}: {}", entry.path.display(), e);
                app.status_message = Some(format!("Unignore failed: {e}"));
                app.status_message_time = Some(std::time::Instant::now());
            } else {
                success_count += 1;
                // Propagate to children if this is a directory
                if entry.is_dir
                    && let Err(e) = ctx.db.update_entries_by_path_prefix(&entry.path, "tracked")
                {
                    tracing::warn!(
                        "Failed to propagate unignore to children of {}: {}",
                        entry.path.display(),
                        e
                    );
                }
                if let Err(e) = audit.record(
                    &user,
                    AuditAction::Unignore,
                    Some(entry.path.as_path()),
                    None,
                    root_id,
                ) {
                    tracing::warn!("Failed to record audit entry for unignore: {}", e);
                }
            }
        }

        if success_count > 0 {
            app.status_message = Some(format!("Unignored {success_count} entry/entries"));
            app.status_message_time = Some(std::time::Instant::now());
            app.refresh_stats(ctx);
        }
        app.exit_visual_mode();
        app.clear_selection();
    }

    /// Request to open files in $VISUAL or $EDITOR.
    ///
    /// Collects paths from selected entries (or the focused entry if none selected)
    /// and sets `external_open_request` for the main loop to handle.
    fn request_open_in_editor(app: &mut App, ctx: &TuiContext) {
        let paths = Self::collect_paths_for_external_open(app, ctx);
        if paths.is_empty() {
            app.status_message = Some("No files to open".to_string());
            app.status_message_time = Some(std::time::Instant::now());
            return;
        }
        app.external_open_request = Some(super::ExternalOpenRequest::Editor(paths));
    }

    /// Request to open files with the system default viewer.
    ///
    /// Collects paths from selected entries (or the focused entry if none selected)
    /// and sets `external_open_request` for the main loop to handle.
    fn request_open_in_system_viewer(app: &mut App, ctx: &TuiContext) {
        let paths = Self::collect_paths_for_external_open(app, ctx);
        if paths.is_empty() {
            app.status_message = Some("No files to open".to_string());
            app.status_message_time = Some(std::time::Instant::now());
            return;
        }
        app.external_open_request = Some(super::ExternalOpenRequest::SystemViewer(paths));
    }

    /// Collect filesystem paths for opening in an external application.
    ///
    /// Returns paths from selected entries if any are selected, otherwise
    /// returns the path of the currently focused entry.
    fn collect_paths_for_external_open(app: &App, ctx: &TuiContext) -> Vec<PathBuf> {
        let Some(entry_rows) = ctx.sorted_entry_rows(app) else {
            return Vec::new();
        };

        if app.selected_entries.is_empty() {
            // No selection - use currently focused entry
            entry_rows
                .get(app.entry_selected_index)
                .map(|(entry, _)| vec![entry.path.clone()])
                .unwrap_or_default()
        } else {
            // Use selected entries
            entry_rows
                .into_iter()
                .filter(|(e, _)| app.selected_entries.contains(&e.id))
                .map(|(e, _)| e.path)
                .collect()
        }
    }

    /// Handle search input mode (typing a search query).
    ///
    /// Characters append to the query, Backspace removes, Enter confirms and
    /// jumps to the first match, Esc cancels the search entirely.
    fn handle_search_input(app: &mut App, ctx: &TuiContext, key: KeyEvent) {
        match key.code {
            KeyCode::Char(c) if !c.is_control() => {
                if let Some(ref mut query) = app.search_query {
                    query.push(c);
                }
            }
            KeyCode::Backspace => {
                if let Some(ref mut query) = app.search_query {
                    query.pop();
                }
            }
            KeyCode::Enter => {
                // Confirm search and jump to first match
                app.search_input_active = false;
                Self::jump_to_first_match(app, ctx);
                app.ensure_cursor_visible = true;
            }
            KeyCode::Esc => {
                // Cancel search entirely
                app.clear_search();
            }
            _ => {}
        }
    }

    /// Jump the cursor to the first search match in the current entry list.
    fn jump_to_first_match(app: &mut App, ctx: &TuiContext) {
        let matches = ctx.compute_current_matches(app);
        if let Some(&first) = matches.first() {
            app.entry_selected_index = first;
        }
    }

    /// Jump the cursor to the next search match after the current position.
    fn jump_to_next_match(app: &mut App, ctx: &TuiContext) {
        let matches = ctx.compute_current_matches(app);
        if matches.is_empty() {
            return;
        }
        // Find the first match index strictly after the current cursor position.
        // If none found, wrap around to the first match.
        let next = matches
            .iter()
            .find(|&&idx| idx > app.entry_selected_index)
            .or_else(|| matches.first());
        if let Some(&idx) = next {
            app.entry_selected_index = idx;
        }
    }

    /// Jump the cursor to the previous search match before the current position.
    fn jump_to_prev_match(app: &mut App, ctx: &TuiContext) {
        let matches = ctx.compute_current_matches(app);
        if matches.is_empty() {
            return;
        }
        // Find the last match index strictly before the current cursor position.
        // If none found, wrap around to the last match.
        let prev = matches
            .iter()
            .rev()
            .find(|&&idx| idx < app.entry_selected_index)
            .or_else(|| matches.last());
        if let Some(&idx) = prev {
            app.entry_selected_index = idx;
        }
    }

    /// Handle add path text input (characters/backspace/enter/esc).
    fn handle_add_path_input(app: &mut App, ctx: &TuiContext, key: KeyEvent) {
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
                    if ctx
                        .app_config
                        .global
                        .tracked_paths
                        .contains(&canonical_path)
                    {
                        tracing::warn!(
                            "Path already tracked (in config): {}",
                            canonical_path.display()
                        );
                        app.pending_add_path = None;
                        return;
                    }

                    if let Ok(roots) = ctx.db.list_roots()
                        && roots.iter().any(|r| r.path == canonical_path)
                    {
                        tracing::warn!("Path already tracked: {}", canonical_path.display());
                        app.pending_add_path = None;
                        return;
                    }

                    // Insert as a root in the database
                    if let Err(e) = ctx.db.insert_root(&canonical_path) {
                        tracing::warn!("Failed to add root to database: {}", e);
                        app.pending_add_path = None;
                        return;
                    }

                    tracing::info!(
                        "Added tracked path: {} (will be included on next refresh)",
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
    /// because they are re-seeded on every refresh. The user must edit the config
    /// file directly to remove those.
    fn initiate_remove_path(app: &mut App, ctx: &TuiContext) {
        let roots = match ctx.db.list_roots() {
            Ok(roots) => roots,
            Err(e) => {
                tracing::warn!("Failed to query roots: {}", e);
                return;
            }
        };

        if let Some(root) = roots.get(app.sidebar_selected_index) {
            let is_config_root = ctx.app_config.global.tracked_paths.contains(&root.path);

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
                                tracing::info!(
                                    "Removed tracked path: {}",
                                    path_to_remove.display()
                                );
                                // If we were browsing this root, clear the view
                                if app.current_path.starts_with(path_to_remove) {
                                    app.current_path = PathBuf::new();
                                    app.current_root_id = None;
                                    app.focus_panel = FocusPanel::Sidebar;
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::warn!(
                                "Root not found in database: {}",
                                path_to_remove.display()
                            );
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

    /// Initiate quota target edit for the selected root.
    ///
    /// When called from the sidebar, uses the selected root. When called from
    /// the entries panel, finds the root containing the current path.
    fn initiate_quota_target(app: &mut App, db: &Database) {
        let roots = match db.list_roots() {
            Ok(roots) => roots,
            Err(e) => {
                tracing::warn!("Failed to query roots: {}", e);
                return;
            }
        };

        // Determine which root to use based on context
        let root = if app.focus_panel == FocusPanel::Sidebar {
            roots.get(app.sidebar_selected_index)
        } else if !app.current_path.as_os_str().is_empty() {
            roots.iter().find(|r| app.current_path.starts_with(&r.path))
        } else {
            None
        };

        if let Some(root) = root {
            app.pending_quota_target = Some(super::PendingQuotaTarget {
                root_id: root.id,
                root_path: root.path.clone(),
                input: String::new(),
                unit: super::ByteUnit::default(),
                focus: super::QuotaTargetFocus::default(),
                current_target: root.target_bytes,
            });
        } else {
            tracing::warn!("No root selected for quota target");
        }
    }

    /// Handle quota target input (digits, Tab, arrows, Enter, Esc).
    fn handle_quota_target_input(app: &mut App, db: &Database, key: KeyEvent) {
        let Some(ref mut target) = app.pending_quota_target else {
            return;
        };

        match target.focus {
            super::QuotaTargetFocus::Size => match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => {
                    target.input.push(c);
                }
                KeyCode::Backspace => {
                    target.input.pop();
                }
                KeyCode::Tab => {
                    target.focus = super::QuotaTargetFocus::Unit;
                }
                KeyCode::Enter => {
                    Self::confirm_quota_target(app, db);
                }
                KeyCode::Esc => {
                    app.pending_quota_target = None;
                }
                _ => {}
            },
            super::QuotaTargetFocus::Unit => match key.code {
                KeyCode::Left | KeyCode::Char('h') => {
                    target.unit = target.unit.prev();
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    target.unit = target.unit.next();
                }
                KeyCode::Tab => {
                    target.focus = super::QuotaTargetFocus::Size;
                }
                KeyCode::Enter => {
                    Self::confirm_quota_target(app, db);
                }
                KeyCode::Esc => {
                    app.pending_quota_target = None;
                }
                _ => {}
            },
        }
    }

    /// Confirm and save the quota target.
    fn confirm_quota_target(app: &mut App, db: &Database) {
        let Some(target) = app.pending_quota_target.take() else {
            return;
        };

        // Empty input or 0 clears the target
        let target_bytes = if target.input.is_empty() {
            None
        } else {
            match target.input.parse::<u64>() {
                Ok(0) => None,
                Ok(value) => Some(target.unit.to_bytes(value)),
                Err(_) => {
                    tracing::warn!("Invalid quota target input: {}", target.input);
                    return;
                }
            }
        };

        if let Err(e) = db.set_root_target_bytes(target.root_id, target_bytes) {
            tracing::warn!("Failed to set quota target: {}", e);
        } else {
            let msg = match target_bytes {
                Some(_) => format!("Quota target set to {} {}", target.input, target.unit),
                None => "Quota target cleared".to_string(),
            };
            app.status_message = Some(msg);
            app.status_message_time = Some(std::time::Instant::now());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::config::{AppConfig, Config};
    use crate::db::Database;
    use crate::tui::{PendingDeferral, PendingEntry};
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

    fn test_context<'a>(db: &'a Database, app_config: &'a AppConfig) -> TuiContext<'a> {
        TuiContext { db, app_config }
    }

    // ===== Navigation and Focus Tests =====

    #[test]
    fn tab_switches_focus_between_panels() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Start with main panel focused (default for immediate file interaction)
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);

        // Tab to sidebar
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Tab));
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);

        // Tab back to main panel
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Tab));
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
    }

    #[test]
    fn h_at_root_level_returns_to_sidebar() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        db.insert_root(Path::new("/test/downloads"))
            .expect("Failed to create test root");

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/downloads");
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('h')));
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
        assert!(app.sidebar_visible);
    }

    #[test]
    fn h_in_subdirectory_navigates_up() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        db.insert_root(Path::new("/test/downloads"))
            .expect("Failed to create test root");

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/downloads/subdir");
        app.entry_selected_index = 3;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('h')));

        assert_eq!(app.current_path, PathBuf::from("/test/downloads"));
        assert_eq!(app.entry_selected_index, 0);
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
    }

    #[test]
    fn h_in_sidebar_is_noop() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 2;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('h')));

        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
        assert_eq!(app.sidebar_selected_index, 2);
    }

    #[test]
    fn l_from_sidebar_enters_root() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        db.insert_root(Path::new("/test/downloads"))
            .expect("Failed to create test root");

        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('l')));

        assert_eq!(app.current_path, PathBuf::from("/test/downloads"));
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn l_on_directory_entry_navigates_into_it() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        let root_id = db
            .insert_root(Path::new("/test/downloads"))
            .expect("Failed to create test root");
        db.upsert_entry(
            root_id,
            Path::new("/test/downloads/subdir"),
            Path::new("/test/downloads"),
            true,
            0,
            None,
        )
        .expect("Failed to create dir entry");
        db.upsert_entry(
            root_id,
            Path::new("/test/downloads/file.txt"),
            Path::new("/test/downloads"),
            false,
            100,
            Some(1000),
        )
        .expect("Failed to create file entry");

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/downloads");
        // Sort by name puts directories first, so index 0 should be the subdir
        app.sort_mode = SortMode::Name;
        app.entry_selected_index = 0;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('l')));

        assert_eq!(app.current_path, PathBuf::from("/test/downloads/subdir"));
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn l_on_file_entry_is_noop() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        let root_id = db
            .insert_root(Path::new("/test/downloads"))
            .expect("Failed to create test root");
        db.upsert_entry(
            root_id,
            Path::new("/test/downloads/file.txt"),
            Path::new("/test/downloads"),
            false,
            100,
            Some(1000),
        )
        .expect("Failed to create file entry");

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/downloads");
        app.entry_selected_index = 0;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('l')));

        // Should remain in the same directory
        assert_eq!(app.current_path, PathBuf::from("/test/downloads"));
    }

    #[test]
    fn j_navigates_down_in_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Navigate down in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('j')));
        assert_eq!(app.sidebar_selected_index, 1);
        assert_eq!(app.entry_selected_index, 0, "File index should not change");

        // Navigate down in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('j')));
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
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Navigate up in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 5;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('k')));
        assert_eq!(app.sidebar_selected_index, 4);

        // Navigate up in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 5;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('k')));
        assert_eq!(app.entry_selected_index, 4);
    }

    #[test]
    fn g_goes_to_top_of_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Go to top in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 10;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('g')));
        assert_eq!(app.sidebar_selected_index, 0);

        // Go to top in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 10;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('g')));
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn capital_g_goes_to_bottom_of_focused_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Simulate list lengths (normally set by render)
        app.sidebar_len = 10;
        app.entry_list_len = 20;

        // Go to bottom in sidebar
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('G')));
        assert_eq!(app.sidebar_selected_index, 9); // len - 1

        // Go to bottom in main panel
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('G')));
        assert_eq!(app.entry_selected_index, 19); // len - 1
    }

    // ===== Sort Mode Tests =====

    #[test]
    fn s_cycles_sort_modes() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        assert_eq!(app.sort_mode, SortMode::Expiration);

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Size);

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Name);

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Modified);

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('s')));
        assert_eq!(app.sort_mode, SortMode::Expiration);
    }

    // ===== View Switching Tests =====

    #[test]
    fn number_1_switches_to_file_list_view() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.view = View::AuditLog;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('1')));
        assert_eq!(app.view, View::FileList);
    }

    #[test]
    fn number_2_switches_to_audit_log_view() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        assert_eq!(app.view, View::FileList);
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('2')));
        assert_eq!(app.view, View::AuditLog);
    }

    #[test]
    fn question_mark_switches_to_help_view() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        assert_eq!(app.view, View::FileList);
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('?')));
        assert_eq!(app.view, View::Help);
    }

    #[test]
    fn number_3_switches_to_help_view() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        assert_eq!(app.view, View::FileList);
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('3')));
        assert_eq!(app.view, View::Help);
    }

    #[test]
    fn help_view_closes_on_any_key() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.view = View::Help;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('x')));
        assert_eq!(app.view, View::FileList);
    }

    #[test]
    fn audit_log_view_returns_to_file_list_on_q() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.view = View::AuditLog;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('q')));
        assert_eq!(app.view, View::FileList);
    }

    #[test]
    fn audit_log_view_switches_to_help_on_question_mark() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.view = View::AuditLog;
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('?')));
        assert_eq!(app.view, View::Help);
    }

    // ===== Quit Tests =====

    #[test]
    fn q_quits_application() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        assert!(!app.should_quit);
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('q')));
        assert!(app.should_quit);
    }

    #[test]
    fn ctrl_c_quits_application() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        assert!(!app.should_quit);
        InputHandler::handle(
            &mut app,
            &ctx,
            make_key_event_with_mods(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert!(app.should_quit);
    }

    // ===== Root Entry/Exit Tests =====

    #[test]
    fn enter_in_sidebar_sets_current_path_and_focuses_main_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Create a root in the database
        db.insert_root(Path::new("/test/downloads"))
            .expect("Failed to create test root");

        // Focus sidebar and select the root
        app.focus_panel = FocusPanel::Sidebar;
        app.sidebar_selected_index = 0;

        // Press Enter
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Enter));

        assert_eq!(app.current_path, PathBuf::from("/test/downloads"));
        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
        assert_eq!(app.entry_selected_index, 0);
    }

    #[test]
    fn enter_in_sidebar_with_no_roots_is_noop() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.focus_panel = FocusPanel::Sidebar;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Enter));

        assert!(app.current_path.as_os_str().is_empty());
        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
    }

    #[test]
    fn backspace_at_root_level_returns_to_sidebar() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        db.insert_root(Path::new("/test/downloads"))
            .expect("Failed to create test root");

        // Simulate being inside a root
        app.current_path = PathBuf::from("/test/downloads");
        app.focus_panel = FocusPanel::MainPanel;

        // Press Backspace
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Backspace));

        assert_eq!(app.focus_panel, FocusPanel::Sidebar);
    }

    #[test]
    fn backspace_not_at_root_level_is_noop() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        db.insert_root(Path::new("/test/downloads"))
            .expect("Failed to create test root");

        // Simulate being inside a subdirectory within a root
        app.current_path = PathBuf::from("/test/downloads/subdir");
        app.focus_panel = FocusPanel::MainPanel;

        // Press Backspace — should not switch to sidebar since we're not at root level
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Backspace));

        assert_eq!(app.focus_panel, FocusPanel::MainPanel);
    }

    // ===== File-Level Action Tests =====

    /// Helper to set up test with a root and entries in the database.
    fn setup_with_files(db: &Database) -> (i64, Vec<i64>) {
        // Create a test root
        let root_id = db
            .insert_root(Path::new("/test/dir"))
            .expect("Failed to create test root");

        // Insert two test entries (files)
        let entry1_id = db
            .upsert_entry(
                root_id,
                Path::new("/test/dir/file1.txt"),
                Path::new("/test/dir"),
                false,
                500,
                Some(100),
            )
            .expect("Failed to create entry1");
        let entry2_id = db
            .upsert_entry(
                root_id,
                Path::new("/test/dir/file2.txt"),
                Path::new("/test/dir"),
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
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);

        // Set up app state to simulate viewing directory with first entry selected
        app.current_root_id = Some(dir_id);
        app.current_path = PathBuf::from("/test/dir");
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;

        // Press 'd' key
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('d')));

        // Should set pending_file_delete with first file
        assert!(
            app.pending_entry_delete.is_some(),
            "pending_file_delete should be set"
        );
        let deletion = app
            .pending_entry_delete
            .as_ref()
            .expect("Expected pending delete");
        assert_eq!(
            deletion.entries.len(),
            1,
            "Should have one file pending deletion"
        );
        assert_eq!(deletion.entries[0].id, file_ids[0], "Should be first file");
        assert_eq!(
            deletion.entries[0].path,
            PathBuf::from("/test/dir/file1.txt"),
            "Path should match first file"
        );
    }

    #[test]
    fn d_key_ignored_when_sidebar_focused() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, _file_ids) = setup_with_files(&db);

        // Focus sidebar (not main panel)
        app.current_root_id = Some(dir_id);
        app.focus_panel = FocusPanel::Sidebar;

        // Press 'd' key
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('d')));

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
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Create a real temporary file for deletion
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_file.txt");
        std::fs::write(&file_path, b"test content").expect("Failed to create temp file");

        // Set up database with the real file path
        let root_id = db
            .insert_root(temp_dir.path())
            .expect("Failed to create test root");
        let entry_id = db
            .upsert_entry(root_id, &file_path, temp_dir.path(), false, 13, Some(100))
            .expect("Failed to create entry");

        app.current_root_id = Some(root_id);

        // Manually set pending delete (simulating 'd' key press)
        app.pending_entry_delete = Some(PendingDeletion {
            entries: vec![PendingEntry {
                id: entry_id,
                path: file_path.clone(),
                is_dir: false,
            }],
            method: RemovalMethod::Trash,
        });

        // Press 'y' to confirm
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('y')));

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
            .list_entries_by_parent(temp_dir.path())
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
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id = Some(dir_id);

        // Manually set pending delete
        app.pending_entry_delete = Some(PendingDeletion {
            entries: vec![PendingEntry {
                id: file_ids[0],
                path: PathBuf::from("/test/dir/file1.txt"),
                is_dir: false,
            }],
            method: RemovalMethod::Trash,
        });

        // Press 'n' to cancel
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('n')));

        // Should clear pending delete
        assert!(
            app.pending_entry_delete.is_none(),
            "pending_file_delete should be cleared"
        );

        // Entry should still be in tracked status
        let entries = db
            .list_entries_by_parent(Path::new("/test/dir"))
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
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, _file_ids) = setup_with_files(&db);

        // Set up app state
        app.current_root_id = Some(dir_id);
        app.current_path = PathBuf::from("/test/dir");
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;

        // Press 'r' key
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('r')));

        // Should set pending_file_deferral
        assert!(
            app.pending_entry_deferral.is_some(),
            "pending_file_deferral should be set"
        );
        let deferral = app
            .pending_entry_deferral
            .as_ref()
            .expect("Expected deferral");
        assert_eq!(deferral.entries.len(), 1);
        assert_eq!(
            deferral.entries[0].path,
            PathBuf::from("/test/dir/file1.txt")
        );
        assert_eq!(deferral.default_days, 90); // from test_config
    }

    #[test]
    fn file_deferral_enter_confirms_with_default_days() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id = Some(dir_id);

        // Manually set pending deferral (empty input means use default)
        app.pending_entry_deferral = Some(PendingDeferral {
            entries: vec![PendingEntry {
                id: file_ids[0],
                path: PathBuf::from("/test/dir/file1.txt"),
                is_dir: false,
            }],
            input: String::new(),
            default_days: 90,
        });

        // Press Enter to confirm
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Enter));

        // Should clear pending deferral
        assert!(
            app.pending_entry_deferral.is_none(),
            "pending_file_deferral should be cleared"
        );

        // Entry should be marked as deferred
        let entries = db
            .list_entries_by_parent(Path::new("/test/dir"))
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
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id = Some(dir_id);

        // Manually set pending deferral with input
        app.pending_entry_deferral = Some(PendingDeferral {
            entries: vec![PendingEntry {
                id: file_ids[0],
                path: PathBuf::from("/test/dir/file1.txt"),
                is_dir: false,
            }],
            input: "30".to_string(),
            default_days: 90,
        });

        // Press Enter to confirm
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Enter));

        // Should clear pending deferral
        assert!(
            app.pending_entry_deferral.is_none(),
            "pending_file_deferral should be cleared"
        );

        // Entry should be deferred with calculated timestamp
        let entries = db
            .list_entries_by_parent(Path::new("/test/dir"))
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
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);

        // Set up app state
        app.current_root_id = Some(dir_id);
        app.current_path = PathBuf::from("/test/dir");
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;

        // Press 'i' key
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('i')));

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
        assert_eq!(entries[0].id, file_ids[0]);
        assert_eq!(entries[0].path, PathBuf::from("/test/dir/file1.txt"));
    }

    #[test]
    fn file_ignore_confirmation_y_ignores_file() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id = Some(dir_id);

        // Manually set pending ignore
        app.pending_entry_ignore = Some(vec![PendingEntry {
            id: file_ids[0],
            path: PathBuf::from("/test/dir/file1.txt"),
            is_dir: false,
        }]);

        // Press 'y' to confirm
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('y')));

        // Should clear pending ignore
        assert!(
            app.pending_entry_ignore.is_none(),
            "pending_file_ignore should be cleared"
        );

        // Entry should be marked as ignored
        let entries = db
            .list_entries_by_parent(Path::new("/test/dir"))
            .expect("Failed to list entries");
        let entry = entries
            .iter()
            .find(|e| e.id == file_ids[0])
            .expect("Entry should exist");
        assert_eq!(entry.status, "ignored", "Entry status should be 'ignored'");
    }

    #[test]
    fn x_key_approves_file_immediately() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);

        // Set up app state
        app.current_root_id = Some(dir_id);
        app.current_path = PathBuf::from("/test/dir");
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;

        // Press 'x' key
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('x')));

        // Entry should be marked as approved without modal confirmation
        let entries = db
            .list_entries_by_parent(Path::new("/test/dir"))
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

    #[test]
    fn x_key_toggles_approved_entry_back_to_tracked() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Set up database with files
        let (dir_id, file_ids) = setup_with_files(&db);
        app.current_root_id = Some(dir_id);

        app.current_path = PathBuf::from("/test/dir");
        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 0;

        // First press approves
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('x')));
        // Second press unapproves
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('x')));

        // Entry should be marked as tracked
        let entries = db
            .list_entries_by_parent(Path::new("/test/dir"))
            .expect("Failed to list entries");
        let entry = entries
            .iter()
            .find(|e| e.id == file_ids[0])
            .expect("Entry should exist");
        assert_eq!(entry.status, "tracked", "Entry status should be 'tracked'");
    }

    // ===== Search Tests =====

    /// Helper to set up a root with several distinctly-named files for search tests.
    fn setup_search_files(db: &Database) -> i64 {
        let root_id = db
            .insert_root(Path::new("/test/search"))
            .expect("Failed to create test root");

        // Create files with distinct names so we can test matching.
        // Default sort is by expiration (ascending), so mtime order matters.
        // Use mtimes that produce a known sort order:
        //   readme.md   mtime=100 (oldest, expires soonest)
        //   data.csv    mtime=200
        //   report.pdf  mtime=300
        //   notes.txt   mtime=400 (newest, expires last)
        for (name, mtime) in [
            ("readme.md", 100),
            ("data.csv", 200),
            ("report.pdf", 300),
            ("notes.txt", 400),
        ] {
            let path = format!("/test/search/{name}");
            db.upsert_entry(
                root_id,
                Path::new(&path),
                Path::new("/test/search"),
                false,
                1000,
                Some(mtime),
            )
            .unwrap_or_else(|_| panic!("Failed to create entry {name}"));
        }

        root_id
    }

    #[test]
    fn find_search_matches_returns_matching_indices() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        let root_id = setup_search_files(&db);

        let entries = db
            .list_entries_by_parent(Path::new("/test/search"))
            .expect("Failed to list entries");
        let mut entry_rows: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                let days = entry.countdown_start.map_or(i64::MAX, |cs| {
                    calculate_expiration(cs, ctx.app_config.global.expiration_days)
                });
                (entry, days)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, SortMode::Expiration);

        // "re" should match "readme.md" and "report.pdf"
        let matches = find_search_matches(&entry_rows, "re");
        assert_eq!(matches.len(), 2);

        // Verify the matched entries are the right ones
        let matched_names: Vec<String> = matches
            .iter()
            .map(|&i| {
                entry_rows[i]
                    .0
                    .path
                    .as_path()
                    .file_name()
                    .expect("entry should have filename")
                    .to_string_lossy()
                    .to_string()
            })
            .collect();
        assert!(matched_names.iter().any(|n| n == "readme.md"));
        assert!(matched_names.iter().any(|n| n == "report.pdf"));

        // Verify no root_id warning by using it
        let _ = root_id;
    }

    #[test]
    fn find_search_matches_is_case_insensitive() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_search_files(&db);

        let entries = db
            .list_entries_by_parent(Path::new("/test/search"))
            .expect("Failed to list entries");
        let mut entry_rows: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                let days = entry.countdown_start.map_or(i64::MAX, |cs| {
                    calculate_expiration(cs, ctx.app_config.global.expiration_days)
                });
                (entry, days)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, SortMode::Expiration);

        // "README" should still match "readme.md"
        let matches = find_search_matches(&entry_rows, "README");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn find_search_matches_empty_query_returns_empty() {
        let entry_rows: Vec<(crate::db::Entry, i64)> = Vec::new();
        let matches = find_search_matches(&entry_rows, "");
        assert!(matches.is_empty());
    }

    #[test]
    fn find_search_matches_no_matches_returns_empty() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_search_files(&db);

        let entries = db
            .list_entries_by_parent(Path::new("/test/search"))
            .expect("Failed to list entries");
        let mut entry_rows: Vec<_> = entries
            .into_iter()
            .map(|entry| {
                let days = entry.countdown_start.map_or(i64::MAX, |cs| {
                    calculate_expiration(cs, ctx.app_config.global.expiration_days)
                });
                (entry, days)
            })
            .collect();
        sort_entry_rows(&mut entry_rows, SortMode::Expiration);

        let matches = find_search_matches(&entry_rows, "zzzzz");
        assert!(matches.is_empty());
    }

    #[test]
    fn slash_enters_search_mode() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.focus_panel = FocusPanel::MainPanel;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('/')));

        assert!(app.search_input_active, "Search input should be active");
        assert_eq!(
            app.search_query,
            Some(String::new()),
            "Search query should be initialized to empty string"
        );
    }

    #[test]
    fn slash_only_works_in_main_panel() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.focus_panel = FocusPanel::Sidebar;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('/')));

        assert!(
            !app.search_input_active,
            "Search should not activate from sidebar"
        );
        assert_eq!(app.search_query, None);
    }

    #[test]
    fn search_input_appends_characters() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        // Enter search mode
        app.search_query = Some(String::new());
        app.search_input_active = true;

        // Type "abc"
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('a')));
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('b')));
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('c')));

        assert_eq!(app.search_query, Some("abc".to_string()));
        assert!(app.search_input_active);
    }

    #[test]
    fn search_input_backspace_removes_character() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.search_query = Some("abc".to_string());
        app.search_input_active = true;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Backspace));

        assert_eq!(app.search_query, Some("ab".to_string()));
    }

    #[test]
    fn search_input_esc_cancels_search() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.search_query = Some("test".to_string());
        app.search_input_active = true;

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Esc));

        assert_eq!(app.search_query, None, "Search query should be cleared");
        assert!(
            !app.search_input_active,
            "Search input should be deactivated"
        );
    }

    #[test]
    fn search_enter_confirms_and_jumps_to_first_match() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        setup_search_files(&db);

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/search");
        app.entry_selected_index = 0;

        // Enter search mode and type "notes"
        app.search_query = Some("notes".to_string());
        app.search_input_active = true;

        // Press Enter to confirm
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Enter));

        assert!(
            !app.search_input_active,
            "Search input should be deactivated after Enter"
        );
        assert_eq!(
            app.search_query,
            Some("notes".to_string()),
            "Search query should be preserved after confirmation"
        );

        // Cursor should have moved to the matching entry.
        // "notes.txt" has mtime=400, so in expiration sort it's last (index 3).
        // We verify the cursor moved away from 0.
        assert_ne!(
            app.entry_selected_index, 0,
            "Cursor should jump to matching entry (not stay at 0)"
        );
    }

    #[test]
    fn n_jumps_to_next_match() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        setup_search_files(&db);

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/search");

        // Set up confirmed search for "re" (matches readme.md and report.pdf)
        app.search_query = Some("re".to_string());
        app.search_input_active = false;

        // Verify matches exist
        let matches = ctx.compute_current_matches(&app);
        assert!(
            matches.len() >= 2,
            "Expected at least 2 matches for 're', got {matches:?}"
        );

        // Start at position 0
        app.entry_selected_index = 0;

        // Call jump_to_next_match directly to verify wrapping
        InputHandler::jump_to_next_match(&mut app, &ctx);
        let first_jump = app.entry_selected_index;
        assert!(
            matches.contains(&first_jump),
            "First jump ({first_jump}) should land on a match. Matches: {matches:?}"
        );

        // Jump again — should advance to next match or wrap
        InputHandler::jump_to_next_match(&mut app, &ctx);
        let second_jump = app.entry_selected_index;
        assert!(
            matches.contains(&second_jump),
            "Second jump ({second_jump}) should land on a match. Matches: {matches:?}"
        );

        // The two positions should be different (we have 2+ matches)
        assert_ne!(
            first_jump, second_jump,
            "Consecutive jumps should land on different matches"
        );
    }

    #[test]
    fn capital_n_jumps_to_previous_match() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        setup_search_files(&db);

        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/search");

        // Set up confirmed search for "re" (matches readme.md and report.pdf)
        app.search_query = Some("re".to_string());
        app.search_input_active = false;

        // Move to a position past the first match
        app.entry_selected_index = 3;

        // Press 'N' to go to previous match
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('N')));

        // Should have moved to a match before position 3
        assert!(
            app.entry_selected_index < 3,
            "N should jump to a match before current position"
        );
    }

    #[test]
    fn esc_clears_confirmed_search() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.focus_panel = FocusPanel::MainPanel;

        // Set up confirmed search (not in input mode)
        app.search_query = Some("test".to_string());
        app.search_input_active = false;

        // Press Esc
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Esc));

        assert_eq!(app.search_query, None, "Esc should clear search query");
        assert!(!app.search_input_active);
    }

    #[test]
    fn navigate_into_clears_search() {
        let mut app = App::new();
        app.search_query = Some("test".to_string());
        app.search_input_active = false;

        app.navigate_into(PathBuf::from("/some/path"));

        assert_eq!(app.search_query, None, "Navigation should clear search");
    }

    #[test]
    fn navigate_up_clears_search() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        let mut app = App::new();
        app.current_path = PathBuf::from("/some/path/child");
        app.search_query = Some("test".to_string());
        app.search_input_active = false;

        app.navigate_up(&ctx);

        assert_eq!(app.search_query, None, "Navigation up should clear search");
    }

    #[test]
    fn n_without_search_is_noop() {
        let (db, _dir) = temp_database();
        let mut app = App::new();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);

        app.focus_panel = FocusPanel::MainPanel;
        app.entry_selected_index = 2;

        // No search active — 'n' should not be intercepted as search navigation
        // (it falls through to the default match arm)
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('n')));

        assert_eq!(
            app.entry_selected_index, 2,
            "n without search should not move cursor"
        );
    }

    // ===== Visual Mode Tests =====

    /// Helper to set up a root with 5 files for visual mode tests.
    /// Returns the `root_id`. Entries are sorted by expiration (ascending mtime).
    /// Sorted order: alpha (100), bravo (200), charlie (300), delta (400), echo (500).
    fn setup_visual_files(db: &Database) -> i64 {
        let root_id = db
            .insert_root(Path::new("/test/visual"))
            .expect("Failed to create test root");

        for (name, mtime) in [
            ("alpha", 100),
            ("bravo", 200),
            ("charlie", 300),
            ("delta", 400),
            ("echo", 500),
        ] {
            let path = format!("/test/visual/{name}");
            db.upsert_entry(
                root_id,
                Path::new(&path),
                Path::new("/test/visual"),
                false,
                1000,
                Some(mtime),
            )
            .unwrap_or_else(|_| panic!("Failed to create entry {name}"));
        }

        root_id
    }

    /// Helper to get sorted entry IDs for the visual test directory.
    fn visual_entry_ids(ctx: &TuiContext) -> Vec<i64> {
        let app = {
            let mut a = App::new();
            a.current_path = PathBuf::from("/test/visual");
            a
        };
        ctx.sorted_entry_rows(&app)
            .expect("should have entries")
            .iter()
            .map(|(e, _)| e.id)
            .collect()
    }

    #[test]
    fn v_enters_visual_mode_with_anchor_at_cursor() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");
        app.entry_selected_index = 2; // charlie

        assert!(!app.is_visual_mode());

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));

        assert!(app.is_visual_mode());
        assert_eq!(app.visual_anchor, Some(2));
        // The entry at index 2 should be selected
        let ids = visual_entry_ids(&ctx);
        assert!(app.selected_entries.contains(&ids[2]));
        assert_eq!(app.selected_entries.len(), 1);
    }

    #[test]
    fn v_again_exits_visual_mode_keeping_selection() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");
        app.entry_selected_index = 1;

        // Enter visual mode
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));
        assert!(app.is_visual_mode());

        let ids = visual_entry_ids(&ctx);
        assert!(app.selected_entries.contains(&ids[1]));

        // Exit visual mode with v again
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));
        assert!(!app.is_visual_mode());
        // Selection should be preserved
        assert!(app.selected_entries.contains(&ids[1]));
    }

    #[test]
    fn visual_mode_j_extends_selection_downward() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");
        app.entry_list_len = 5;
        app.entry_selected_index = 1; // bravo

        // Enter visual mode at index 1
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));

        // Move down to index 3
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('j')));
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('j')));

        let ids = visual_entry_ids(&ctx);
        // Should have indices 1, 2, 3 selected (bravo, charlie, delta)
        assert!(app.selected_entries.contains(&ids[1]));
        assert!(app.selected_entries.contains(&ids[2]));
        assert!(app.selected_entries.contains(&ids[3]));
        assert!(!app.selected_entries.contains(&ids[0]));
        assert!(!app.selected_entries.contains(&ids[4]));
    }

    #[test]
    fn visual_mode_k_extends_selection_upward() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");
        app.entry_list_len = 5;
        app.entry_selected_index = 3; // delta

        // Enter visual mode at index 3
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));

        // Move up to index 1
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('k')));
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('k')));

        let ids = visual_entry_ids(&ctx);
        // Should have indices 1, 2, 3 selected
        assert!(app.selected_entries.contains(&ids[1]));
        assert!(app.selected_entries.contains(&ids[2]));
        assert!(app.selected_entries.contains(&ids[3]));
        assert!(!app.selected_entries.contains(&ids[0]));
        assert!(!app.selected_entries.contains(&ids[4]));
    }

    #[test]
    fn visual_mode_preserves_pre_existing_space_selections() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");
        app.entry_list_len = 5;
        app.entry_selected_index = 0;

        let ids = visual_entry_ids(&ctx);

        // Space-select item 0 (alpha) — Space also advances cursor to 1
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char(' ')));
        assert!(app.selected_entries.contains(&ids[0]));
        assert_eq!(app.entry_selected_index, 1);

        // Now enter visual mode at index 1 (bravo)
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));

        // Move down to index 3 (delta)
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('j')));
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('j')));

        // Alpha (pre-visual) + bravo, charlie, delta (visual range) should all be selected
        assert!(
            app.selected_entries.contains(&ids[0]),
            "pre-visual Space selection should be preserved"
        );
        assert!(app.selected_entries.contains(&ids[1]));
        assert!(app.selected_entries.contains(&ids[2]));
        assert!(app.selected_entries.contains(&ids[3]));
        assert!(!app.selected_entries.contains(&ids[4]));
    }

    #[test]
    fn visual_mode_shrinks_when_cursor_reverses() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");
        app.entry_list_len = 5;
        app.entry_selected_index = 1; // bravo

        // Enter visual mode
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));

        // Extend down to index 3
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('j')));
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('j')));

        let ids = visual_entry_ids(&ctx);
        assert_eq!(app.selected_entries.len(), 3); // 1, 2, 3

        // Now reverse: move back up to index 2
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('k')));

        // Range should shrink to [1, 2]
        assert!(app.selected_entries.contains(&ids[1]));
        assert!(app.selected_entries.contains(&ids[2]));
        assert!(
            !app.selected_entries.contains(&ids[3]),
            "delta should be deselected after cursor moved back"
        );
    }

    #[test]
    fn esc_exits_visual_mode_but_keeps_selection() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");
        app.entry_list_len = 5;
        app.entry_selected_index = 1;

        // Enter visual mode and extend
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('j')));

        let ids = visual_entry_ids(&ctx);
        assert_eq!(app.selected_entries.len(), 2);

        // Esc exits visual mode
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Esc));
        assert!(!app.is_visual_mode());
        // Selection preserved
        assert!(app.selected_entries.contains(&ids[1]));
        assert!(app.selected_entries.contains(&ids[2]));
    }

    #[test]
    fn space_exits_visual_mode() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");
        app.entry_list_len = 5;
        app.entry_selected_index = 2;

        // Enter visual mode
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));
        assert!(app.is_visual_mode());

        // Space should exit visual mode
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char(' ')));
        assert!(!app.is_visual_mode());
    }

    #[test]
    fn h_navigation_exits_visual_mode() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual/subdir");
        app.entry_selected_index = 0;

        // Manually enter visual mode state
        app.visual_anchor = Some(0);

        // h navigates up and should exit visual mode
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('h')));
        assert!(!app.is_visual_mode());
    }

    #[test]
    fn visual_mode_g_extends_to_top() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");
        app.entry_list_len = 5;
        app.entry_selected_index = 3; // delta

        // Enter visual mode at index 3
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));

        // g jumps to top
        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('g')));

        let ids = visual_entry_ids(&ctx);
        // Range should be [0, 3] inclusive
        assert!(app.selected_entries.contains(&ids[0]));
        assert!(app.selected_entries.contains(&ids[1]));
        assert!(app.selected_entries.contains(&ids[2]));
        assert!(app.selected_entries.contains(&ids[3]));
        assert_eq!(app.selected_entries.len(), 4);
    }

    #[test]
    fn v_on_empty_directory_is_noop() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        db.insert_root(Path::new("/test/empty"))
            .expect("Failed to create test root");

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/empty");

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('v')));
        assert!(!app.is_visual_mode());
    }

    #[test]
    fn a_selects_all_entries_in_current_directory() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::MainPanel;
        app.current_path = PathBuf::from("/test/visual");

        assert!(app.selected_entries.is_empty());

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('a')));

        let ids = visual_entry_ids(&ctx);
        assert_eq!(app.selected_entries.len(), ids.len());
        for id in &ids {
            assert!(app.selected_entries.contains(id));
        }
    }

    #[test]
    fn a_in_sidebar_does_nothing() {
        let (db, _dir) = temp_database();
        let app_config = AppConfig::from_global(test_config());
        let ctx = test_context(&db, &app_config);
        setup_visual_files(&db);

        let mut app = App::new();
        app.focus_panel = FocusPanel::Sidebar;

        assert!(app.selected_entries.is_empty());

        InputHandler::handle(&mut app, &ctx, make_key_event(KeyCode::Char('a')));

        // Should remain empty since we're in sidebar
        assert!(app.selected_entries.is_empty());
    }
}
